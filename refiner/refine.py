import json
import logging
import os

from refiner.models.offchain_schema import OffChainSchema
from refiner.models.output import Output
from refiner.transformer.unwrapped_spotify_transformer import UnwrappedSpotifyTransformer
from refiner.config import settings
from refiner.utils.encrypt import encrypt_file
from refiner.utils.ipfs import upload_file_to_ipfs, upload_json_to_ipfs

logger = logging.getLogger(__name__)

class Refiner:
    def __init__(self):
        self.db_path = os.path.join(settings.OUTPUT_DIR, 'db.libsql')
        # Ensure output directory exists
        os.makedirs(settings.OUTPUT_DIR, exist_ok=True)


    def transform(self) -> Output:
        logger.info("Starting data transformation for Unwrapped Spotify Data")
        output = Output()

        total_models_generated_across_all_files = 0
        json_files_found_and_attempted = 0

        # Initialize transformer once if DB is cumulative or once per file if DB is recreated.
        # Current UnwrappedSpotifyTransformer (via DataTransformer) recreates DB on init.
        # So, it must be initialized inside the loop if multiple JSONs are to be processed into *separate* DB states,
        # or if one JSON overwrites the previous.
        # Given Vana's model of one input -> one refined output, we expect one JSON.
        # If multiple JSONs are in input/, the last one processed will be the final DB content.

        # Let's assume one primary JSON file is expected, or they are merged.
        # The current DataTransformer._initialize_database deletes and recreates.
        # So, only the last processed JSON file's data will persist if multiple are present.
        # This refinement is for "a contribution", typically one results.json.

        transformer = UnwrappedSpotifyTransformer(self.db_path) # Initializes DB (deletes if exists)

        for input_filename in os.listdir(settings.INPUT_DIR):
            input_file_path = os.path.join(settings.INPUT_DIR, input_filename)
            if os.path.isfile(input_file_path) and input_filename.lower().endswith('.json'):
                json_files_found_and_attempted += 1
                logger.info(f"Processing input file: {input_filename}")
                try:
                    with open(input_file_path, 'r') as f:
                        input_data = json.load(f)
                except json.JSONDecodeError as e:
                    logger.error(f"Error decoding JSON from {input_filename}: {e}. Skipping this file.")
                    continue
                except Exception as e:
                    logger.error(f"Error reading file {input_filename}: {e}. Skipping this file.")
                    continue

                # Transform data from the current file
                # The transformer's DB is already initialized. If multiple JSONs, it's accumulating.
                # If DataTransformer._initialize_database is called per file, then it's per file.
                # As it stands, UnwrappedSpotifyTransformer's __init__ calls _initialize_database,
                # so if created outside the loop, it's one DB. If inside, it's one DB per file (last one wins).
                # For this specific Unwrapped use case, we assume one primary results.json.
                # If multiple JSONs are present, and transformer is init outside, they will all be processed into *one* DB.

                models_from_current_file = transformer.transform(input_data)

                if models_from_current_file:
                    try:
                        # Save these models. The transformer instance (self.db_path) is the same.
                        num_saved = transformer.save_models(models_from_current_file)
                        logger.info(f"Saved {num_saved} models from {input_filename} to the database.")
                        total_models_generated_across_all_files += num_saved # Accumulate total models

                        # Generate and set the schema definition in the output object (only once)
                        if not output.output_schema and num_saved > 0 : # Ensure schema is for a non-empty DB
                            schema_str = transformer.get_schema()
                            if schema_str: # Ensure schema string is not empty
                                schema_obj = OffChainSchema(
                                    name=settings.SCHEMA_NAME,
                                    version=settings.SCHEMA_VERSION,
                                    description=settings.SCHEMA_DESCRIPTION,
                                    dialect=settings.SCHEMA_DIALECT,
                                    schema_definition=schema_str
                                )
                                output.output_schema = schema_obj

                                schema_file_path = os.path.join(settings.OUTPUT_DIR, 'schema.json')
                                with open(schema_file_path, 'w') as sf:
                                    json.dump(schema_obj.model_dump(exclude_none=True), sf, indent=4) # exclude_none for cleaner output
                                logger.info(f"Schema definition saved to {schema_file_path}")

                                if settings.PINATA_API_KEY and settings.PINATA_API_SECRET:
                                    try:
                                        schema_ipfs_hash = upload_json_to_ipfs(schema_obj.model_dump(exclude_none=True))
                                        logger.info(f"Schema uploaded to IPFS with hash: {schema_ipfs_hash}")
                                        # output.schema_ipfs_url = f"ipfs://{schema_ipfs_hash}" # Store if needed by Vana
                                    except Exception as e:
                                        logger.error(f"Failed to upload schema to IPFS: {e}")
                                else:
                                    logger.info("Pinata API Key/Secret not set. Skipping IPFS upload for schema.")
                            else:
                                logger.warning("Generated schema string is empty. Schema will not be included in output.")
                    except Exception as e:
                        logger.error(f"Failed to save models from {input_filename} to database: {e}. Continuing...")
                        # Potentially some models from this file failed to save. total_models_generated_across_all_files might be optimistic.
                        # However, save_models re-raises, so this block might not be hit if save_models fails hard.
                else:
                    logger.info(f"No models generated from {input_filename}. Nothing to save for this file.")

        # After processing all JSON files
        if json_files_found_and_attempted > 0 and total_models_generated_across_all_files == 0:
            logger.error("Data refinement process completed, but no valid records were generated and saved from the input file(s).")
            raise ValueError("No records refined from input JSON file(s). Halting process.")
        elif json_files_found_and_attempted == 0:
            # This case should ideally be caught by __main__.py before calling Refiner.
            logger.warning("No JSON input files were found in the input directory to process.")
            # No ValueError here, as no work was attempted. __main__ might raise FileNotFoundError.
            return output # Return empty output

        # Proceed with encryption and IPFS upload only if models were generated and saved
        if total_models_generated_across_all_files > 0:
            if not os.path.exists(self.db_path):
                logger.error(f"Database file {self.db_path} not found after processing, but models were expected. Cannot encrypt or upload.")
                # This indicates a potential issue, perhaps DB initialization failed silently or was deleted.
                # Output will have no refinement_url.
            else:
                try:
                    encrypted_path = encrypt_file(settings.REFINEMENT_ENCRYPTION_KEY, self.db_path)
                    logger.info(f"Database encrypted to: {encrypted_path}")

                    if settings.PINATA_API_KEY and settings.PINATA_API_SECRET:
                        try:
                            ipfs_hash = upload_file_to_ipfs(encrypted_path)
                            # Use the configured Pinata gateway for the URL
                            gateway_prefix = settings.PINATA_API_GATEWAY.rstrip('/')
                            output.refinement_url = f"{gateway_prefix}/{ipfs_hash}"
                            logger.info(f"Encrypted database uploaded to IPFS: {output.refinement_url}")
                        except Exception as e:
                            logger.error(f"Failed to upload refined database to IPFS: {e}")
                            output.refinement_url = f"file://{encrypted_path}"
                    else:
                        logger.info("Pinata API Key/Secret not set. Skipping IPFS upload for refined database.")
                        output.refinement_url = f"file://{encrypted_path}"
                except Exception as e:
                    logger.error(f"Error during database encryption or IPFS upload preparation: {e}")
                    # output.refinement_url will remain None or be a local file path if encryption succeeded but upload failed.
        else:
            # This case (total_models_generated_across_all_files == 0) is handled by the ValueError above if JSONs were attempted.
            # If no JSONs were attempted, this path is fine (empty output returned).
            logger.info("No models were generated or saved, so no database to encrypt or upload.")


        logger.info(f"Data transformation processing finished. Final output: {output.model_dump_json(indent=2, exclude_none=True)}")
        return output