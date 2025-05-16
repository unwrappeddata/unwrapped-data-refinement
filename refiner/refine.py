import json
import logging
import os

from refiner.models.offchain_schema import OffChainSchema
from refiner.models.output import Output
from refiner.transformer.unwrapped_spotify_transformer import UnwrappedSpotifyTransformer
from refiner.config import settings
from refiner.utils.encrypt import encrypt_file
from refiner.utils.ipfs import upload_file_to_ipfs, upload_json_to_ipfs

class Refiner:
    def __init__(self):
        self.db_path = os.path.join(settings.OUTPUT_DIR, 'db.libsql')

    def transform(self) -> Output:
        """Transform all input files into the database."""
        logging.info("Starting data transformation for Unwrapped Spotify Data")
        output = Output() # Initializes with output_schema=None and refinement_url=None

        processed_files = 0
        for input_filename in os.listdir(settings.INPUT_DIR):
            input_file = os.path.join(settings.INPUT_DIR, input_filename)
            if os.path.isfile(input_file) and os.path.splitext(input_file)[1].lower() == '.json':
                logging.info(f"Processing input file: {input_filename}")
                with open(input_file, 'r') as f:
                    try:
                        input_data = json.load(f)
                    except json.JSONDecodeError as e:
                        logging.error(f"Error decoding JSON from {input_filename}: {e}")
                        continue # Skip this file

                    # The DataTransformer._initialize_database (called by __init__)
                    # deletes and recreates the DB. This is fine for a single input JSON.
                    transformer = UnwrappedSpotifyTransformer(self.db_path)
                    transformer.process(input_data)
                    logging.info(f"Transformed {input_filename}")
                    processed_files +=1

                    # Generate and set the schema definition in the output object
                    # This will only happen once, for the first processed file.
                    if not output.output_schema:
                        schema_obj = OffChainSchema(
                            name=settings.SCHEMA_NAME,
                            version=settings.SCHEMA_VERSION,
                            description=settings.SCHEMA_DESCRIPTION,
                            dialect=settings.SCHEMA_DIALECT,
                            schema_definition=transformer.get_schema() # Use renamed field
                        )
                        output.output_schema = schema_obj # Assign to renamed field

                        # Save the schema.json locally
                        schema_file_path = os.path.join(settings.OUTPUT_DIR, 'schema.json')
                        with open(schema_file_path, 'w') as sf:
                            json.dump(schema_obj.model_dump(), sf, indent=4)
                        logging.info(f"Schema definition saved to {schema_file_path}")

                        # Upload the schema to IPFS if Pinata credentials are provided
                        if settings.PINATA_API_KEY and settings.PINATA_API_SECRET:
                            try:
                                schema_ipfs_hash = upload_json_to_ipfs(schema_obj.model_dump())
                                logging.info(f"Schema uploaded to IPFS with hash: {schema_ipfs_hash}")
                                # Optionally, store this IPFS hash in the output if needed by Vana service
                                # output.schema_ipfs_url = f"ipfs://{schema_ipfs_hash}"
                            except Exception as e:
                                logging.error(f"Failed to upload schema to IPFS: {e}")
                        else:
                            logging.warning("Pinata API Key/Secret not set. Skipping IPFS upload for schema.")

                    # If we intend one DB per input file, encryption and upload should happen here,
                    # and the loop should probably break or handle multiple output CIDs.
                    # Given Vana's model, one refinement job usually processes one input file.
                    # So, this loop processing multiple JSONs into one DB might be an edge case
                    # or for local testing. Let's assume for now only one JSON is expected in /input.

        if processed_files > 0:
            # Encrypt and upload the database to IPFS. This happens after all files are processed.
            # If only one JSON file was in input_dir, self.db_path contains its refined data.
            try:
                encrypted_path = encrypt_file(settings.REFINEMENT_ENCRYPTION_KEY, self.db_path)
                logging.info(f"Database encrypted to: {encrypted_path}")

                if settings.PINATA_API_KEY and settings.PINATA_API_SECRET:
                    try:
                        ipfs_hash = upload_file_to_ipfs(encrypted_path)
                        output.refinement_url = f"{settings.PINATA_API_GATEWAY}/{ipfs_hash}"

                        logging.info(f"Encrypted database uploaded to IPFS with hash: {ipfs_hash}")
                    except Exception as e:
                        logging.error(f"Failed to upload refined database to IPFS: {e}")
                        output.refinement_url = f"file://{encrypted_path}" # Fallback to local file path
                else:
                    logging.warning("Pinata API Key/Secret not set. Skipping IPFS upload for refined database.")
                    output.refinement_url = f"file://{encrypted_path}" # Local file path if not uploaded
            except Exception as e:
                logging.error(f"Error during database encryption or upload: {e}")
                # Potentially set output.refinement_url to None or an error indicator

        elif processed_files == 0:
            logging.warning("No JSON files were processed from the input directory.")
            # Output will have None for output_schema and refinement_url

        logging.info(f"Data transformation completed. Output: {output.model_dump_json(indent=2)}")
        return output