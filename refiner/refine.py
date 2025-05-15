import json
import logging
import os

from refiner.models.offchain_schema import OffChainSchema
from refiner.models.output import Output
from refiner.transformer.unwrapped_transformer import UnwrappedTransformer
from refiner.config import settings
from refiner.utils.encrypt import encrypt_file
from refiner.utils.ipfs import upload_file_to_ipfs, upload_json_to_ipfs

class Refiner:
    def __init__(self):
        self.db_path = os.path.join(settings.OUTPUT_DIR, 'db.libsql')

    def transform(self) -> Output:
        """Transform all input files into the database."""
        logging.info("Starting data transformation for Unwrapped Proofs")
        output = Output()

        # Iterate through files and transform data
        input_file_processed = False
        for input_filename in os.listdir(settings.INPUT_DIR):
            if input_filename.lower().endswith('.json'):
                input_file_path = os.path.join(settings.INPUT_DIR, input_filename)
                logging.info(f"Found input JSON file: {input_file_path}")
                try:
                    with open(input_file_path, 'r') as f:
                        input_data = json.load(f)
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON from {input_filename}: {e}")
                    continue # Skip this file

                # Transform data
                transformer = UnwrappedTransformer(self.db_path)
                transformer.process(input_data)
                logging.info(f"Transformed {input_filename}")
                input_file_processed = True

                # Create a schema based on the SQLAlchemy schema
                schema = OffChainSchema(
                    name=settings.SCHEMA_NAME,
                    version=settings.SCHEMA_VERSION,
                    description=settings.SCHEMA_DESCRIPTION,
                    dialect=settings.SCHEMA_DIALECT,
                    schema=transformer.get_schema()
                )
                output.schema = schema

                # Upload the schema to IPFS
                schema_file = os.path.join(settings.OUTPUT_DIR, 'schema.json')
                with open(schema_file, 'w') as f:
                    json.dump(schema.model_dump(), f, indent=4)

                if settings.PINATA_API_KEY and settings.PINATA_API_SECRET:
                    try:
                        schema_ipfs_hash = upload_json_to_ipfs(schema.model_dump())
                        logging.info(f"Schema uploaded to IPFS with hash: {schema_ipfs_hash}")
                    except Exception as e:
                        logging.error(f"Failed to upload schema to IPFS: {e}")
                else:
                    logging.warning("Pinata API Key/Secret not configured. Skipping IPFS upload for schema.")


                # Encrypt and upload the database to IPFS
                encrypted_path = encrypt_file(settings.REFINEMENT_ENCRYPTION_KEY, self.db_path)

                # Only upload refinement if Pinata keys are configured
                if settings.PINATA_API_KEY and settings.PINATA_API_SECRET:
                    try:
                        ipfs_hash = upload_file_to_ipfs(encrypted_path)
                        output.refinement_url = f"{settings.PINATA_API_GATEWAY}/{ipfs_hash}"
                        logging.info(f"Refined data (encrypted DB) uploaded to IPFS: {output.refinement_url}")
                    except Exception as e:
                        logging.error(f"Failed to upload refined data to IPFS: {e}")
                        # Set a local file path if IPFS upload fails but Pinata was configured
                        output.refinement_url = f"file://{encrypted_path}"
                else:
                    logging.warning("Pinata API Key/Secret not configured. Skipping IPFS upload for refined data.")
                    # Provide a local file path for the encrypted database if not uploading
                    output.refinement_url = f"file://{encrypted_path}"

                break # Assuming only one main JSON proof file to process

        if not input_file_processed:
            logging.warning("No JSON input file was processed.")
            # For now, let's allow an empty output if no files were processed,
            # but the `run()` in __main__.py will raise FileNotFoundError if INPUT_DIR is empty.
            # If INPUT_DIR has non-JSON files, it will reach here.
            if not os.path.exists(self.db_path): # If DB wasn't even created
                # Create an empty DB so encryption doesn't fail
                UnwrappedTransformer(self.db_path) # This initializes an empty DB
                logging.info("Created an empty database as no input files were processed.")


        logging.info("Data transformation completed.")
        return output