import json
import logging
import os
import requests
import hashlib
from multiformats_cid import CIDv0 as ActualCID
from multiformats import multihash as mh_tool

from refiner.config import settings

logger = logging.getLogger(__name__)

PINATA_FILE_API_ENDPOINT = "https://api.pinata.cloud/pinning/pinFileToIPFS"
PINATA_JSON_API_ENDPOINT = "https://api.pinata.cloud/pinning/pinJSONToIPFS"

def upload_json_to_ipfs(data):
    """
    Uploads JSON data to IPFS using Pinata API.
    :param data: JSON data to upload (dictionary or list)
    :return: IPFS hash
    """
    if not settings.PINATA_API_KEY or not settings.PINATA_API_SECRET:
        raise Exception("Error: Pinata IPFS API credentials not found, please check your environment variables")

    headers = {
        "Content-Type": "application/json",
        "pinata_api_key": settings.PINATA_API_KEY,
        "pinata_secret_api_key": settings.PINATA_API_SECRET
    }

    try:
        response = requests.post(
            PINATA_JSON_API_ENDPOINT,
            data=json.dumps(data),
            headers=headers
        )
        response.raise_for_status()

        result = response.json()
        logging.info(f"Successfully uploaded JSON to IPFS with hash: {result['IpfsHash']}")
        logging.info(f"Access at: {settings.PINATA_API_GATEWAY}/{result['IpfsHash']}")
        return result['IpfsHash']

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while uploading JSON to IPFS: {e}")
        raise e

def upload_file_to_ipfs(file_path=None):
    """
    Uploads a file to IPFS using Pinata API (https://pinata.cloud/)
    :param file_path: Path to the file to upload (defaults to encrypted database)
    :return: IPFS hash
    """
    if file_path is None:
        # Default to the encrypted database file
        file_path = os.path.join(settings.OUTPUT_DIR, "db.libsql.pgp")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
        
    if not settings.PINATA_API_KEY or not settings.PINATA_API_SECRET:
        raise Exception("Error: Pinata IPFS API credentials not found, please check your environment variables")

    headers = {
        "pinata_api_key": settings.PINATA_API_KEY,
        "pinata_secret_api_key": settings.PINATA_API_SECRET
    }

    try:
        with open(file_path, 'rb') as file:
            files = {
                'file': file
            }
            response = requests.post(
                PINATA_FILE_API_ENDPOINT,
                files=files,
                headers=headers
            )
        
        response.raise_for_status()
        result = response.json()
        logging.info(f"Successfully uploaded file to IPFS with hash: {result['IpfsHash']}")
        logging.info(f"Access at: {settings.PINATA_API_GATEWAY}/{result['IpfsHash']}")
        return result['IpfsHash']

    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred while uploading file to IPFS: {e}")
        raise e


def calculate_cid_for_bytes(data_bytes: bytes, version: int = 1, codec_name: str = "dag-pb") -> str:
    """
    Calculates an IPFS CID for given bytes.
    :param data_bytes: The raw bytes of the content.
    :param version: CID version (0 or 1).
    :param codec_name: Name of the codec (e.g., 'dag-pb', 'raw', 'dag-json').
                       CIDv0 is implicitly 'dag-pb'.
    :return: String representation of the CID.
    """
    data_hash = hashlib.sha256(data_bytes).digest()
    wrapped_multihash = mh_tool.wrap(data_hash, "sha2-256")

    if version == 1:
        cid_obj = ActualCID(wrapped_multihash)
    elif version == 0:
        # CIDv0 is implicitly dag-pb. The multihash itself forms the core of the CIDv0.
        if codec_name != "dag-pb":
            logger.debug(f"CIDv0 requested but codec is {codec_name}. CIDv0 is implicitly dag-pb.")
        cid_obj = ActualCID(wrapped_multihash)
    else:
        raise ValueError(f"Unsupported CID version: {version}")
    return str(cid_obj)


def calculate_cid_for_json_obj(data_dict: dict, version: int = 1, codec_name: str = "dag-pb") -> str:
    """
    Calculates the IPFS CID for a JSON serializable dictionary
    by serializing it to UTF-8 bytes first.
    Uses `calculate_cid_for_bytes`.
    """
    try:
        # Serialize with sorted keys and no unnecessary whitespace for consistency
        json_bytes = json.dumps(data_dict, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return calculate_cid_for_bytes(json_bytes, version=version, codec_name=codec_name)
    except Exception as e:
        logger.error(f"Error calculating IPFS CID for JSON object: {e}")
        json_str_preview = json.dumps(data_dict)[:200]
        logger.debug(f"Problematic JSON (preview for CID calculation): {json_str_preview}")
        return f"ErrorCalculatingCID: {e}"


def _log_request_exception_details(e: requests.exceptions.RequestException, operation: str, endpoint: str):
    """Helper to log detailed information from requests exceptions."""
    base_message = f"An error occurred during IPFS {operation} to {endpoint}: {e}"
    logger.error(base_message)
    if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
        logger.error(
            f"HTTPError details for IPFS {operation}: "
            f"Status Code: {e.response.status_code}. "
            f"Response Headers: {e.response.headers}. "
            f"Response Text (first 500 chars): {e.response.text[:500]}"
        )
    elif isinstance(e, requests.exceptions.ConnectionError):
        logger.error(f"ConnectionError during IPFS {operation}: Check network connectivity and Pinata API endpoint ({endpoint}).")
    elif isinstance(e, requests.exceptions.Timeout):
        logger.error(f"Timeout during IPFS {operation}: The request to Pinata ({endpoint}) timed out after {settings.PINATA_TIMEOUT}s.")


# Test with: python -m refiner.utils.ipfs
if __name__ == "__main__":
    print("Running IPFS utility tests...")

    test_json_data = {"name": "test_object", "version": 1, "details": {"value": 42, "status": "active"}}
    print(f"Test JSON data: {test_json_data}")

    cid_v1_dpb = calculate_cid_for_json_obj(test_json_data, version=1, codec_name="dag-pb")
    print(f"Calculated CID (v1, dag-pb, base32): {cid_v1_dpb}")

    cid_v0_dpb = calculate_cid_for_json_obj(test_json_data, version=0, codec_name="dag-pb") # CIDv0 is implicitly dag-pb
    print(f"Calculated CID (v0, dag-pb, base58btc): {cid_v0_dpb}")

    cid_v1_djson = calculate_cid_for_json_obj(test_json_data, version=1, codec_name="dag-json")
    print(f"Calculated CID (v1, dag-json, base32): {cid_v1_djson}")
    print(f"done")

    ipfs_hash = upload_file_to_ipfs()
    print(f"File uploaded to IPFS with hash: {ipfs_hash}")

    ipfs_hash = upload_json_to_ipfs()
    print(f"JSON uploaded to IPFS with hash: {ipfs_hash}")
    print(f"Access at: {settings.PINATA_API_GATEWAY}/{ipfs_hash}")