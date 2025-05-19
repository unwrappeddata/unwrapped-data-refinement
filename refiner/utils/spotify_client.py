import logging
import time
import requests
from typing import Dict, Any, List, Optional

from refiner.config import settings

logger = logging.getLogger(__name__)

class SpotifyAPIClient:
    BASE_URL = settings.SPOTIFY_API_URL
    TOKEN_URL = settings.SPOTIFY_TOKEN_URL

    def __init__(self, client_id: str, client_secret: str):
        if not client_id or not client_secret:
            logger.error("Spotify Client ID and Secret must be provided in environment variables (SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET). API calls will fail.")
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.token_expires_at: Optional[float] = None
        self.session = requests.Session()
        self.api_call_delay = getattr(settings, 'SPOTIFY_API_CALL_DELAY_SECONDS',
                                      getattr(settings, 'API_CALL_DELAY_SECONDS', 0.05))
        self.api_call_count: int = 0 # Initialize API call counter

    def _get_access_token(self) -> bool:
        if not self.client_id or not self.client_secret:
            logger.error("Cannot get Spotify token: Client ID or Secret not configured.")
            return False

        if self.access_token and self.token_expires_at and time.time() < self.token_expires_at:
            return True

        try:
            self.api_call_count += 1 # Count token request
            logger.debug(f"Spotify API call #{self.api_call_count} (auth): POST {self.TOKEN_URL}")
            auth_response = self.session.post(
                self.TOKEN_URL,
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials"},
                timeout=10
            )
            auth_response.raise_for_status()
            token_data = auth_response.json()
            self.access_token = token_data["access_token"]
            self.token_expires_at = time.time() + token_data["expires_in"] - 60
            logger.info("Successfully obtained new Spotify API access token.")
            return True
        except requests.RequestException as e:
            logger.error(f"Error obtaining Spotify access token: {e}")
            self.access_token = None
            self.token_expires_at = None
            return False

    def _make_request(self, method: str, endpoint: str, params: Optional[Dict] = None, json_data: Optional[Dict] = None, retries: int = 3) -> Optional[Any]:
        if not self._get_access_token():
            return None

        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"

        for attempt in range(retries):
            try:
                time.sleep(self.api_call_delay)

                self.api_call_count += 1 # Count data request
                logger.debug(f"Spotify API call #{self.api_call_count}: {method} {url} | Params: {params} | JSON: {json_data is not None}")

                response = self.session.request(method, url, headers=headers, params=params, json=json_data, timeout=15)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", "1"))
                    logger.warning(f"Rate limit hit for {url}. Retrying after {retry_after} seconds. Attempt {attempt + 1}/{retries}")
                    if attempt + 1 >= retries:
                        logger.error(f"Max retries reached for rate limit on {url}.")
                        response.raise_for_status()
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()

                if response.status_code == 204 or not response.content:
                    return None
                return response.json()

            except requests.RequestException as e:
                logger.error(f"RequestException on {method} {url} (attempt {attempt + 1}/{retries}): {e}")
                if attempt + 1 >= retries:
                    logger.error(f"Failed request to {url} after {retries} attempts.")
                    return None
                time.sleep(min(30, (2 ** attempt)))
        return None

    # ... rest of the methods (get_artists, get_artist, get_tracks, get_track) remain the same ...
    def get_artists(self, artist_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        if not artist_ids:
            return []

        unique_artist_ids = list(set(artist_id for artist_id in artist_ids if artist_id))
        if not unique_artist_ids:
            return []

        all_artists_data_map = {}

        for i in range(0, len(unique_artist_ids), settings.SPOTIFY_MAX_IDS_PER_BATCH):
            batch_ids = unique_artist_ids[i:i + settings.SPOTIFY_MAX_IDS_PER_BATCH]
            # logger.debug(f"Fetching artist batch: {batch_ids}") # Covered by _make_request debug log
            params = {"ids": ",".join(batch_ids)}
            response_data = self._make_request("GET", "artists", params=params)

            if response_data and "artists" in response_data:
                for artist_data in response_data["artists"]:
                    if artist_data and 'id' in artist_data:
                        all_artists_data_map[artist_data['id']] = artist_data
            else:
                logger.warning(f"Failed to fetch or parse artist data for batch starting with: {batch_ids[0] if batch_ids else 'N/A'}")

        ordered_results = [all_artists_data_map.get(aid) for aid in unique_artist_ids]
        return ordered_results

    def get_artist(self, artist_id: str) -> Optional[Dict[str, Any]]:
        if not artist_id: return None
        return self._make_request("GET", f"artists/{artist_id}")

    def get_tracks(self, track_ids: List[str]) -> List[Optional[Dict[str, Any]]]:
        if not track_ids:
            return []
        unique_track_ids = list(set(track_id for track_id in track_ids if track_id))
        if not unique_track_ids:
            return []

        all_tracks_data_map = {}

        for i in range(0, len(unique_track_ids), settings.SPOTIFY_MAX_IDS_PER_BATCH):
            batch_ids = unique_track_ids[i:i + settings.SPOTIFY_MAX_IDS_PER_BATCH]
            # logger.debug(f"Fetching track batch: {batch_ids}") # Covered by _make_request debug log
            params = {"ids": ",".join(batch_ids)}
            response_data = self._make_request("GET", "tracks", params=params)

            if response_data and "tracks" in response_data:
                for track_data in response_data["tracks"]:
                    if track_data and 'id' in track_data:
                        all_tracks_data_map[track_data['id']] = track_data
            else:
                logger.warning(f"Failed to fetch or parse track data for batch starting with: {batch_ids[0] if batch_ids else 'N/A'}")

        ordered_results = [all_tracks_data_map.get(tid) for tid in unique_track_ids]
        return ordered_results

    def get_track(self, track_id: str) -> Optional[Dict[str, Any]]:
        if not track_id: return None
        return self._make_request("GET", f"tracks/{track_id}")