import logging
import time
import requests
from typing import Dict, Any, List, Optional

from refiner.models.refined import Base, User, UserListeningStats, Artist, PlayedTrack, UserTopArtistAssoc
from refiner.transformer.base_transformer import DataTransformer
from refiner.models.unrefined import UnwrappedData
from refiner.utils.date import parse_timestamp
from refiner.config import settings

# Configure logging for this module
logger = logging.getLogger(__name__)

class UnwrappedSpotifyTransformer(DataTransformer):
    """
    Transformer for Unwrapped Spotify data, enriching artist info via an external API.
    """

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.data_refinement_api_base_url = settings.DATA_REFINEMENT_API_BASE_URL.rstrip('/')
        self.api_call_delay = settings.API_CALL_DELAY_SECONDS
        self.session = requests.Session() # Use a session for potential keep-alive

    def _fetch_artist_details_from_api(self, artist_id: str) -> Optional[Dict[str, Any]]:
        """Fetches artist details from the /artists/{id} endpoint."""
        try:
            url = f"{self.data_refinement_api_base_url}/artists/{artist_id}"
            response = self.session.get(url, timeout=5)
            time.sleep(self.api_call_delay) # Respect API rate limits/load
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"Artist ID {artist_id} not found via API (404).")
                return None
            else:
                logger.error(f"Error fetching artist {artist_id} from API: {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            logger.error(f"RequestException fetching artist {artist_id}: {e}")
            return None

    def _fetch_track_details_from_api(self, track_id: str) -> Optional[Dict[str, Any]]:
        """Fetches track details from the /tracks/{id} endpoint."""
        try:
            url = f"{self.data_refinement_api_base_url}/tracks/{track_id}" # Assuming tracks endpoint is under the same base
            response = self.session.get(url, timeout=5)
            time.sleep(self.api_call_delay)
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"Error fetching track {track_id} from API: {response.status_code} - {response.text}")
                return None
        except requests.RequestException as e:
            logger.error(f"RequestException fetching track {track_id}: {e}")
            return None

    def transform(self, data: Dict[str, Any]) -> List[Base]:
        unrefined = UnwrappedData.model_validate(data)
        models_to_save: List[Base] = []
        artists_cache: Dict[str, Artist] = {}

        # 1. Create User
        refined_user = User(
            id_hash=unrefined.user.id_hash,
            country=unrefined.user.country,
            product=unrefined.user.product,
            file_id=settings.FILE_ID
        )
        models_to_save.append(refined_user)

        # 2. Create UserListeningStats
        stats = unrefined.stats
        first_listen_dt = parse_timestamp(stats.first_listen) if stats.first_listen else None
        last_listen_dt = parse_timestamp(stats.last_listen) if stats.last_listen else None

        listening_stats = UserListeningStats(
            user_id_hash=unrefined.user.id_hash,
            total_minutes=stats.total_minutes,
            track_count=stats.track_count,
            unique_artists_count=stats.unique_artists_count,
            activity_period_days=stats.activity_period_days,
            first_listen_at=first_listen_dt,
            last_listen_at=last_listen_dt
        )
        models_to_save.append(listening_stats)

        # Helper to create or update artist in cache and models_to_save
        def get_or_create_artist_from_api_data(artist_id: str, api_data: Optional[Dict[str, Any]], source_track_id_for_name_fallback: Optional[str] = None) -> Artist:
            if artist_id in artists_cache:
                # If from a less detailed source (like track API), update if more details found
                artist_obj = artists_cache[artist_id]
                if api_data and (artist_obj.name.startswith("[") or not artist_obj.popularity): # If placeholder or missing details
                    artist_obj.name = api_data.get('name', artist_obj.name)
                    artist_obj.popularity = api_data.get('popularity')
                    artist_obj.genres = api_data.get('genres')
                    artist_obj.followers_total = api_data.get('followers', {}).get('total') if api_data.get('followers') else None
                    artist_obj.primary_image_url = api_data.get('images', [{}])[0].get('url') if api_data.get('images') else None
                return artist_obj

            artist_name = f"[UNKNOWN_ARTIST_ID:{artist_id}]" # Default placeholder
            artist_popularity = None
            artist_genres = []
            artist_followers_total = None
            artist_primary_image_url = None

            if api_data: # Data from /artists/{id}
                artist_name = api_data.get('name', artist_name)
                artist_popularity = api_data.get('popularity')
                artist_genres = api_data.get('genres', [])
                if api_data.get('followers') is not None: # Check if 'followers' key exists
                    artist_followers_total = api_data['followers'].get('total')
                if api_data.get('images') and isinstance(api_data['images'], list) and len(api_data['images']) > 0:
                    artist_primary_image_url = api_data['images'][0].get('url')
            elif source_track_id_for_name_fallback: # Fallback to /tracks/{id} for name only
                logger.info(f"Artist {artist_id} not found via artist API, attempting track API fallback using track {source_track_id_for_name_fallback}.")
                track_api_data = self._fetch_track_details_from_api(source_track_id_for_name_fallback)
                if track_api_data and 'artists' in track_api_data:
                    for art_in_track in track_api_data['artists']:
                        if art_in_track.get('id') == artist_id and art_in_track.get('name'):
                            artist_name = art_in_track['name']
                            logger.info(f"Found name '{artist_name}' for artist {artist_id} via track API fallback.")
                            break

            new_artist = Artist(
                id=artist_id,
                name=artist_name,
                popularity=artist_popularity,
            )
            artists_cache[artist_id] = new_artist
            models_to_save.append(new_artist)

            if artist_name == f"[UNKNOWN_ARTIST_ID:{artist_id}]":
                logger.warning(f"!Artist {artist_id} not found via API! for track {track_data.track_id}")
                exit(1)

            return new_artist

        # 3. Process Played Tracks
        for track_data in unrefined.tracks:

            if track_data.track_id == track_data.artist_id:
                # logger.warning(f"Track {track_data.track_id} has the same ID as its artist. Skipping.")
                continue

            track_api_data = self._fetch_track_details_from_api(track_data.track_id)

            if not track_api_data:
                logger.warning(f"Track {track_data.track_id} not found via API.")
                continue

            api_data = track_api_data.get('artists', []) # Get the first artist or an empty dict if none
            if not api_data or len(api_data) == 0:
                logger.warning(f"Track {track_data.track_id} has no associated artists.")
                continue

            artist_api_data = api_data[0]

            artist_id = artist_api_data['id']
            if artist_id not in artists_cache:
                get_or_create_artist_from_api_data(artist_id, artist_api_data, source_track_id_for_name_fallback=track_data.track_id)

            played_track = PlayedTrack(
                user_id_hash=unrefined.user.id_hash,
                track_id=track_data.track_id,
                artist_id=artist_id,
                duration_ms=track_data.duration_ms,
                listened_at=parse_timestamp(track_data.listened_at)
            )
            models_to_save.append(played_track)

        return models_to_save