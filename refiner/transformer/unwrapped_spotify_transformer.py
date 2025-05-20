import logging
from typing import Dict, Any, List, Optional
from collections import defaultdict
from datetime import datetime

from refiner.models.refined import Base, User, UserListeningStats, Artist, PlayedTrack, UserTopArtistAssoc
from refiner.transformer.base_transformer import DataTransformer
from refiner.models.unrefined import UnwrappedData
from refiner.utils.date import parse_timestamp
from refiner.config import settings
from refiner.utils.spotify_client import SpotifyAPIClient

# Configure logging for this module
logger = logging.getLogger(__name__)

class UnwrappedSpotifyTransformer(DataTransformer):
    """
    Transformer for Unwrapped Spotify data, enriching artist info via Spotify API
    and deriving top artists from play history. Skips artists/tracks not found via API.
    """

    def __init__(self, db_path: str):
        super().__init__(db_path)
        self.spotify_client = SpotifyAPIClient(
            client_id=settings.SPOTIFY_CLIENT_ID,
            client_secret=settings.SPOTIFY_CLIENT_SECRET
        )
        if not settings.SPOTIFY_CLIENT_ID or not settings.SPOTIFY_CLIENT_SECRET:
            logger.warning("Spotify Client ID or Secret is not configured. Artist/Track enrichment will likely fail or be incomplete.")


    def transform(self, data: Dict[str, Any]) -> List[Base]:
        # Reset API call count for each transform call if transformer instance is reused for multiple files
        # (though current Refiner creates one transformer for all files)
        # If UnwrappedSpotifyTransformer is long-lived and processes multiple independent inputs,
        # you might want to reset self.spotify_client.api_call_count = 0 here.
        # However, for a single run processing one main results.json, this is fine as is.

        try:
            unrefined = UnwrappedData.model_validate(data)
        except Exception as e:
            logger.error(f"Failed to validate input data with UnwrappedData model: {e}")
            logger.debug(f"Problematic data snippet: {str(data)[:500]}")
            return []

        models_to_save: List[Base] = []
        artists_in_db_cache: Dict[str, Artist] = {}
        map_input_artist_id_to_db_id: Dict[str, str] = {}

        # 1. Create User
        refined_user = User(
            id_hash=unrefined.user.id_hash,
            country=unrefined.user.country,
            product=unrefined.user.product
        )
        models_to_save.append(refined_user)

        # 2. Create UserListeningStats
        stats_data = unrefined.stats
        first_listen_dt = parse_timestamp(stats_data.first_listen) if stats_data.first_listen else None
        last_listen_dt = parse_timestamp(stats_data.last_listen) if stats_data.last_listen else None

        listening_stats = UserListeningStats(
            user_id_hash=unrefined.user.id_hash,
            total_minutes=stats_data.total_minutes,
            track_count=stats_data.track_count,
            unique_artists_count=stats_data.unique_artists_count,
            activity_period_days=stats_data.activity_period_days,
            first_listen_at=first_listen_dt,
            last_listen_at=last_listen_dt
        )
        models_to_save.append(listening_stats)

        # 3. Artist Processing
        all_artist_ids_from_input_tracks = list(set(
            t.artist_id for t in unrefined.tracks if t.artist_id and t.track_id != t.artist_id
        ))

        if all_artist_ids_from_input_tracks:
            logger.info(f"Found {len(all_artist_ids_from_input_tracks)} unique artist IDs in input tracks. Fetching from Spotify API...")
            spotify_artists_api_responses = self.spotify_client.get_artists(all_artist_ids_from_input_tracks)

            for i, input_id_sent_to_api in enumerate(all_artist_ids_from_input_tracks):
                api_data = spotify_artists_api_responses[i]

                if api_data and api_data.get('id') and api_data.get('name'):
                    id_from_api_response = api_data['id']
                    artist_name = api_data['name']

                    map_input_artist_id_to_db_id[input_id_sent_to_api] = id_from_api_response

                    if id_from_api_response not in artists_in_db_cache:
                        new_artist = Artist(
                            id=id_from_api_response,
                            name=artist_name,
                            popularity=api_data.get('popularity'),
                            genres=api_data.get('genres', []),
                            followers_total=api_data.get('followers', {}).get('total'),
                            primary_image_url=(api_data.get('images', [{}])[0].get('url') if api_data.get('images') else None)
                        )
                        artists_in_db_cache[id_from_api_response] = new_artist
                        models_to_save.append(new_artist)
                    if input_id_sent_to_api != id_from_api_response:
                        logger.info(f"Spotify API mapped input artist ID {input_id_sent_to_api} to {id_from_api_response} ({artist_name}).")
                else:
                    logger.warning(f"Artist ID {input_id_sent_to_api} from input data not found, failed to fetch, or lacked essential fields (ID, name) from Spotify API. This artist and associated tracks will be skipped.")

        # 4. Process Played Tracks & Derive Top Artists data
        artist_play_stats = defaultdict(lambda: {"play_count": 0, "last_played_at": None})
        actual_played_tracks_added = 0

        for track_entry in unrefined.tracks:
            if track_entry.track_id == track_entry.artist_id:
                logger.debug(f"Skipping track {track_entry.track_id} as its ID matches artist_id.")
                continue

            if not track_entry.artist_id:
                logger.warning(f"Track {track_entry.track_id} is missing artist_id in input. Skipping.")
                continue

            input_artist_id_for_track = track_entry.artist_id
            db_artist_id_for_fk = map_input_artist_id_to_db_id.get(input_artist_id_for_track)

            if not db_artist_id_for_fk:
                logger.warning(f"Skipping track {track_entry.track_id} (input artist: {input_artist_id_for_track}) because its artist was not resolved or mapped to a DB artist ID.")
                continue

            listened_at_dt = parse_timestamp(track_entry.listened_at)
            played_track = PlayedTrack(
                user_id_hash=refined_user.id_hash,
                track_id=track_entry.track_id,
                artist_id=db_artist_id_for_fk,
                duration_ms=track_entry.duration_ms,
                listened_at=listened_at_dt
            )
            models_to_save.append(played_track)
            actual_played_tracks_added += 1

            artist_play_stats[db_artist_id_for_fk]["play_count"] += 1
            current_last_played = artist_play_stats[db_artist_id_for_fk]["last_played_at"]
            if current_last_played is None or listened_at_dt > current_last_played:
                artist_play_stats[db_artist_id_for_fk]["last_played_at"] = listened_at_dt

        # 5. Create UserTopArtistAssoc from derived data
        actual_top_artists_added = 0
        if actual_played_tracks_added > 0:
            logger.info(f"Deriving top artists from {len(artist_play_stats)} unique played artists.")
            for art_id_in_db, stats in artist_play_stats.items():
                if art_id_in_db not in artists_in_db_cache:
                    logger.error(f"CRITICAL LOGIC ERROR: Artist ID {art_id_in_db} in play_stats but not in artists_in_db_cache. Skipping UserTopArtistAssoc.")
                    continue

                top_artist_assoc = UserTopArtistAssoc(
                    user_id_hash=refined_user.id_hash,
                    artist_id=art_id_in_db,
                    play_count=stats["play_count"],
                    last_played_at=stats["last_played_at"]
                )
                models_to_save.append(top_artist_assoc)
                actual_top_artists_added += 1

        logger.info(f"Data transformation for this file yielded {len(models_to_save)} model instances. "
                    f"Artists in DB: {len(artists_in_db_cache)}. "
                    f"Played tracks added: {actual_played_tracks_added}. "
                    f"Top artist associations added: {actual_top_artists_added}.")

        # Log the total API calls made by the Spotify client for processing this input file
        logger.info(f"Spotify API client made approximately {self.spotify_client.api_call_count} calls for this transformation.")

        return models_to_save