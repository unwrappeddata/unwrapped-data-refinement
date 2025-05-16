import logging
from typing import Dict, Any, List
from refiner.models.refined import Base, User, UserListeningStats, Artist, PlayedTrack, UserTopArtistAssoc
from refiner.transformer.base_transformer import DataTransformer
from refiner.models.unrefined import UnwrappedData
from refiner.utils.date import parse_timestamp
from refiner.config import settings # To get FILE_ID

class UnwrappedSpotifyTransformer(DataTransformer):
    """
    Transformer for Unwrapped Spotify data.
    """

    def transform(self, data: Dict[str, Any]) -> List[Base]:
        """
        Transform raw Unwrapped Spotify data into SQLAlchemy model instances.
        """
        unrefined = UnwrappedData.model_validate(data)
        models_to_save: List[Base] = []
        artists_cache: Dict[str, Artist] = {} # Cache for artist objects

        # 1. Create User
        refined_user = User(
            id_hash=unrefined.user.id_hash,
            country=unrefined.user.country,
            product=unrefined.user.product,
            file_id=settings.FILE_ID # Get FILE_ID from settings
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

        # 3. Process Top Artists first to populate artist details
        if unrefined.top_artists_medium_term:
            for top_artist_data in unrefined.top_artists_medium_term:
                if top_artist_data.id not in artists_cache:
                    primary_image = top_artist_data.images[0].url if top_artist_data.images else None
                    followers = top_artist_data.followers.total if top_artist_data.followers else None

                    artist = Artist(
                        id=top_artist_data.id,
                        name=top_artist_data.name,
                        popularity=top_artist_data.popularity,
                        genres=top_artist_data.genres, # Will be stored as JSON
                        followers_total=followers,
                        primary_image_url=primary_image
                    )
                    artists_cache[artist.id] = artist
                    models_to_save.append(artist)
                else: # Artist might exist from tracks, update with more details
                    artist = artists_cache[top_artist_data.id]
                    artist.name = top_artist_data.name # Ensure name is set
                    artist.popularity = top_artist_data.popularity or artist.popularity
                    artist.genres = top_artist_data.genres or artist.genres # Ensure genres are updated
                    artist.followers_total = (top_artist_data.followers.total if top_artist_data.followers else None) or artist.followers_total
                    artist.primary_image_url = (top_artist_data.images[0].url if top_artist_data.images else None) or artist.primary_image_url


                # Create UserTopArtistAssoc
                play_count_int = None
                if top_artist_data.play_count is not None:
                    try:
                        play_count_int = int(top_artist_data.play_count)
                    except ValueError:
                        logging.warning(f"Could not parse play_count '{top_artist_data.play_count}' to int for artist {top_artist_data.id}")

                last_played_dt = parse_timestamp(top_artist_data.last_played) if top_artist_data.last_played else None

                assoc = UserTopArtistAssoc(
                    user_id_hash=unrefined.user.id_hash,
                    artist_id=top_artist_data.id,
                    play_count=play_count_int,
                    last_played_at=last_played_dt
                )
                models_to_save.append(assoc)

        # 4. Process Played Tracks
        for track_data in unrefined.tracks:
            # Ensure artist exists in cache/db
            if track_data.artist_id not in artists_cache:
                # This artist was not in top_artists, create with minimal info
                artist = Artist(
                    id=track_data.artist_id,
                    name=f"[ID: {track_data.artist_id}]" # Placeholder name
                )
                artists_cache[artist.id] = artist
                models_to_save.append(artist)

            # Create PlayedTrack
            played_track = PlayedTrack(
                user_id_hash=unrefined.user.id_hash,
                track_id=track_data.track_id,
                artist_id=track_data.artist_id,
                duration_ms=track_data.duration_ms,
                listened_at=parse_timestamp(track_data.listened_at)
            )
            models_to_save.append(played_track)

        return models_to_save