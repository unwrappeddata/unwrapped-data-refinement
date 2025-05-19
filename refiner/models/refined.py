from datetime import datetime
from sqlalchemy import Column, String, Integer, Float, DateTime, ForeignKey, Text, JSON
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base # Use this for SQLAlchemy Base

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id_hash = Column(String, primary_key=True, index=True)
    country = Column(String, nullable=True)
    product = Column(String, nullable=True)
    file_id = Column(Integer, index=True, nullable=True) # From environment variable

    listening_stats = relationship("UserListeningStats", back_populates="user", uselist=False, cascade="all, delete-orphan")
    played_tracks = relationship("PlayedTrack", back_populates="user", cascade="all, delete-orphan")
    top_artists_assoc = relationship("UserTopArtistAssoc", back_populates="user", cascade="all, delete-orphan")

class UserListeningStats(Base):
    __tablename__ = 'user_listening_stats'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id_hash = Column(String, ForeignKey('users.id_hash'), nullable=False, index=True, unique=True)
    total_minutes = Column(Integer, nullable=False)
    track_count = Column(Integer, nullable=False)
    unique_artists_count = Column(Integer, nullable=False)
    activity_period_days = Column(Integer, nullable=False)
    first_listen_at = Column(DateTime, nullable=True)
    last_listen_at = Column(DateTime, nullable=True)
    refined_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="listening_stats")

class Artist(Base):
    __tablename__ = 'artists'
    id = Column(String, primary_key=True, index=True) # Spotify artist ID
    name = Column(String, nullable=False) # Will use placeholder if name not in top_artists
    popularity = Column(Integer, nullable=True)
    played_tracks = relationship("PlayedTrack", back_populates="artist")
    top_artist_for_users_assoc = relationship("UserTopArtistAssoc", back_populates="artist")

class PlayedTrack(Base):
    __tablename__ = 'played_tracks'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id_hash = Column(String, ForeignKey('users.id_hash'), nullable=False, index=True)
    track_id = Column(String, nullable=False, index=True)
    artist_id = Column(String, ForeignKey('artists.id'), nullable=False, index=True)
    duration_ms = Column(Integer, nullable=False)
    listened_at = Column(DateTime, nullable=False, index=True)

    user = relationship("User", back_populates="played_tracks")
    artist = relationship("Artist", back_populates="played_tracks")

class UserTopArtistAssoc(Base):
    __tablename__ = 'user_top_artists'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id_hash = Column(String, ForeignKey('users.id_hash'), nullable=False, index=True)
    artist_id = Column(String, ForeignKey('artists.id'), nullable=False, index=True)
    play_count = Column(Integer, nullable=True)
    last_played_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="top_artists_assoc")
    artist = relationship("Artist", back_populates="top_artist_for_users_assoc")