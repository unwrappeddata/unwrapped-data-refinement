from datetime import datetime
from sqlalchemy import Column, String, Integer, Float, Boolean, DateTime, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Base model for SQLAlchemy
Base = declarative_base()

class UnwrappedProof(Base):
    __tablename__ = 'unwrapped_proofs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    file_id = Column(Integer, nullable=False, unique=True, index=True)
    dlp_id = Column(Integer, nullable=False)
    is_valid = Column(Boolean, nullable=False)
    score = Column(Float, nullable=False)
    authenticity_score = Column(Float, nullable=False)
    ownership_score = Column(Float, nullable=False)
    quality_score = Column(Float, nullable=False)
    uniqueness_score = Column(Float, nullable=False)

    proof_version = Column(String)
    job_id = Column(String)
    owner_address = Column(String)
    account_id_hash = Column(String, nullable=True, index=True) # Nullable if proof is invalid
    error_message = Column(String, nullable=True) # Populated if is_valid is False

    processed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Relationships
    attributes = relationship("ProofAttribute", back_populates="proof", uselist=False, cascade="all, delete-orphan")
    source_file_metadata = relationship("SourceFileMetadata", back_populates="proof", uselist=False, cascade="all, delete-orphan")

class ProofAttribute(Base):
    __tablename__ = 'proof_attributes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    proof_file_id = Column(Integer, ForeignKey('unwrapped_proofs.file_id'), nullable=False, unique=True)

    track_count = Column(Integer)
    total_minutes_listened = Column(Integer)
    is_data_validated = Column(Boolean)
    activity_period_days = Column(Integer)
    unique_artist_count = Column(Integer)
    was_previously_contributed = Column(Boolean)
    times_rewarded = Column(Integer)
    total_points_raw = Column(Integer)
    differential_points_raw = Column(Integer)

    # Relationship
    proof = relationship("UnwrappedProof", back_populates="attributes")
    points_breakdown = relationship("PointsBreakdownScore", back_populates="attribute_detail", uselist=False, cascade="all, delete-orphan")

class PointsBreakdownScore(Base):
    __tablename__ = 'points_breakdown_scores'

    id = Column(Integer, primary_key=True, autoincrement=True)
    attribute_id = Column(Integer, ForeignKey('proof_attributes.id'), nullable=False, unique=True)

    volume_points = Column(Integer)
    volume_reason = Column(String)
    diversity_points = Column(Integer)
    diversity_reason = Column(String)
    history_points = Column(Integer)
    history_reason = Column(String)

    # Relationship
    attribute_detail = relationship("ProofAttribute", back_populates="points_breakdown")

class SourceFileMetadata(Base):
    __tablename__ = 'source_file_metadata'

    id = Column(Integer, primary_key=True, autoincrement=True)
    proof_file_id = Column(Integer, ForeignKey('unwrapped_proofs.file_id'), nullable=False, unique=True)

    source_system = Column(String) # e.g., "TEE"
    source_file_url = Column(String)
    encrypted_checksum = Column(String)
    decrypted_checksum = Column(String)

    # Relationship
    proof = relationship("UnwrappedProof", back_populates="source_file_metadata")