from typing import Dict, Any, List
from refiner.models.refined import Base, UnwrappedProof, ProofAttribute, PointsBreakdownScore, SourceFileMetadata
from refiner.transformer.base_transformer import DataTransformer
from refiner.models.unrefined import UnwrappedProofInput, AttributesInputValid, AttributesInputError
import logging

class UnwrappedTransformer(DataTransformer):
    """
    Transformer for Unwrapped Proof of Contribution data.
    """

    def transform(self, data: Dict[str, Any]) -> List[Base]:
        """
        Transform raw Unwrapped proof data into SQLAlchemy model instances.

        Args:
            data: Dictionary containing Unwrapped proof data

        Returns:
            List of SQLAlchemy model instances
        """
        # Validate data with Pydantic
        try:
            unrefined_proof = UnwrappedProofInput.model_validate(data)
        except Exception as e:
            logging.error(f"Pydantic validation failed for input data: {e}")
            # log and skip this record.
            return []


        models_to_save = []

        # Create UnwrappedProof instance
        proof_instance = UnwrappedProof(
            file_id=unrefined_proof.metadata.file_id,
            dlp_id=unrefined_proof.dlp_id,
            is_valid=unrefined_proof.valid,
            score=unrefined_proof.score,
            authenticity_score=unrefined_proof.authenticity,
            ownership_score=unrefined_proof.ownership,
            quality_score=unrefined_proof.quality,
            uniqueness_score=unrefined_proof.uniqueness,
            proof_version=unrefined_proof.metadata.version,
            job_id=unrefined_proof.metadata.job_id,
            owner_address=unrefined_proof.metadata.owner_address
            # processed_at is handled by default in the model
        )

        if unrefined_proof.valid and isinstance(unrefined_proof.attributes, AttributesInputValid):
            valid_attributes = unrefined_proof.attributes
            proof_instance.account_id_hash = valid_attributes.account_id_hash

            # Create ProofAttribute instance
            attribute_instance = ProofAttribute(

                # proof_file_id=proof_instance.file_id,
                track_count=valid_attributes.track_count,
                total_minutes_listened=valid_attributes.total_minutes,
                is_data_validated=valid_attributes.data_validated,
                activity_period_days=valid_attributes.activity_period_days,
                unique_artist_count=valid_attributes.unique_artists,
                was_previously_contributed=valid_attributes.previously_contributed,
                times_rewarded=valid_attributes.times_rewarded,
                total_points_raw=valid_attributes.total_points,
                differential_points_raw=valid_attributes.differential_points
            )

            # Create PointsBreakdownScore instance
            points_breakdown = valid_attributes.points_breakdown
            points_score_instance = PointsBreakdownScore(
                volume_points=points_breakdown.volume_points,
                volume_reason=points_breakdown.volume_reason,
                diversity_points=points_breakdown.diversity_points,
                diversity_reason=points_breakdown.diversity_reason,
                history_points=points_breakdown.history_points,
                history_reason=points_breakdown.history_reason
            )

            attribute_instance.points_breakdown = points_score_instance
            proof_instance.attributes = attribute_instance

        elif not unrefined_proof.valid and isinstance(unrefined_proof.attributes, AttributesInputError):
            proof_instance.error_message = unrefined_proof.attributes.error
            # account_id_hash remains None as per model definition

        # Create SourceFileMetadata instance
        file_meta_input = unrefined_proof.metadata.file
        source_file_instance = SourceFileMetadata(

            # proof_file_id=proof_instance.file_id,
            source_system=file_meta_input.source,
            source_file_url=file_meta_input.url,
            encrypted_checksum=file_meta_input.checksums.encrypted,
            decrypted_checksum=file_meta_input.checksums.decrypted
        )
        proof_instance.source_file_metadata = source_file_instance

        models_to_save.append(proof_instance)

        return models_to_save