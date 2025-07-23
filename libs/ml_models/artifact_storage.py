"""Artifact storage management for ML experiments."""

import hashlib
import json
import shutil
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel


class ArtifactMetadata(BaseModel):
    """Metadata for stored artifacts."""

    name: str
    size: int
    content_type: str
    checksum: str
    upload_time: datetime
    run_uuid: str
    artifact_path: str


class ArtifactInfo(BaseModel):
    """Artifact information response."""

    name: str
    size: int
    content_type: str
    upload_time: datetime
    artifact_path: str
    download_url: str | None = None


class ArtifactManager:
    """Manager for ML experiment artifacts."""

    def __init__(self, base_path: str | Path = "./mlruns"):
        """Initialize artifact manager.

        Args:
            base_path: Base directory for storing artifacts
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(exist_ok=True)

    def _get_run_artifacts_path(self, run_uuid: str) -> Path:
        """Get the artifacts directory for a specific run."""
        return self.base_path / run_uuid / "artifacts"

    def _calculate_checksum(self, file_content: bytes) -> str:
        """Calculate MD5 checksum of file content."""
        return hashlib.md5(file_content).hexdigest()

    def _save_metadata(self, metadata: ArtifactMetadata, artifacts_path: Path) -> None:
        """Save artifact metadata to JSON file."""
        metadata_path = artifacts_path / f"{metadata.name}.metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata.model_dump(mode="json"), f, indent=2, default=str)

    def _load_metadata(
        self, artifact_name: str, artifacts_path: Path
    ) -> ArtifactMetadata | None:
        """Load artifact metadata from JSON file."""
        metadata_path = artifacts_path / f"{artifact_name}.metadata.json"
        if not metadata_path.exists():
            return None

        with open(metadata_path) as f:
            data = json.load(f)
            # Convert ISO string back to datetime
            data["upload_time"] = datetime.fromisoformat(data["upload_time"])
            return ArtifactMetadata(**data)

    async def store_artifact(
        self,
        run_uuid: str,
        artifact_name: str,
        file_content: bytes,
        content_type: str = "application/octet-stream",
        artifact_path: str | None = None,
    ) -> ArtifactMetadata:
        """Store an artifact for a run.

        Args:
            run_uuid: UUID of the experiment run
            artifact_name: Name of the artifact
            file_content: Binary content of the artifact
            content_type: MIME type of the artifact
            artifact_path: Custom path within the run's artifacts directory

        Returns:
            Metadata of the stored artifact
        """
        artifacts_path = self._get_run_artifacts_path(run_uuid)
        artifacts_path.mkdir(parents=True, exist_ok=True)

        # Determine storage path
        if artifact_path:
            storage_path = artifacts_path / artifact_path
            storage_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            storage_path = artifacts_path / artifact_name

        # Calculate checksum
        checksum = self._calculate_checksum(file_content)

        # Save the artifact
        with open(storage_path, "wb") as f:
            f.write(file_content)

        # Create metadata
        metadata = ArtifactMetadata(
            name=artifact_name,
            size=len(file_content),
            content_type=content_type,
            checksum=checksum,
            upload_time=datetime.utcnow(),
            run_uuid=run_uuid,
            artifact_path=artifact_path or artifact_name,
        )

        # Save metadata
        self._save_metadata(metadata, artifacts_path)

        return metadata

    async def store_artifact_from_file(
        self,
        run_uuid: str,
        artifact_name: str,
        file_path: str | Path,
        content_type: str = "application/octet-stream",
        artifact_path: str | None = None,
    ) -> ArtifactMetadata:
        """Store an artifact from a file.

        Args:
            run_uuid: UUID of the experiment run
            artifact_name: Name of the artifact
            file_path: Path to the source file
            content_type: MIME type of the artifact
            artifact_path: Custom path within the run's artifacts directory

        Returns:
            Metadata of the stored artifact
        """
        source_path = Path(file_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {file_path}")

        with open(source_path, "rb") as f:
            file_content = f.read()

        return await self.store_artifact(
            run_uuid=run_uuid,
            artifact_name=artifact_name,
            file_content=file_content,
            content_type=content_type,
            artifact_path=artifact_path,
        )

    async def get_artifact(
        self,
        run_uuid: str,
        artifact_name: str,
    ) -> tuple[bytes, ArtifactMetadata] | None:
        """Retrieve an artifact and its metadata.

        Args:
            run_uuid: UUID of the experiment run
            artifact_name: Name of the artifact

        Returns:
            Tuple of (file_content, metadata) or None if not found
        """
        artifacts_path = self._get_run_artifacts_path(run_uuid)

        # Load metadata
        metadata = self._load_metadata(artifact_name, artifacts_path)
        if not metadata:
            return None

        # Load artifact content
        artifact_file_path = artifacts_path / metadata.artifact_path
        if not artifact_file_path.exists():
            return None

        with open(artifact_file_path, "rb") as f:
            content = f.read()

        # Verify checksum
        calculated_checksum = self._calculate_checksum(content)
        if calculated_checksum != metadata.checksum:
            raise ValueError(f"Artifact checksum mismatch for {artifact_name}")

        return content, metadata

    async def list_artifacts(self, run_uuid: str) -> list[ArtifactInfo]:
        """List all artifacts for a run.

        Args:
            run_uuid: UUID of the experiment run

        Returns:
            List of artifact information
        """
        artifacts_path = self._get_run_artifacts_path(run_uuid)
        if not artifacts_path.exists():
            return []

        artifacts = []

        # Find all metadata files
        for metadata_file in artifacts_path.glob("*.metadata.json"):
            artifact_name = metadata_file.stem.replace(".metadata", "")
            metadata = self._load_metadata(artifact_name, artifacts_path)

            if metadata:
                artifacts.append(
                    ArtifactInfo(
                        name=metadata.name,
                        size=metadata.size,
                        content_type=metadata.content_type,
                        upload_time=metadata.upload_time,
                        artifact_path=metadata.artifact_path,
                        download_url=f"/api/v1/experiments/runs/{run_uuid}/artifacts/{artifact_name}",
                    )
                )

        return artifacts

    async def delete_artifact(self, run_uuid: str, artifact_name: str) -> bool:
        """Delete an artifact and its metadata.

        Args:
            run_uuid: UUID of the experiment run
            artifact_name: Name of the artifact

        Returns:
            True if deleted successfully, False if not found
        """
        artifacts_path = self._get_run_artifacts_path(run_uuid)

        # Load metadata to get artifact path
        metadata = self._load_metadata(artifact_name, artifacts_path)
        if not metadata:
            return False

        # Delete artifact file
        artifact_file_path = artifacts_path / metadata.artifact_path
        if artifact_file_path.exists():
            artifact_file_path.unlink()

        # Delete metadata file
        metadata_file_path = artifacts_path / f"{artifact_name}.metadata.json"
        if metadata_file_path.exists():
            metadata_file_path.unlink()

        return True

    async def copy_artifacts_to_run(
        self,
        source_run_uuid: str,
        target_run_uuid: str,
        artifact_names: list[str] | None = None,
    ) -> list[ArtifactMetadata]:
        """Copy artifacts from one run to another.

        Args:
            source_run_uuid: UUID of the source run
            target_run_uuid: UUID of the target run
            artifact_names: List of specific artifacts to copy (None for all)

        Returns:
            List of copied artifact metadata
        """
        source_artifacts = await self.list_artifacts(source_run_uuid)

        if artifact_names:
            source_artifacts = [a for a in source_artifacts if a.name in artifact_names]

        copied_artifacts = []

        for artifact_info in source_artifacts:
            # Get the artifact content
            result = await self.get_artifact(source_run_uuid, artifact_info.name)
            if result:
                content, metadata = result

                # Store in target run
                new_metadata = await self.store_artifact(
                    run_uuid=target_run_uuid,
                    artifact_name=metadata.name,
                    file_content=content,
                    content_type=metadata.content_type,
                    artifact_path=metadata.artifact_path,
                )
                copied_artifacts.append(new_metadata)

        return copied_artifacts

    def get_artifacts_size(self, run_uuid: str) -> int:
        """Get total size of all artifacts for a run.

        Args:
            run_uuid: UUID of the experiment run

        Returns:
            Total size in bytes
        """
        artifacts_path = self._get_run_artifacts_path(run_uuid)
        if not artifacts_path.exists():
            return 0

        total_size = 0
        for file_path in artifacts_path.rglob("*"):
            if file_path.is_file() and not file_path.name.endswith(".metadata.json"):
                total_size += file_path.stat().st_size

        return total_size

    def cleanup_run_artifacts(self, run_uuid: str) -> bool:
        """Delete all artifacts for a run.

        Args:
            run_uuid: UUID of the experiment run

        Returns:
            True if cleanup was successful
        """
        artifacts_path = self._get_run_artifacts_path(run_uuid)
        if artifacts_path.exists():
            shutil.rmtree(artifacts_path)
            return True
        return False
