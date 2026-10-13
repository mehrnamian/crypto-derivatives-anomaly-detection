from __future__ import annotations

import tempfile
from pathlib import Path, PurePosixPath

from azure.core.exceptions import ResourceExistsError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

from derivs_anomaly.config import Settings
from derivs_anomaly.pipeline.process import process_bronze


class AzureLakeRunner:
    """Stage ADLS Gen2 files locally, run deterministic jobs, and sync outputs back."""

    OUTPUT_FILESYSTEMS = ("silver", "gold", "quarantine")

    def __init__(self, settings: Settings) -> None:
        account_url = settings.storage_account_url
        if not account_url or not account_url.startswith("https://"):
            raise ValueError("DERIVS_STORAGE_ACCOUNT_URL must be an HTTPS ADLS Gen2 endpoint")
        self.settings = settings
        self.credential = DefaultAzureCredential()
        self.service = DataLakeServiceClient(account_url=account_url, credential=self.credential)

    def process(self) -> dict[str, int | float]:
        with tempfile.TemporaryDirectory(prefix="derivs-anomaly-") as temp_dir:
            local_root = Path(temp_dir)
            self._download_bronze(local_root / "bronze")
            self._download_existing_gold(local_root / "gold")
            result = process_bronze(
                local_root,
                model_version=self.settings.model_version,
                threshold=self.settings.zscore_threshold,
            )
            for filesystem_name in self.OUTPUT_FILESYSTEMS:
                self._upload_tree(filesystem_name, local_root / filesystem_name)
            return result

    def _download_bronze(self, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        filesystem = self.service.get_file_system_client("bronze")
        for item in filesystem.get_paths(path=None, recursive=True):
            if item.is_directory:
                continue
            relative = PurePosixPath(item.name)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError("unexpected unsafe path returned from bronze storage")
            destination = target.joinpath(*relative.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            downloader = filesystem.get_file_client(item.name).download_file()
            destination.write_bytes(downloader.readall())

    def _upload_tree(self, filesystem_name: str, root: Path) -> None:
        filesystem = self.service.get_file_system_client(filesystem_name)
        if not root.exists():
            return
        for path in root.rglob("*"):
            if not path.is_file() or path.name.startswith("."):
                continue
            relative = path.relative_to(root).as_posix()
            parent = PurePosixPath(relative).parent
            for index in range(1, len(parent.parts) + 1):
                directory_name = "/".join(parent.parts[:index])
                try:
                    filesystem.get_directory_client(directory_name).create_directory()
                except ResourceExistsError:
                    pass
            filesystem.get_file_client(relative).upload_data(path.read_bytes(), overwrite=True)

    def _download_existing_gold(self, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        filesystem = self.service.get_file_system_client("gold")
        for name in ("minutes.parquet", "anomaly_scores.parquet", "gaps.jsonl"):
            try:
                downloader = filesystem.get_file_client(name).download_file()
            except Exception as exc:
                if getattr(exc, "status_code", None) == 404:
                    continue
                raise
            (target / name).write_bytes(downloader.readall())

    def close(self) -> None:
        self.service.close()
        self.credential.close()


def process_azure_lake(settings: Settings) -> dict[str, int | float]:
    runner = AzureLakeRunner(settings)
    try:
        return runner.process()
    finally:
        runner.close()
