from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DERIVS_",
        env_file=".env",
        extra="ignore",
    )

    exchange: str = "binance"
    symbol: str = "BTCUSDT"
    data_dir: Path = Path("./data/lake")
    api_key: SecretStr | None = None
    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)
    model_version: str = "robust-zscore-v1"
    oi_poll_seconds: int = Field(default=15, ge=1)
    capture_seconds: int = Field(default=300, ge=1)
    watchdog_seconds: int = Field(default=30, ge=1)
    gold_recompute_hours: int = Field(default=2, ge=1)
    alert_merge_minutes: int = Field(default=15, ge=0)
    zscore_threshold: float = Field(default=6.0, gt=0)
    rolling_window_minutes: int = Field(default=1440, ge=2)
    min_rolling_periods: int = Field(default=60, ge=2)
    ws_max_hours: int = Field(default=23, ge=1)
    log_json: bool = True
    models_dir: Path = Path("./data/models")
    eventhub_namespace: str | None = None
    eventhub_name: str | None = None
    storage_account_url: str | None = None
    mlflow_tracking_uri: str | None = None

    bronze_dir: Path = Field(default=Path(), exclude=True)
    silver_dir: Path = Field(default=Path(), exclude=True)
    gold_dir: Path = Field(default=Path(), exclude=True)
    quarantine_dir: Path = Field(default=Path(), exclude=True)

    @field_validator("api_key")
    @classmethod
    def require_strong_api_key(cls, value: SecretStr | None) -> SecretStr | None:
        if value is not None and len(value.get_secret_value()) < 32:
            raise ValueError("api_key must contain at least 32 characters")
        return value

    def model_post_init(self, _context: object) -> None:
        if self.min_rolling_periods > self.rolling_window_minutes:
            raise ValueError("min_rolling_periods cannot exceed rolling_window_minutes")
        object.__setattr__(self, "bronze_dir", self.data_dir / "bronze")
        object.__setattr__(self, "silver_dir", self.data_dir / "silver")
        object.__setattr__(self, "gold_dir", self.data_dir / "gold")
        object.__setattr__(self, "quarantine_dir", self.data_dir / "quarantine")

    def ensure_directories(self) -> None:
        for path in (
            self.bronze_dir,
            self.silver_dir,
            self.gold_dir,
            self.quarantine_dir,
            self.models_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
