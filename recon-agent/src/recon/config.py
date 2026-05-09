from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Required
    database_url: str = "postgresql://recon:recon@localhost:5432/recon"

    # LLM (Phase 3)
    anthropic_api_key: str = ""
    model_triage: str = "claude-haiku-4-5"
    model_invest: str = "claude-sonnet-4-6"
    agent_max_steps: int = 6
    agent_timeout_s: int = 30

    # Rules
    rules_tolerance_paise: int = 1

    # Retry
    retry_initial_s: float = 1.0
    retry_max_attempts: int = 3

    # PII (Phase 6)
    pan_hmac_key: str = ""
    pan_aes_key: str = ""

    # API (Phase 5)
    api_bearer_token: str = "changeme"

    # Kafka (Phase 2)
    kafka_brokers: str = "kafka:9092"
    kafka_consumer_group_prefix: str = "recon"

    # Observability (Phase 6)
    otel_exporter_otlp_endpoint: str = "http://otel-collector:4317"

    # Feature flags
    disable_auto_approve: bool = False
    mock_llm: bool = False


settings = Settings()
