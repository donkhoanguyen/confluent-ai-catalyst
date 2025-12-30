"""
CryptoSentinel Settings Module

Loads configuration from environment variables with validation.
"""

from functools import lru_cache
from typing import List, Optional

from pydantic import Field, computed_field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        populate_by_name=True,
    )

    # -------------------------------------------------------------------------
    # Confluent Cloud / Kafka
    # -------------------------------------------------------------------------
    kafka_bootstrap_servers: str = Field(
        ...,
        description="Confluent Cloud bootstrap servers",
    )
    kafka_api_key: str = Field(
        ...,
        description="Kafka API key",
    )
    kafka_api_secret: str = Field(
        ...,
        description="Kafka API secret",
    )
    schema_registry_url: str = Field(
        ...,
        description="Schema Registry URL",
    )
    schema_registry_api_key: str = Field(
        ...,
        description="Schema Registry API key",
    )
    schema_registry_api_secret: str = Field(
        ...,
        description="Schema Registry API secret",
    )

    # -------------------------------------------------------------------------
    # Google AI / Vertex AI
    # -------------------------------------------------------------------------
    gemini_api_key: Optional[str] = Field(
        default=None,
        description="Gemini API key from AI Studio (used when use_vertex_ai=False or as fallback)",
    )
    google_application_credentials: Optional[str] = Field(
        default=None,
        alias="GOOGLE_APPLICATION_CREDENTIALS",
        description="Path to a GCP service account key JSON (used by Vertex AI via ADC).",
    )
    gcp_project_id: Optional[str] = Field(
        default=None,
        description="GCP Project ID for Vertex AI (required when use_vertex_ai=True)",
    )
    gcp_region: str = Field(
        default="us-central1",
        description="GCP region for Vertex AI",
    )
    use_vertex_ai: bool = Field(
        default=True,
        description="Use Vertex AI for structured output (recommended for reliability)",
    )

    @model_validator(mode="after")
    def _validate_llm_settings(self) -> "Settings":
        """
        Validate LLM configuration.

        - If Vertex AI is enabled, require `gcp_project_id`.
        - If Vertex AI is disabled, require `gemini_api_key`.
        """
        if self.use_vertex_ai:
            if not self.gcp_project_id:
                raise ValueError(
                    "GCP_PROJECT_ID is required when USE_VERTEX_AI=true. "
                    "Either set GCP_PROJECT_ID (and auth) or set USE_VERTEX_AI=false to use GEMINI_API_KEY."
                )
        else:
            if not self.gemini_api_key:
                raise ValueError(
                    "GEMINI_API_KEY is required when USE_VERTEX_AI=false. "
                    "Get one from AI Studio and set it in your .env."
                )
        return self

    # -------------------------------------------------------------------------
    # Reddit API
    # -------------------------------------------------------------------------
    reddit_client_id: str = Field(
        ...,
        description="Reddit app client ID",
    )
    reddit_client_secret: str = Field(
        ...,
        description="Reddit app client secret",
    )
    reddit_user_agent: str = Field(
        default="CryptoSentinel/1.0",
        description="Reddit API user agent",
    )

    # -------------------------------------------------------------------------
    # CoinGecko API
    # -------------------------------------------------------------------------
    coingecko_api_key: Optional[str] = Field(
        default=None,
        description="CoinGecko API key (optional for free tier)",
    )

    # -------------------------------------------------------------------------
    # News APIs
    # -------------------------------------------------------------------------
    newsdata_api_key: Optional[str] = Field(
        default=None,
        description="NewsData.io API key (free tier: 200 req/day)",
    )
    gnews_api_key: Optional[str] = Field(
        default=None,
        description="GNews API key (free tier: 100 req/day, backup)",
    )

    # -------------------------------------------------------------------------
    # Application Settings
    # -------------------------------------------------------------------------
    # These are stored as comma-separated strings in .env
    tracked_coins_str: str = Field(
        default="bitcoin,ethereum,solana,dogecoin",
        alias="TRACKED_COINS",
        description="Cryptocurrencies to track (comma-separated)",
    )
    tracked_subreddits_str: str = Field(
        default="cryptocurrency,bitcoin,ethereum,solana,wallstreetbets",
        alias="TRACKED_SUBREDDITS",
        description="Subreddits to monitor (comma-separated)",
    )

    # API Server
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)

    # Streamlit
    streamlit_port: int = Field(default=8501)

    # Logging
    log_level: str = Field(default="INFO")

    # Offline/demo mode
    offline_mode: bool = Field(
        default=False,
        description="Run in offline demo mode (no external services)",
    )

    # -------------------------------------------------------------------------
    # Agent Configuration
    # -------------------------------------------------------------------------
    agent_mode: bool = Field(
        default=False,
        description="Enable autonomous agent mode",
    )
    mcp_server_url: Optional[str] = Field(
        default=None,
        description="MCP server URL for Confluent operations",
    )
    confluent_admin_api_url: Optional[str] = Field(
        default=None,
        description="Confluent Admin API URL (fallback if MCP not available)",
    )
    max_hypotheses: int = Field(
        default=10,
        description="Maximum number of concurrent hypotheses",
    )
    discovery_timeout: int = Field(
        default=3600,
        description="Maximum time per discovery cycle (seconds)",
    )

    # -------------------------------------------------------------------------
    # Computed Properties
    # -------------------------------------------------------------------------
    @computed_field
    def tracked_coins(self) -> List[str]:
        """Get list of tracked coins."""
        return [c.strip() for c in self.tracked_coins_str.split(",") if c.strip()]

    @computed_field
    def tracked_subreddits(self) -> List[str]:
        """Get list of tracked subreddits."""
        return [s.strip() for s in self.tracked_subreddits_str.split(",") if s.strip()]

    # -------------------------------------------------------------------------
    # Kafka Configuration Helpers
    # -------------------------------------------------------------------------
    def get_kafka_producer_config(self) -> dict:
        """Get Kafka producer configuration."""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka_api_key,
            "sasl.password": self.kafka_api_secret,
        }

    def get_kafka_consumer_config(self, group_id: str) -> dict:
        """Get Kafka consumer configuration."""
        return {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": self.kafka_api_key,
            "sasl.password": self.kafka_api_secret,
        }

    def get_schema_registry_config(self) -> dict:
        """Get Schema Registry configuration."""
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}",
        }


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()

