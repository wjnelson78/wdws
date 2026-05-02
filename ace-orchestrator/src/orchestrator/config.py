from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    anthropic_api_key: str
    anthropic_default_model: str = "claude-opus-4-7"
    anthropic_fallback_model: str = "claude-sonnet-4-6"
    anthropic_drafting_model: str = "claude-opus-4-7"

    database_url: str
    ace_database_url: str

    ace_mcp_url: str = "https://klunky.12432.net/mcp/sse"
    ms365_mcp_url: str = "https://microsoft365.mcp.claude.com/mcp"
    investigator_mcp_url: str = ""

    athena_system_prompt_doc_id: str = "897be05c-6a59-481d-a237-1bf0ff393474"

    orchestrator_host: str = "0.0.0.0"
    orchestrator_port: int = 8080
    orchestrator_api_key: str
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    default_cache_ttl: str = "5m"
    drafting_cache_ttl: str = "1h"

    active_system_prompt_version: str = Field(default="v2.0")

    default_user_identifier: str = "will.nelson"
    user_context_cache_ttl_seconds: int = 300

    # --- Memory system (Phase 2) ---
    memory_database_url: str = ""
    memory_database_reader_url: str = ""
    embedding_service_url: str = "http://127.0.0.1:9098"

    classifier_model: str = "claude-sonnet-4-6"
    encoder_model: str = "claude-sonnet-4-6"
    classifier_timeout_seconds: int = 10
    encoder_timeout_seconds: int = 30
    encoder_retry_max_attempts: int = 3

    dreaming_model: str = "claude-opus-4-7"
    dreaming_nightly_cron: str = "0 2 * * *"
    dreaming_weekly_cron: str = "0 2 * * 0"
    dreaming_daily_budget_usd: float = 3.00
    dreaming_significance_threshold: float = 0.70

    council_default_model: str = "claude-sonnet-4-6"
    council_synthesizer_model: str = "claude-opus-4-7"
    council_mini_threshold: float = 0.40
    council_full_threshold: float = 0.75
    council_daily_budget_usd: float = 3.00
    council_soft_cap_percent: int = 75
    council_category_floors: str = '{"legal_filing":"mini","medical_decision":"full"}'

    adaptation_model: str = "claude-opus-4-7"
    adaptation_weekly_cron: str = "0 6 * * 0"
    adaptation_max_auto_changes_per_week: int = 5
    adaptation_report_domain: str = "legal"
    adaptation_report_tag: str = "adaptation-report"

    retention_legal_policy: str = "preserve"
    retention_medical_policy: str = "preserve"
    retention_casual_policy: str = "compress_30_delete_90"
    retention_dev_policy: str = "compress_30"
    retention_default_policy: str = "compress_30"

    memory_encoder_enabled: bool = True
    memory_retrieval_enabled: bool = True
    dreaming_enabled: bool = True
    council_enabled: bool = True
    council_kill_switch: bool = False
    adaptation_enabled: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
