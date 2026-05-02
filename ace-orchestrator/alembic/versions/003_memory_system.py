"""memory schema: episodic, semantic, procedural, affective + dreaming + council + adaptation

Revision ID: 003
Revises: 002
Create Date: 2026-04-21
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SCHEMA = "memory"


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

    # =========================================================
    # EPISODIC MEMORY
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.episodes (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            conversation_id UUID REFERENCES orchestrator.conversations(id) ON DELETE SET NULL,
            message_id UUID REFERENCES orchestrator.messages(id) ON DELETE SET NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            event_type TEXT NOT NULL,
            participants JSONB NOT NULL DEFAULT '[]'::jsonb,
            domain TEXT,
            summary TEXT NOT NULL,
            full_text TEXT NOT NULL,
            linked_case_id TEXT,
            linked_entity_ids UUID[],
            salience NUMERIC(4,3) NOT NULL DEFAULT 0.5,
            emotional_valence NUMERIC(4,3) DEFAULT 0,
            embedding vector(1024),
            retention_tier TEXT NOT NULL DEFAULT 'full',
            last_accessed_at TIMESTAMPTZ,
            access_count INTEGER NOT NULL DEFAULT 0,
            shared_with_users UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_episodes_user_time ON {SCHEMA}.episodes(user_id, occurred_at DESC)"
    )
    op.execute(
        f"CREATE INDEX idx_episodes_user_salience ON {SCHEMA}.episodes(user_id, salience DESC) "
        f"WHERE retention_tier != 'deleted'"
    )
    op.execute(
        f"CREATE INDEX idx_episodes_user_case ON {SCHEMA}.episodes(user_id, linked_case_id) "
        f"WHERE linked_case_id IS NOT NULL"
    )
    op.execute(
        f"CREATE INDEX idx_episodes_domain ON {SCHEMA}.episodes(domain, occurred_at DESC)"
    )
    op.execute(
        f"CREATE INDEX idx_episodes_embedding ON {SCHEMA}.episodes "
        f"USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100)"
    )

    # =========================================================
    # SEMANTIC MEMORY
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.semantic_entities (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            entity_type TEXT NOT NULL,
            canonical_name TEXT NOT NULL,
            aliases TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
            attributes JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            first_seen_at TIMESTAMPTZ NOT NULL,
            last_reinforced_at TIMESTAMPTZ,
            reference_count INTEGER NOT NULL DEFAULT 1,
            embedding vector(1024),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE UNIQUE INDEX uniq_entities_user_canonical ON {SCHEMA}.semantic_entities(user_id, entity_type, canonical_name)"
    )
    op.execute(
        f"CREATE INDEX idx_entities_type ON {SCHEMA}.semantic_entities(user_id, entity_type)"
    )
    op.execute(
        f"CREATE INDEX idx_entities_embedding ON {SCHEMA}.semantic_entities "
        f"USING ivfflat (embedding vector_cosine_ops) WITH (lists = 50)"
    )

    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.semantic_claims (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            subject_id UUID NOT NULL REFERENCES {SCHEMA}.semantic_entities(id) ON DELETE CASCADE,
            predicate TEXT NOT NULL,
            object_id UUID REFERENCES {SCHEMA}.semantic_entities(id) ON DELETE CASCADE,
            object_value TEXT,
            confidence NUMERIC(4,3) NOT NULL DEFAULT 0.5,
            source_episode_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
            valid_from TIMESTAMPTZ,
            valid_to TIMESTAMPTZ,
            superseded_by UUID REFERENCES {SCHEMA}.semantic_claims(id),
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_claims_subject ON {SCHEMA}.semantic_claims(subject_id, predicate)"
    )
    op.execute(
        f"CREATE INDEX idx_claims_valid ON {SCHEMA}.semantic_claims(user_id) WHERE valid_to IS NULL"
    )

    # =========================================================
    # PROCEDURAL MEMORY
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.procedures (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            name TEXT NOT NULL,
            description TEXT,
            domain TEXT,
            trigger_conditions JSONB NOT NULL,
            steps JSONB NOT NULL,
            source_episode_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
            observed_count INTEGER NOT NULL DEFAULT 1,
            confidence NUMERIC(4,3) NOT NULL DEFAULT 0.5,
            last_reinforced_at TIMESTAMPTZ,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_procedures_user_active ON {SCHEMA}.procedures(user_id, active, confidence DESC)"
    )

    # =========================================================
    # AFFECTIVE MEMORY
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.affects (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            subject_type TEXT NOT NULL,
            subject_ref TEXT NOT NULL,
            affect_type TEXT NOT NULL,
            intensity NUMERIC(4,3) NOT NULL DEFAULT 0.5,
            direction NUMERIC(4,3) NOT NULL DEFAULT 0,
            source_episode_ids UUID[] NOT NULL DEFAULT ARRAY[]::UUID[],
            confidence NUMERIC(4,3) NOT NULL DEFAULT 0.5,
            last_reinforced_at TIMESTAMPTZ,
            active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE UNIQUE INDEX uniq_affects_subject ON {SCHEMA}.affects(user_id, subject_type, subject_ref, affect_type)"
    )

    # =========================================================
    # DREAMING (CONSOLIDATION)
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.dream_reports (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            kind TEXT NOT NULL,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            episodes_processed INTEGER NOT NULL,
            patterns_extracted INTEGER NOT NULL DEFAULT 0,
            entities_created INTEGER NOT NULL DEFAULT 0,
            entities_reinforced INTEGER NOT NULL DEFAULT 0,
            claims_added INTEGER NOT NULL DEFAULT 0,
            claims_superseded INTEGER NOT NULL DEFAULT 0,
            procedures_added INTEGER NOT NULL DEFAULT 0,
            procedures_reinforced INTEGER NOT NULL DEFAULT 0,
            affects_added INTEGER NOT NULL DEFAULT 0,
            episodes_compressed INTEGER NOT NULL DEFAULT 0,
            episodes_deleted INTEGER NOT NULL DEFAULT 0,
            hypotheses JSONB NOT NULL DEFAULT '[]'::jsonb,
            significant_hypotheses_count INTEGER NOT NULL DEFAULT 0,
            surfaced_to_user_at TIMESTAMPTZ,
            cost_usd NUMERIC(10,4),
            input_tokens INTEGER,
            output_tokens INTEGER,
            model TEXT,
            duration_ms INTEGER,
            narrative TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_dreams_user_time ON {SCHEMA}.dream_reports(user_id, created_at DESC)"
    )

    # =========================================================
    # COUNCIL
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.council_sessions (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            conversation_id UUID REFERENCES orchestrator.conversations(id) ON DELETE SET NULL,
            message_id UUID REFERENCES orchestrator.messages(id) ON DELETE SET NULL,
            tier TEXT NOT NULL,
            trigger_reason TEXT NOT NULL,
            personas_convened TEXT[] NOT NULL,
            framing_prompt TEXT,
            rounds JSONB NOT NULL DEFAULT '[]'::jsonb,
            synthesis TEXT,
            recommendation TEXT,
            user_accepted BOOLEAN,
            user_response TEXT,
            cost_usd NUMERIC(10,4),
            input_tokens INTEGER,
            output_tokens INTEGER,
            duration_ms INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_council_user_time ON {SCHEMA}.council_sessions(user_id, created_at DESC)"
    )

    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.council_gate_log (
            id BIGSERIAL PRIMARY KEY,
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            message_id UUID,
            category TEXT,
            classifier_score NUMERIC(4,3),
            tier_recommended TEXT,
            tier_actual TEXT,
            override_reason TEXT,
            classifier_reasoning TEXT,
            council_session_id UUID REFERENCES {SCHEMA}.council_sessions(id) ON DELETE SET NULL,
            recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_gate_log_user_time ON {SCHEMA}.council_gate_log(user_id, recorded_at DESC)"
    )

    # =========================================================
    # ADAPTATION AGENT
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.adaptation_reports (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            window_start TIMESTAMPTZ NOT NULL,
            window_end TIMESTAMPTZ NOT NULL,
            metrics JSONB NOT NULL,
            observations JSONB NOT NULL DEFAULT '[]'::jsonb,
            autonomous_changes JSONB NOT NULL DEFAULT '[]'::jsonb,
            recommendations JSONB NOT NULL DEFAULT '[]'::jsonb,
            questions_for_principal JSONB NOT NULL DEFAULT '[]'::jsonb,
            watch_list JSONB NOT NULL DEFAULT '[]'::jsonb,
            report_markdown TEXT NOT NULL,
            ace_document_id TEXT,
            surfaced_to_user_at TIMESTAMPTZ,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )

    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.adaptation_changes (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            report_id UUID REFERENCES {SCHEMA}.adaptation_reports(id) ON DELETE SET NULL,
            parameter_path TEXT NOT NULL,
            before_value JSONB NOT NULL,
            after_value JSONB NOT NULL,
            rationale TEXT NOT NULL,
            authority TEXT NOT NULL,
            applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            reverted_at TIMESTAMPTZ
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_adaptation_changes_user ON {SCHEMA}.adaptation_changes(user_id, applied_at DESC)"
    )

    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.adaptation_recommendations (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            report_id UUID NOT NULL REFERENCES {SCHEMA}.adaptation_reports(id) ON DELETE CASCADE,
            proposal JSONB NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            resolved_at TIMESTAMPTZ,
            resolved_by TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        f"CREATE INDEX idx_adapt_recs_pending ON {SCHEMA}.adaptation_recommendations(user_id, status)"
    )

    # =========================================================
    # RUNTIME CONFIG
    # =========================================================
    op.execute(
        f"""
        CREATE TABLE {SCHEMA}.system_config (
            user_id UUID NOT NULL REFERENCES orchestrator.users(id) ON DELETE CASCADE,
            parameter_path TEXT NOT NULL,
            value JSONB NOT NULL,
            source TEXT NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (user_id, parameter_path)
        )
        """
    )

    # =========================================================
    # ALTER orchestrator.conversations: add council_mode (Phase 2 §11.3)
    # =========================================================
    op.add_column(
        "conversations",
        sa.Column("council_mode", sa.Text, nullable=True),
        schema="orchestrator",
    )


def downgrade() -> None:
    op.drop_column("conversations", "council_mode", schema="orchestrator")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.system_config")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.adaptation_recommendations")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.adaptation_changes")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.adaptation_reports")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.council_gate_log")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.council_sessions")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.dream_reports")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.affects")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.procedures")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.semantic_claims")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.semantic_entities")
    op.execute(f"DROP TABLE IF EXISTS {SCHEMA}.episodes")
    op.execute(f"DROP SCHEMA IF EXISTS {SCHEMA}")
