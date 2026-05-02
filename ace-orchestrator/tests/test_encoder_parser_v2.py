"""M2.2 additions to the encoder JSON parser tests."""
from orchestrator.services.memory_system.encoder import EncodedEpisode, MemoryEncoder


def test_parses_full_v2_episode_with_entities_and_claims():
    raw = """
    {
      "summary": "Asked about Carlisle's appointment.",
      "full_text": "Will asked when Carlisle was appointed. Athena confirmed April 1, 2026.",
      "domain": "legal",
      "linked_case_id": "biia-25-18153",
      "salience": 0.85,
      "emotional_valence": 0.0,
      "participants": ["user:will.nelson", "athena"],
      "entities": [
        {
          "entity_type": "person",
          "canonical_name": "Chris Carlisle",
          "aliases": ["Carlisle"],
          "attributes": {"firm": "CBCL"}
        }
      ],
      "claims": [
        {
          "subject": "Chris Carlisle",
          "subject_type": "person",
          "predicate": "appointed_on",
          "object": null,
          "object_type": null,
          "object_value": "2026-04-01",
          "confidence": 0.95
        }
      ]
    }
    """
    out = MemoryEncoder._parse_encoder_output(raw)
    assert isinstance(out, EncodedEpisode)
    assert len(out.entities) == 1
    assert out.entities[0].canonical_name == "Chris Carlisle"
    assert out.entities[0].aliases == ["Carlisle"]
    assert len(out.claims) == 1
    assert out.claims[0].predicate == "appointed_on"
    assert out.claims[0].object_value == "2026-04-01"


def test_parses_episode_with_empty_entities_and_claims():
    raw = """
    {
      "summary": "Hi.",
      "full_text": "User said hi. Athena said hi back.",
      "salience": 0.05,
      "emotional_valence": 0.1,
      "participants": ["user:will.nelson", "athena"],
      "entities": [],
      "claims": []
    }
    """
    out = MemoryEncoder._parse_encoder_output(raw)
    assert out.entities == []
    assert out.claims == []


def test_parses_episode_without_entities_field_at_all():
    """Older encoder responses (M2.1) have no entities/claims keys; we must still parse them."""
    raw = '{"summary":"x","full_text":"y","salience":0.5}'
    out = MemoryEncoder._parse_encoder_output(raw)
    assert out.entities == []
    assert out.claims == []


def test_rejects_claim_with_missing_predicate():
    raw = """
    {
      "summary": "x",
      "full_text": "y",
      "salience": 0.5,
      "claims": [
        {"subject": "X", "subject_type": "person", "object": null, "object_type": null, "object_value": "z", "confidence": 0.5}
      ]
    }
    """
    import pytest
    with pytest.raises(ValueError):
        MemoryEncoder._parse_encoder_output(raw)
