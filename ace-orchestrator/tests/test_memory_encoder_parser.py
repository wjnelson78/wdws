"""Tests for the encoder's JSON parser. Covers fenced code, validation errors."""
import pytest

from orchestrator.services.memory_system.encoder import EncodedEpisode, MemoryEncoder


def test_parses_plain_json():
    raw = '{"summary":"x","full_text":"y","salience":0.5}'
    out = MemoryEncoder._parse_encoder_output(raw)
    assert isinstance(out, EncodedEpisode)
    assert out.summary == "x"


def test_parses_fenced_json():
    raw = "```json\n{\"summary\":\"x\",\"full_text\":\"y\",\"salience\":0.7}\n```"
    out = MemoryEncoder._parse_encoder_output(raw)
    assert out.salience == 0.7


def test_parses_bare_fenced_block():
    raw = "```\n{\"summary\":\"x\",\"full_text\":\"y\",\"salience\":0.4}\n```"
    out = MemoryEncoder._parse_encoder_output(raw)
    assert out.summary == "x"


def test_rejects_missing_required_fields():
    with pytest.raises(ValueError, match="failed validation"):
        MemoryEncoder._parse_encoder_output('{"summary":"x"}')


def test_rejects_bad_json():
    with pytest.raises(ValueError, match="non-JSON"):
        MemoryEncoder._parse_encoder_output("not json at all")


def test_clamps_out_of_range_salience():
    with pytest.raises(ValueError):
        MemoryEncoder._parse_encoder_output(
            '{"summary":"x","full_text":"y","salience":1.5}'
        )


def test_parses_full_record_with_optional_fields():
    raw = """
    {
      "summary": "Asked about Snohomish PRR deadline.",
      "full_text": "Will asked about the Snohomish County PRR response deadline. Athena confirmed April 24.",
      "domain": "legal",
      "linked_case_id": "2:26-cv-00776-JCC",
      "salience": 0.8,
      "emotional_valence": -0.1,
      "participants": ["user:will.nelson", "athena"]
    }
    """
    out = MemoryEncoder._parse_encoder_output(raw)
    assert out.linked_case_id == "2:26-cv-00776-JCC"
    assert out.participants == ["user:will.nelson", "athena"]
