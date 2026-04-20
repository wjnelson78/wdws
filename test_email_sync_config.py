import unittest

from email_sync_config import (
    TARGET_DOMAINS,
    build_sync_health_report,
    resolve_sync_configuration,
)


class SyncConfigurationTests(unittest.TestCase):
    def test_chelan_county_domain_is_in_code_defaults(self):
        self.assertIn("co.chelan.wa.us", TARGET_DOMAINS)

    def test_db_rule_records_can_be_indexed_objects(self):
        class FakeRecord:
            def __init__(self, **values):
                self._values = values

            def __getitem__(self, key):
                return self._values[key]

        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me"],
            default_domains=["snoco.org"],
            default_specific_emails=[],
            db_rules=[
                FakeRecord(rule_type="domain", pattern="snoco.org"),
                FakeRecord(rule_type="domain", pattern="mhb.com"),
            ],
        )

        self.assertEqual(config.db_domains, ["snoco.org", "mhb.com"])

    def test_cli_domains_override_db_domains(self):
        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me", "athena@seattleseahawks.me"],
            default_domains=["snoco.org", "mhb.com"],
            default_specific_emails=["alpha@example.com"],
            cli_domains=["mhb.com"],
            db_mailboxes=["athena@seattleseahawks.me"],
            db_rules=[
                {"rule_type": "domain", "pattern": "snoco.org"},
                {"rule_type": "domain", "pattern": "playfasa.com"},
            ],
        )

        self.assertTrue(config.explicit_domains)
        self.assertEqual(config.domains, ["mhb.com"])
        self.assertEqual(config.db_domains, ["snoco.org", "playfasa.com"])

    def test_cli_mailboxes_override_db_mailboxes(self):
        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me", "athena@seattleseahawks.me"],
            default_domains=["snoco.org"],
            default_specific_emails=[],
            cli_mailboxes=["william@seattleseahawks.me"],
            db_mailboxes=["athena@seattleseahawks.me"],
            db_rules=[{"rule_type": "domain", "pattern": "snoco.org"}],
        )

        self.assertTrue(config.explicit_mailboxes)
        self.assertEqual(config.mailboxes, ["william@seattleseahawks.me"])
        self.assertEqual(config.db_mailboxes, ["athena@seattleseahawks.me"])

    def test_domain_drift_is_reported(self):
        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me"],
            default_domains=["snoco.org", "mhb.com"],
            default_specific_emails=[],
            db_rules=[
                {"rule_type": "domain", "pattern": "snoco.org"},
                {"rule_type": "domain", "pattern": "playfasa.com"},
            ],
        )

        self.assertEqual(config.code_only_domains, ["mhb.com"])
        self.assertEqual(config.db_only_domains, ["playfasa.com"])

    def test_specific_emails_merge_and_deduplicate(self):
        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me"],
            default_domains=["snoco.org"],
            default_specific_emails=["Alpha@Example.com", "beta@example.com"],
            db_rules=[
                {"rule_type": "email_address", "pattern": "alpha@example.com"},
                {"rule_type": "email_address", "pattern": "gamma@example.com"},
            ],
        )

        self.assertEqual(
            config.specific_emails,
            ["alpha@example.com", "beta@example.com", "gamma@example.com"],
        )

    def test_health_report_is_healthy_when_config_is_aligned(self):
        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me"],
            default_domains=["snoco.org", "mhb.com"],
            default_specific_emails=["alpha@example.com"],
            db_mailboxes=["william@seattleseahawks.me"],
            db_rules=[
                {"rule_type": "domain", "pattern": "snoco.org"},
                {"rule_type": "domain", "pattern": "mhb.com"},
                {"rule_type": "email_address", "pattern": "alpha@example.com"},
            ],
        )

        report = build_sync_health_report(config)

        self.assertEqual(report["status"], "healthy")
        self.assertFalse(report["drift"]["has_drift"])

    def test_health_report_is_degraded_when_domain_drift_exists(self):
        config = resolve_sync_configuration(
            default_mailboxes=["william@seattleseahawks.me"],
            default_domains=["snoco.org", "mhb.com"],
            default_specific_emails=[],
            db_mailboxes=["william@seattleseahawks.me"],
            db_rules=[
                {"rule_type": "domain", "pattern": "snoco.org"},
                {"rule_type": "domain", "pattern": "playfasa.com"},
            ],
        )

        report = build_sync_health_report(config)

        self.assertEqual(report["status"], "degraded")
        self.assertTrue(report["drift"]["has_drift"])
        self.assertTrue(report["issues"])

    def test_health_report_is_unhealthy_without_targets(self):
        config = resolve_sync_configuration(
            default_mailboxes=[],
            default_domains=[],
            default_specific_emails=[],
        )

        report = build_sync_health_report(config)

        self.assertEqual(report["status"], "unhealthy")
        self.assertIn("No email sync mailboxes are configured.", report["issues"])
        self.assertIn(
            "No email sync domains or specific email rules are configured.",
            report["issues"],
        )


if __name__ == "__main__":
    unittest.main()