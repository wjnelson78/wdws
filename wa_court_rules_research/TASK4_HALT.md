# Task 4 HALT

**Time:** 2026-05-01T15:24:36.565875

**Reason:** Unexpected

## Details

```
new row for relation "cross_references" violates check constraint "chk_cross_ref_type"
DETAIL:  Failing row contains (da657e05-00c8-4e79-838a-296038aeff27, 8d899edc-7e8e-4a70-a5a5-150f36ccbbbd, WAC 308-94-050, null, state-wa-wac, WAC, 308-94-050, null, null, ion Number, Decals, and                $61 Validation Tabs (WAC ..., cross-rule-wac, 2026-05-01 15:24:36.549077-07, null, null).

Traceback (most recent call last):
  File "/opt/wdws/wa_court_rules_research/task4_deferred_ingest.py", line 770, in main
    xref_result = insert_xrefs(conn, citation_to_uuid, all_records)
  File "/opt/wdws/wa_court_rules_research/task4_deferred_ingest.py", line 554, in insert_xrefs
    psycopg2.extras.execute_values(cur, """
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^
        INSERT INTO wa_court_rules.cross_references
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ...<3 lines>...
        VALUES %s
        ^^^^^^^^^
    """, rows)
    ^^^^^^^^^^
  File "/usr/local/lib/python3.13/dist-packages/psycopg2/extras.py", line 1299, in execute_values
    cur.execute(b''.join(parts))
    ~~~~~~~~~~~^^^^^^^^^^^^^^^^^
psycopg2.errors.CheckViolation: new row for relation "cross_references" violates check constraint "chk_cross_ref_type"
DETAIL:  Failing row contains (da657e05-00c8-4e79-838a-296038aeff27, 8d899edc-7e8e-4a70-a5a5-150f36ccbbbd, WAC 308-94-050, null, state-wa-wac, WAC, 308-94-050, null, null, ion Number, Decals, and                $61 Validation Tabs (WAC ..., cross-rule-wac, 2026-05-01 15:24:36.549077-07, null, null).


```
