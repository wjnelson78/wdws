#!/usr/bin/env python3
"""Fix OSINT API person detail queries to include phone_type, email_type, address dates."""
import re

API_PATH = '/opt/wdws/dashboard/osint_api.py'

with open(API_PATH) as f:
    content = f.read()

fixes = 0

# Fix 1: Addresses query - include person_addresses fields
old = """    # Addresses
    addresses = await p.fetch(\"\"\"
        SELECT a.* FROM addresses a
        JOIN person_addresses pa ON pa.address_id = a.id
        WHERE pa.person_id = $1::uuid
    \"\"\", person_id)"""

new = """    # Addresses (include junction table data: is_current, address_type, dates)
    addresses = await p.fetch(\"\"\"
        SELECT a.*,
               pa.is_current AS pa_is_current,
               pa.address_type AS pa_address_type,
               pa.date_first_seen AS pa_date_first_seen,
               pa.date_last_seen AS pa_date_last_seen
        FROM addresses a
        JOIN person_addresses pa ON pa.address_id = a.id
        WHERE pa.person_id = $1::uuid
        ORDER BY pa.is_current DESC NULLS LAST, pa.date_last_seen DESC NULLS LAST
    \"\"\", person_id)"""

if old in content:
    content = content.replace(old, new)
    fixes += 1
    print("Fixed addresses query")

# Fix 2: Phones query - order by current first
old = """    # Phones
    phones = await p.fetch(\"\"\"
        SELECT ph.* FROM phone_numbers ph
        JOIN person_phones pp ON pp.phone_id = ph.id
        WHERE pp.person_id = $1::uuid
    \"\"\", person_id)"""

new = """    # Phones (include is_current from junction)
    phones = await p.fetch(\"\"\"
        SELECT ph.*, pp.is_current AS pp_is_current
        FROM phone_numbers ph
        JOIN person_phones pp ON pp.phone_id = ph.id
        WHERE pp.person_id = $1::uuid
        ORDER BY pp.is_current DESC NULLS LAST
    \"\"\", person_id)"""

if old in content:
    content = content.replace(old, new)
    fixes += 1
    print("Fixed phones query")

# Fix 3: Emails query - include is_current from junction
old = """    # Emails
    emails = await p.fetch(\"\"\"
        SELECT e.* FROM email_addresses e
        JOIN person_emails pe ON pe.email_id = e.id
        WHERE pe.person_id = $1::uuid
    \"\"\", person_id)"""

new = """    # Emails (include is_current/best_match from junction)
    emails = await p.fetch(\"\"\"
        SELECT e.*, pe.is_current AS pe_is_current
        FROM email_addresses e
        JOIN person_emails pe ON pe.email_id = e.id
        WHERE pe.person_id = $1::uuid
        ORDER BY pe.is_current DESC NULLS LAST
    \"\"\", person_id)"""

if old in content:
    content = content.replace(old, new)
    fixes += 1
    print("Fixed emails query")

with open(API_PATH, 'w') as f:
    f.write(content)

print(f"\nApplied {fixes} fixes to {API_PATH}")
