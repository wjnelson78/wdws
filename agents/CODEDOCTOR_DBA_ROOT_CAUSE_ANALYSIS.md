# Code Doctor Diagnosis: DBA Agent Persistent Failures

**Date**: 2026-04-06  
**Agent**: agent_dba.py  
**Status**: INVESTIGATING  
**Confidence**: 0.6 (unable to access database directly; analysis based on code review)

## Problem Statement

Agent DBA is failing 3+ times with the same error. Despite previous repairs for type consistency, the agent continues to fail.

## Code Analysis: Likely Root Causes

### Issue 1: EXISTS() Query Column Access (CRITICAL)
**Location**: Lines 556-563  
**Pattern**:
```python
table_exists = await ctx.query("""
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'ops' AND table_name = 'agent_runs'
    )
""")

if table_exists and table_exists[0].get("exists", False):
```

**Problem**: 
- The EXISTS() function may not return a column named "exists" in all PostgreSQL versions
- Some versions name it differently or return raw boolean values
- If asyncpg doesn't map the result correctly, `.get("exists")` could fail with KeyError

**Risk**: HIGH - This is in the agent error rate section (lines 564+) which runs late in the agent execution

---

### Issue 2: Decimal Type Sanitization (HIGH)
**Location**: Lines 745-755  
**Problem**:
- Sanitization happens AFTER the summary is built (line 704-716)
- The summary_items list (line 704) references metrics values that might still be Decimal types
- While line 705 does `{pct_display}%` where pct_display is from line 702 (which uses `.get()`), other values might fail

**Specific Risk**: Line 705 references `pct_display` which comes from `metrics.get('connection_pct', 'N/A')` - if connection_pct is a Decimal and the code tries to use it in f-strings later, it could cause type issues in JSON serialization at return time

---

### Issue 3: Indented SQL Whitespace (MEDIUM)
**Location**: Lines 151-158  
**Problem**:
```python
connections = await ctx.query("""
                SELECT state, COUNT(*) as count,
                       MAX(EXTRACT(EPOCH FROM now() - state_change)) as max_age_secs
                FROM pg_stat_activity
                WHERE datname = current_database()
                GROUP BY state
                ORDER BY count DESC
            """)
```

The leading whitespace in the multiline SQL string might cause parsing issues if:
- The SQL normalization layer (schema_aliases) is sensitive to whitespace
- The database driver is sensitive to leading spaces

**Impact**: While unlikely, this could cause repeated query parsing failures

---

### Issue 4: Missing Decimal Import (MEDIUM)
**Location**: Line 22  
**Current**: `from decimal import Decimal`
**Check**: Confirm this import exists (not shown in read output)

If this import is missing, all the `isinstance(v, Decimal)` checks would fail silently, causing Decimals to not be sanitized.

---

## Most Likely Culprit

**Issue 1 (EXISTS query column access)** is the highest probability cause because:
1. It's a direct dict access with `.get("exists")` - if the key name differs, it silently returns False
2. It's used to determine whether to run the agent error rate check (line 564)
3. If the column isn't named "exists", line 563 would be False, and the error rate section would be skipped
4. But if there's an exception INSIDE the if block (lines 564-576), it might be caught but indicate a structural issue with query results

---

## Recommended Fixes

### Fix 1: Robust EXISTS Query Handling
Instead of relying on column name "exists", check the first value directly:

```python
table_exists = await ctx.query("""
    SELECT EXISTS(
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'ops' AND table_name = 'agent_runs'
    ) as table_exists_check
""")

# More robust: check if table_exists is non-empty and has a truthy first value
if table_exists:
    # Get the first (and only) result's first key's value
    first_row = table_exists[0]
    # The EXISTS() result will be the first value
    table_exists_bool = next(iter(first_row.values())) if first_row else False
else:
    table_exists_bool = False

if table_exists_bool:
    # ... rest of code
```

### Fix 2: Trim SQL Whitespace
Remove leading whitespace from multiline SQL strings:

```python
connections = await ctx.query("""SELECT state, COUNT(*) as count,
       MAX(EXTRACT(EPOCH FROM now() - state_change)) as max_age_secs
FROM pg_stat_activity
WHERE datname = current_database()
GROUP BY state
ORDER BY count DESC""")
```

### Fix 3: Verify Decimal Import
Ensure line 22 has: `from decimal import Decimal`

---

## Next Steps

1. **Verify the actual error message** - Query ops.agent_runs table for the exact error message from the 3 recent failures
2. **Check PostgreSQL version** - Determine if there's a version-specific issue with EXISTS() column naming
3. **Test agent_dba in isolation** - Run a single test execution with debug logging enabled
4. **Apply fixes** - Deploy the three fixes above and re-run the agent

