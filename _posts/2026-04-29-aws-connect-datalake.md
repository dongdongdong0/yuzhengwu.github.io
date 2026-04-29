---
layout: post
title:  "Lessons Learned: Lake Formation & Athena View Challenges with AWS Connect Data Lake"
date:   2026-04-29 10:00:00 +0800
categories: AWS Data Engineering
---

This post documents the technical challenges I encountered while building a data pipeline on top of AWS Connect Analytics Data Lake, including Lake Formation permission issues and Athena view development. Hopefully this saves someone else a few hours of debugging.

---

## 1. Lake Formation Permission Configuration

### 1.1 LF Hybrid Mode Broke Existing Access

**Problem:** After enabling Lake Formation and setting myself as an LF administrator, the client user — who previously had access to all databases and tables via IAM — could suddenly only see the `default` database in Power BI.

**Root Cause:** When Lake Formation is enabled and any permission grant is made, the account transitions into **hybrid mode**. In hybrid mode, access requires **both** IAM permissions **and** LF permissions to be satisfied simultaneously. Since I had not granted any LF permissions to the client user, LF effectively blocked all access — even though the IAM policy was unchanged.

**Solution:** Go to LF → Data lake permissions → Grant, and re-grant `DESCRIBE` on all databases and `SELECT + DESCRIBE` on all tables the client user needs to access.

---

### 1.2 Cross-Account Shared Table Authorization

**Problem:** When trying to grant the client user access to tables shared from the AWS Connect account via AWS RAM, the grant failed with:

```
Error granting catalog permissions to ARNs: arn:aws:iam::241533152081:user/aws-powerbi-role.
Unexpected error has occurred trying to grant permissions.
Resource does not exist or requester is not authorized to access requested permissions.
```

I also tried granting myself `Grantable` permissions on the shared table, but that failed too:

```
An error occurred attempting to grant opt-in permission for principal on table
"connect-data-db" with database name "shift_activities_dev": "Resource is not supported".
```

**Root Cause:** As a consumer account, we received the shared table without `Grantable` permission. This means we can query the data ourselves, but we cannot re-grant access to other IAM users within our account. AWS Connect Data Lake's data share only supports account-level sharing — there is no option to specify individual IAM user ARNs as recipients.

**Solutions Considered:**

| Option | Description | Pro | Con |
|---|---|---|---|
| LF Admin (current) | Set client user as LF admin | Simple, immediate | Overly broad access |
| Local view wrapper | Wrap shared table in Athena view | Clean | ODBC driver penetrates view and checks underlying table permissions anyway |
| CTAS copy | Copy data into own S3 bucket | Precise permission control | Requires periodic refresh, higher complexity |

**Current approach:** Set the client user as LF admin for the DEV environment. Plan to evaluate CTAS for PROD.

---

## 2. Athena View Development

### 2.1 ODBC Timestamp Incompatibility

**Problem:** Athena queries ran fine, but Power BI reported:

```
OLE DB or ODBC error: [DataSource.Error] ODBC: ERROR [HY000] [AmazonAthena][AthenaClientError]:
ErrorType: 1100, ExceptionMessage: INVALID_CAST_ARGUMENT:
Value cannot be cast to timestamp: 2026-04-02T16:09:14Z.
```

**Root Cause:** The CTR timestamp fields are stored as strings in the format `2026-03-15T21:06:33Z`. While `CAST(... AS TIMESTAMP)` works fine in Athena directly, the ODBC driver used by Power BI does not handle this ISO 8601 format during type conversion.

**Solution:** Replace all `CAST(field AS TIMESTAMP)` with `date_parse`:

```sql
-- Before (works in Athena, fails via ODBC)
CAST(ctr.initiationtimestamp AS TIMESTAMP) AS initiation_timestamp

-- After (works in both Athena and Power BI via ODBC)
date_parse(ctr.initiationtimestamp, '%Y-%m-%dT%H:%i:%sZ') AS initiation_timestamp
```

First verify the timestamp format in your data before applying:

```sql
SELECT DISTINCT
    regexp_extract(initiationtimestamp, '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}') AS format_sample
FROM your_database.your_table
WHERE initiationtimestamp IS NOT NULL
LIMIT 10;
```

If your data has milliseconds (e.g., `2026-03-15T21:06:33.000Z`), you would need a different format string. Always verify first.

---

### 2.2 VOICE Disposition Always Null

**Problem:** In Power BI, `disposition` was always null for VOICE contacts, but populated for CHAT contacts.

**Investigation:**

```sql
-- Step 1: Check disposition in the view
SELECT channel, disposition, COUNT(*)
FROM your_view
GROUP BY channel, disposition
ORDER BY channel;

-- Step 2: Check the raw table
SELECT channel, initiationmethod, attributes_clean['disposition'] AS disposition
FROM your_ctr_table
WHERE attributes_clean['disposition'] IS NOT NULL
LIMIT 20;

-- Result: All records with disposition have channel = 'CHAT' and initiationmethod = 'API'
```

**Root Cause:** This is by design in AWS Connect. When a VOICE contact ends, Connect automatically creates a Guide-type CHAT contact (`channel=CHAT, initiationmethod=API`) to let the agent fill in ACW information including disposition in the Agent Workspace interface. The two contacts are linked via `relatedcontactid`:

```
VOICE contact (contactid = aaaa)
    → has agent, no disposition
    
CHAT Guide contact (contactid = dddd, relatedcontactid = aaaa)
    → no agent, has disposition
```

**Solution:** Use a CTE + LEFT JOIN to enrich VOICE contacts with the disposition from the linked CHAT Guide contact, and filter out the Guide CHAT rows:

```sql
CREATE OR REPLACE VIEW your_contact_view AS

WITH disposition_cte AS (
    SELECT
        relatedcontactid,
        attributes_clean['disposition'] AS disposition
    FROM your_ctr_table
    WHERE channel = 'CHAT'
      AND initiationmethod = 'API'
      AND attributes_clean['disposition'] IS NOT NULL
)

SELECT
    ctr.*,
    COALESCE(
        ctr.attributes_clean['disposition'],
        disp.disposition
    ) AS disposition
FROM your_ctr_table ctr
LEFT JOIN disposition_cte disp
    ON ctr.contactid = disp.relatedcontactid
WHERE NOT (
    ctr.channel = 'CHAT'
    AND ctr.initiationmethod = 'API'
    AND ctr.attributes_clean['disposition'] IS NOT NULL
);
```

---

### 2.3 CA vs US Schema Differences

The CA and US regions use different data types for the `attributes_clean` field, which caused issues when trying to reuse the same view SQL across regions.

**US region error (when using dot notation):**
```
SYNTAX_ERROR: line 1:1: Column 'ctr.attributes_clean.disposition' cannot be resolved
```

**CA region error (when using bracket notation):**
```
SYNTAX_ERROR: line 1:1: Column 'attributes_clean['disposition']' cannot be resolved
```

**Root Cause:**

| Item | US | CA |
|---|---|---|
| `attributes_clean` type | `map<string,string>` | `struct<field:string,...>` |
| Access syntax | `attributes_clean['disposition']` | `attributes_clean.disposition` |

**Other schema differences between regions:**

| Field | US | CA |
|---|---|---|
| `language` fallback | 3 fields: `lan`, `languageselected`, `language` | 2 fields only: `lan`, `languageselected` |
| `additionalemailrecipients` | Present | Not present — causes error: `Column type is unknown: additional_email_recipients` |
| `chat AgentFirstResponseTimestamp` | Present | Not present — causes error: `Column type is unknown: chat_contact_metrics_agent_first_response_timestamp` |

**Key lesson:** For missing fields in CA, using `NULL AS field_name` does NOT work — it throws a `Column type is unknown` error. The column must be removed entirely from the CA view.

**Language field example:**

```sql
-- US version (3 fallbacks)
COALESCE(
    NULLIF(ctr.attributes_clean['lan'], ''),
    NULLIF(ctr.attributes_clean['languageselected'], ''),
    NULLIF(ctr.attributes_clean['language'], '')
) AS language

-- CA version (2 fallbacks, dot notation)
COALESCE(
    NULLIF(ctr.attributes_clean.lan, ''),
    NULLIF(ctr.attributes_clean.languageselected, '')
) AS language
```

---

### 2.4 Other Athena Gotchas

**QUALIFY syntax not supported:**

Athena does not support the `QUALIFY` clause (which is available in Snowflake and BigQuery). Use a subquery with `ROW_NUMBER()` instead:

```sql
-- Not supported in Athena
SELECT * FROM my_table
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) = 1;

-- Athena-compatible alternative
SELECT * FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY version DESC) AS rn
    FROM my_table
)
WHERE rn = 1;
```

---

## 3. Key Takeaways

- **LF hybrid mode is a trap** — once activated, all access requires explicit LF grants. Removing grants does not restore the previous IAM-only behavior automatically.
- **Cross-account shared tables are second-class citizens** — you can query them but you cannot manage their permissions as if they were your own tables.
- **Always verify timestamp formats before writing views** — `date_parse` format strings are strict and will fail silently or throw runtime errors on mismatched formats.
- **The Guide CHAT pattern is not obvious** — AWS Connect creates these automatically and they can skew metrics if not filtered out.
- **CA and US are not interchangeable** — even within the same product, schema differences between regions require separate view implementations.
- **`NULL AS column_name` does not always work in Athena views** — when the underlying table does not have that column at all, remove it from the view rather than using NULL.
