# Diagnosing and Fixing Silent Data Loss in an AWS Connect CTR Pipeline

## Background

In this project, I built a data pipeline to extract Contact Trace Records (CTR) from Amazon Connect for contact center analytics. The architecture looked like this:

```
Amazon Connect → Kinesis Data Stream → Firehose → S3 → Glue Crawler → Athena → ODBC → Power BI
```

The pipeline was deployed using CloudFormation across multiple client environments (CA and US production). Clients accessed the data through an ODBC connection from Power BI to Athena, querying Glue-managed tables backed by S3. Everything seemed to be working — data was flowing, Glue tables were created, and dashboards were rendering. Until a client noticed something was wrong in their Power BI report.

---

## Problem 1: Firehose Concatenated JSON — Silent Data Loss at Scale

### Symptoms

A client compared data in the AWS Connect UI against the Athena-backed view and found a significant discrepancy. For a specific agent on a specific day, the UI showed **14 contacts handled**, but the view only returned **3**.

### Investigation

The first step was to rule out the view logic. The view was a simple `SELECT` with no `GROUP BY` or `DISTINCT` — a 1:1 mapping from the raw table. So the problem had to be in the data itself.

I queried the raw table directly:

```sql
SELECT contactid, agent.username AS agent_name
FROM "your_database"."your_ctr_table"
WHERE agent.username = 'agent@example.com'
AND date(from_iso8601_timestamp(initiationtimestamp)) = DATE '2026-04-12'
```

Still only 3 results. So I checked the partition distribution:

```sql
SELECT partition_2, partition_3, COUNT(*) AS record_count
FROM "your_database"."your_ctr_table"
WHERE partition_0 = '2026' AND partition_1 = '04' AND partition_2 = '12'
GROUP BY partition_2, partition_3
ORDER BY partition_3
```

All 22 hourly partitions for April 12 existed — the data was physically in S3. But Athena was only reading **1 record per file**.

I downloaded one of the raw `.gz` files from S3 and discovered the root cause:

```
{"ContactId":"aaa","Agent":...}{"ContactId":"bbb","Agent":...}{"ContactId":"ccc","Agent":...}
```

Multiple JSON records were **concatenated without newline delimiters**. Athena's default JSON SerDe reads one record per line — so it was silently dropping everything after the first record in each file.

### Root Cause

When deploying Firehose via CloudFormation with gzip compression enabled, the `NewLineDelimiter` parameter defaults to `DISABLED`. This means all records written within a buffer window are concatenated together with no separator.

In low-traffic environments, each buffer window may contain only one record — making the issue invisible. As traffic grows, multiple records get packed into a single file, and the problem surfaces.

### Solution

**Step 1: Fix new data**

Enable `NewLineDelimiter` in the Firehose delivery stream configuration. After this change, all new files are correctly formatted.

**Step 2: Fix historical data**

The key insight was to use Python's `json.JSONDecoder.raw_decode()`, which can parse concatenated JSON by tracking the exact end position of each object:

```python
def parse_records(raw_content):
    """
    Handles both concatenated JSON and newline-delimited JSON.
    raw_decode() finds one complete JSON object starting at idx,
    returns the object and the position where it ends.
    We then move idx forward and repeat until EOF.
    """
    records = []
    decoder = json.JSONDecoder()
    idx = 0
    raw_content = raw_content.strip()
    while idx < len(raw_content):
        # Skip whitespace including newlines
        while idx < len(raw_content) and raw_content[idx] in ' \t\r\n':
            idx += 1
        if idx >= len(raw_content):
            break
        try:
            obj, end_idx = decoder.raw_decode(raw_content, idx)
            records.append(obj)
            idx += end_idx - idx
        except json.JSONDecodeError as e:
            print(f"Parse error at position {idx}: {e}", flush=True)
            break
    return records
```

This function works regardless of whether the file has newline delimiters or not — making it **idempotent** and safe to run on any file.

The full remediation script reads from a backup S3 location, parses all records, and rewrites each file with proper newline delimiters:

```python
def process_file(key, dest_key):
    response = s3.get_object(Bucket=BACKUP_BUCKET, Key=key)
    compressed = response['Body'].read()

    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as f:
        raw_content = f.read().decode('utf-8')

    records = parse_records(raw_content)

    if not records:
        print(f"Warning: no records parsed from {key}", flush=True)
        return

    # Check if already processed (idempotent check)
    if 'attributes_clean' in records[0]:
        s3.put_object(Bucket=BACKUP_BUCKET, Key=dest_key, Body=compressed)
        return

    fixed_content = '\n'.join(json.dumps(r, ensure_ascii=False) for r in records) + '\n'

    out_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=out_buffer, mode='wb') as f:
        f.write(fixed_content.encode('utf-8'))

    s3.put_object(
        Bucket=BACKUP_BUCKET,
        Key=dest_key,
        Body=out_buffer.getvalue(),
        ContentType='application/gzip'
    )
```

### Lesson Learned

**Don't use time-based cutoffs to determine whether a file needs processing.** My first attempt used a `CUTOFF_DAY` and `CUTOFF_HOUR` variable to skip files before a certain date, with the intention of avoiding unnecessary work on already-processed files. However, this introduced a subtle bug — the logic only compared the day of month, completely ignoring the month itself. As a result, files from March 17th–31st were incorrectly treated as "after the cutoff" (day `17` > day `16`) and skipped entirely, leaving them unprocessed.

The better approach is to **let the data speak for itself**: check whether the file already has the expected structure (e.g., `attributes_clean` field present), and skip it if so. This makes the script truly idempotent — safe to run multiple times with consistent results, without any risk of month-boundary bugs.

---

## Problem 2: AWS Glue Schema Inference Failure (131072 Character Limit)

### Symptoms

After remediating the historical data, I tried running a Glue Crawler to rebuild the table schema. It failed with:

```
ValidationException: Value at 'table.storageDescriptor.columns.44.member.type'
failed to satisfy constraint: Member must have length less than or equal to 131072
```

### Root Cause

AWS Glue Crawlers infer schema by scanning all files and **merging** every unique field they encounter across the entire dataset into a single `struct` type definition. In this pipeline, the `Attributes` field in CTR data varies significantly across different contact flows — some flows add dozens of unique keys that others don't.

When Glue merges all observed keys into one struct definition, the resulting type string can exceed the 131,072 character limit for a single column's type definition.

The issue is particularly tricky because:
- The limit applies to a **single column's type string**, not the sum of all columns
- The Crawler doesn't report which column is the problem until the write fails
- Re-running on a smaller date range shifts which column index is reported

### Solution

The Crawler was initially run on a small dataset (when the pipeline first launched and data volume was low), and it successfully generated a working table schema. Rather than rebuilding from scratch with a full DDL statement, I updated the existing Crawler-generated table directly in the Glue Console:

- Changed the `Attributes` column type from the auto-inferred `struct` to `string` — preserving the raw value without Glue trying to infer its structure
- Changed the `attributes_clean` column type from `struct` to `map<string,string>` — flexible, no predefined fields needed, and no character limit concern

This approach avoided rewriting the entire table definition while still resolving the schema inference failure.

```json
{
  "SchemaChangePolicy": {
    "UpdateBehavior": "LOG",
    "DeleteBehavior": "LOG"
  }
}
```

Querying `map` fields in Athena uses bracket notation:

```sql
SELECT
    contactid,
    attributes_clean['reason_of_contact'] AS reason_of_contact,
    attributes_clean['parkname']          AS park_name,
    attributes_clean['disposition']       AS disposition
FROM your_ctr_table
WHERE attributes_clean['reason_of_contact'] IS NOT NULL
```

---

## Problem 3: Disposition Field Missing in VOICE Contacts

### Symptoms

While reviewing a Power BI report, I noticed that the `disposition` field was always empty for VOICE channel contacts. I cross-checked against the AWS Connect UI directly and confirmed that agents had indeed filled in disposition values after calls — but those values were not appearing in the data. This discrepancy prompted a deeper investigation into how Connect records ACW data.

### Investigation

Querying the raw table confirmed it:

```sql
SELECT contactid, agent.username, attributes_clean['disposition'] AS disposition, channel
FROM your_ctr_table
WHERE channel = 'VOICE'
AND agent.username IS NOT NULL
AND attributes_clean['disposition'] IS NOT NULL
LIMIT 10
-- Returns 0 rows
```

But `disposition` values did exist — only on records where `channel = 'CHAT'` and `initiationmethod = 'API'`.

### Root Cause

This is by design in Amazon Connect. When a VOICE call ends, Connect automatically creates a **Guide-type CHAT contact** to capture After Contact Work (ACW), including disposition. This CHAT contact:

- Has `channel = 'CHAT'` and `initiationmethod = 'API'`
- Has no `agent.username` (it's system-generated)
- Contains the disposition filled in by the agent
- Is linked to the original VOICE contact via `relatedcontactid`

### Solution

Use a CTE in the view to join VOICE contacts with their corresponding CHAT Guide contacts:

```sql
CREATE OR REPLACE VIEW your_ctr_view AS
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
    ctr.contactid,
    ctr.channel,
    ctr.agent.username                                    AS agent_name,
    try(cast(from_iso8601_timestamp(ctr.initiationtimestamp) AS timestamp)) AS initiation_ts,
    ctr.attributes_clean['reason_of_contact']             AS reason_of_contact,
    ctr.attributes_clean['parkname']                      AS park_name,
    coalesce(d.disposition, ctr.attributes_clean['disposition']) AS disposition,
    -- ... other fields
FROM your_ctr_table ctr
LEFT JOIN disposition_cte d ON ctr.contactid = d.relatedcontactid
WHERE ctr.channel = 'VOICE'
OR (ctr.channel = 'CHAT' AND ctr.initiationmethod != 'API')
```

---

## Data Remediation Workflow

For anyone facing a similar situation, here is the safe remediation approach used throughout this project:

```
1. Create backup bucket
2. Copy raw data to backup/rawdata/
3. Process: parse → transform → write to backup/processeddata/
4. Validate processed data (spot checks + record count verification)
5. Swap: copy processeddata back to source bucket
   - Source bucket has versioning enabled as rollback safety net
   - Firehose continues writing new data uninterrupted during swap
```

### Copy Script

```python
import boto3
import sys

def copy_bucket(source_bucket, dest_bucket, dest_prefix):
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket)

    total = 0
    for page in pages:
        for obj in page.get('Contents', []):
            src_key = obj['Key']
            dest_key = dest_prefix + src_key

            s3.copy_object(
                CopySource={'Bucket': source_bucket, 'Key': src_key},
                Bucket=dest_bucket,
                Key=dest_key
            )
            total += 1
            if total % 500 == 0:
                print(f"Copied {total} files...", flush=True)

    print(f"\nDone! Total {total} files copied.", flush=True)

if __name__ == '__main__':
    source_bucket = sys.argv[1]
    dest_bucket   = sys.argv[2]
    dest_prefix   = sys.argv[3]
    copy_bucket(source_bucket, dest_bucket, dest_prefix)
```

Run as:
```bash
nohup python3 copy_bucket.py your-source-bucket your-backup-bucket ctr/rawdata/ > copy_log.txt 2>&1 &
```

### Swap Script

```python
import boto3

s3 = boto3.client('s3')

BACKUP_BUCKET    = 'your-backup-bucket'
SOURCE_BUCKET    = 'your-source-bucket'
PROCESSED_PREFIX = 'ctr/processeddata/2026/'

def main():
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=BACKUP_BUCKET, Prefix=PROCESSED_PREFIX)

    total = 0
    for page in pages:
        for obj in page.get('Contents', []):
            src_key = obj['Key']
            if not src_key.endswith('.gz'):
                continue

            dest_key = src_key.replace('ctr/processeddata/', '')

            s3.copy_object(
                CopySource={'Bucket': BACKUP_BUCKET, 'Key': src_key},
                Bucket=SOURCE_BUCKET,
                Key=dest_key
            )
            total += 1
            if total % 500 == 0:
                print(f"Copied {total} files...", flush=True)

    print(f"\nDone! Total {total} files copied.", flush=True)

if __name__ == '__main__':
    main()
```

**Key principles:**
- Raw data is never modified in place
- Processing is idempotent — check data state, not timestamps
- Validate before every swap
- S3 versioning provides a rollback mechanism at no extra operational cost

---

## Key Takeaways

1. **Always enable `NewLineDelimiter` in Firehose when using compression.** It is `DISABLED` by default in CloudFormation and easy to miss.

2. **Athena silently drops malformed records.** There are no errors — queries just return fewer rows than expected. Always cross-validate against the source system.

3. **Glue Crawler schema inference does not scale well with highly variable JSON.** For production pipelines with dynamic attributes, prefer manual DDL with `map<string,string>` or `string` types over auto-inferred structs.

4. **Idempotent data processing is more reliable than time-based cutoffs.** Check the data's own state to decide whether processing is needed.

5. **Understand your source system's data model.** The missing `disposition` on VOICE contacts looked like a bug but was intentional Connect behavior. Always investigate before assuming it's a pipeline issue.
