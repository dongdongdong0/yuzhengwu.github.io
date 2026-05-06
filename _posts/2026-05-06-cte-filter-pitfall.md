---
layout: post
title: "A Subtle CTE Filtering Bug That Showed Wrong Scheduling Data in Power BI"
date: 2026-05-06 10:00:00 +0800
categories: AWS Data Engineering
---

This post documents a subtle but impactful bug I encountered while building a scheduling data view on top of AWS Connect Data Lake. The root cause was a seemingly reasonable filtering decision inside a CTE — one that turned out to produce silently incorrect results.

---

## Background

I was building an Athena view called `schedule_view` for a client using AWS Connect's scheduling data. The view pulls from three core tables:

- `staff_shifts` — one record per shift per agent, with versioning
- `staff_shift_activities` — activity segments within each shift, also versioned
- `shift_activities` — a dictionary of activity types

Both `staff_shifts` and `staff_shift_activities` use a versioning mechanism: every time a shift or activity is modified, a new record is inserted with an incremented version number. The previous versions remain in the table. There is also an `is_deleted` flag — when a shift is deleted, Connect inserts a new version with `is_deleted = true`.

My initial view used CTEs to deduplicate each table and keep only the latest version:

```sql
latest_shifts AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY instance_id, shift_id
                ORDER BY shift_version DESC
            ) AS rn
        FROM staff_shifts
        WHERE is_deleted = false   -- ← filtering BEFORE deduplication
    )
    WHERE rn = 1
)
```

This looked correct at first glance. Filter out deleted records, then take the latest version. Simple.

---

## The Problem

A client reported that a specific agent — Jordan McIntyre — had no shift scheduled on April 27, 2026 in the published Connect calendar, but the shift was still appearing in the Power BI report.

After digging into the raw data, I found this in `staff_shifts`:

| shift_version | is_deleted | last_updated_timestamp |
|---|---|---|
| 1001 | False | 2026-04-02 07:11:35 |
| 1002 | True  | 2026-04-09 16:36:43 |

The shift was created on April 2 (version 1001, `is_deleted = false`) and then **deleted on April 9** (version 1002, `is_deleted = true`). This is exactly how AWS Connect handles deletions — it doesn't remove the record, it inserts a new version with `is_deleted = true`.

---

## Why the Bug Happened

Here is the critical issue with the original CTE logic:

```sql
WHERE is_deleted = false   -- applied BEFORE ROW_NUMBER()
```

By filtering out `is_deleted = true` records **before** the window function runs, version 1002 (the deletion record) is eliminated from the dataset entirely. The `ROW_NUMBER()` then sees only version 1001, which has `is_deleted = false`, and returns it as the "latest" version.

The result: **a deleted shift appears in the view as if it still exists.**

The correct behavior should be:

1. Take the latest version first (version 1002)
2. Then check whether that latest version is deleted

The fix is to move the `is_deleted` filter **outside** the subquery:

```sql
-- WRONG: filter before deduplication
latest_shifts AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY instance_id, shift_id
                ORDER BY shift_version DESC
            ) AS rn
        FROM staff_shifts
        WHERE is_deleted = false   -- ← eliminates the deletion record before ROW_NUMBER
    )
    WHERE rn = 1
)

-- CORRECT: deduplicate first, then filter
latest_shifts AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY instance_id, shift_id
                ORDER BY shift_version DESC
            ) AS rn
        FROM staff_shifts          -- ← no filter here
    )
    WHERE rn = 1
      AND is_deleted = false       -- ← filter AFTER taking the latest version
)
```

With the corrected logic, version 1002 (`is_deleted = true`) is the latest version and gets assigned `rn = 1`. The outer `WHERE is_deleted = false` then correctly excludes it from the results.

---

## The Final Approach: Expose, Don't Filter

After discovering this bug, I made a more fundamental change to the view design. Instead of filtering `is_deleted` inside the view at all, I removed the filter entirely and exposed the `is_deleted` field as a column:

```sql
latest_shifts AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY instance_id, shift_id
                ORDER BY shift_version DESC
            ) AS rn
        FROM staff_shifts   -- no is_deleted filter
    )
    WHERE rn = 1            -- just take the latest version
)

-- In the SELECT:
ss.is_deleted AS shift_is_deleted,
ssa.is_deleted AS shift_activity_is_deleted,
sa.is_deleted AS activity_type_is_deleted
```

This way:
- The latest version of every record is always returned
- `is_deleted` reflects the true current state of that record
- The client can filter in Power BI according to their own business logic

---

## The Lesson

**As a data engineer, avoid applying too many filters inside your views.**

Every filter you add is an assumption you are making on behalf of the consumer. In this case, my assumption was: "deleted records should not appear, so I will filter them out." That assumption was correct in intent, but the implementation was subtly wrong because I did not fully understand the versioning mechanism.

The safer approach is:

- Handle technical concerns in the view (deduplication, joins, type casting)
- Expose business-relevant flags as columns (`is_deleted`, `activity_status`, `timeoff_status`)
- Let the consumer decide what to include or exclude based on their actual business needs

Data consumers are closest to the business logic. They know better than you which records matter and which do not. Your job is to give them clean, accurate, complete data — not pre-filtered data based on your own assumptions.

This bug was silent. No error was thrown. The view ran successfully and returned results. The only way it was caught was because a real user noticed a discrepancy between the source system and the report. That is the most dangerous kind of bug in data engineering.
