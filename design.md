# Design Document – Assignment D0 ETL Pipeline

## Introduction

This document explains the architectural decisions, validation rules, transformation logic, and reasoning behind the ETL pipeline implemented for **Assignment D0 – Simple Data Processing Pipeline**.

The goal of this assignment is to:
- Read two raw input CSV files
- Validate the integrity of the data
- Fix inconsistencies where appropriate
- Remove invalid rows
- Produce a clean flat file suitable for BI analysis
- Generate summary metrics for reporting

This document outlines **why** the pipeline is built the way it is and **how** each design choice supports the requirements.

## Problem Overview

A healthcare company provided two raw CSV files:

1. **memberInfo.csv**
    Contains the official list of active members.

2. **memberPaidInfo.csv**
Contains payment records, which may include:
    - Missing names
    - Incorrect names
    - Payment entries for non-existent members

The BI team needs data that is **accurate, consistent, and valid** according to business rules.

The task is to create an automated pipeline that:

- Cleans and validates the data
- Outputs a stable, trusted dataset
- Provides aggregated payment insights

## Business Rules & Assumptions

These rules drive the entire ETL logic.

1. **Member Validation Rule**

A payment row is valid only if the `memberId` exists in `memberInfo.csv`.

`memberInfo.csv` is assumed to be:
- Authoritative
- Complete
- Accurate
We treat it as the **source of truth**.

2. **Name Consistency Rule**

If `fullName` exists in the paid data, it must:
- Match the expected full name from `memberInfo`
- Match in a **case-insensitive, whitespace-normalized** way

3. **Missing Names Rule**

If `fullName` is missing in `memberPaidInfo.csv`, this is acceptable.

The pipeline reconstructs it as `firstName + " " + lastName` from `memberInfo.csv`.

4. **Invalid Rows**

Invalid rows include:
- Member IDs not present in the master list
- Name mismatches
- Corrupt or irrelevant payment records

These rows are excluded from the final dataset.

## ETL Pipeline Design

1. **Extract Layer**

Purpose:
Load raw CSV files into memory using pandas.

Operations:
- Read `memberInfo.csv`
- Read `memberPaidInfo.csv`

Why pandas?
- Easy CSV handling
- Powerful join/filter operations
- Ideal for light-weight ETL tasks

2. **Transform Layer**

This is the core of the assignment.

- **Build Expected Full Name**
The pipeline constructs:

`expectedFullName = firstName.strip() + " " + lastName.strip()`

This ensures:
- Consistent formatting
- No trailing/leading spaces

- **Merge Raw Payments with Member Info**
We perform a left join on `memberId`:

`merged = member_paid.merge(member_info[['memberId', 'expectedFullName']], on='memberId', how='left')`

This enables us to:
- Validate existence of memberId
- Compare provided name with expected name

- **Apply Validation Masks**

**Mask 1: memberId must exist**

`valid_id_mask = expectedFullName.notna()`

**Mask 2: Full name must match OR be missing**

`no_conflict_mask = (fullName is null) OR (normalized names match)`

A record is valid if both masks are True:

`valid_mask = valid_id_mask & no_conflict_mask`

This produces:
- A `valid_rows` DataFrame
- An `invalid_rows` DataFrame (for debugging/troubleshooting)

- **Name Reconstruction**

For valid rows with missing `fullName`:

`use expectedFullName`

This ensures all cleaned records have a consistent name.

- **Final Column Selection**

The final cleaned dataset includes:

- `memberId`
- `fullName` (validated or reconstructed)
- `paidAmount`

This matches the expected output schema required by the assignment.

3. **Load Layer**

The final cleaned dataset is written to:

`data-engineering/data/cleaned_member_payments.csv`

Design choice:
- Using `.to_csv()` keeps the output flat and portable
- Avoids requiring a database or additional services
- Aligns with the assignment’s simplicity requirement

4. **Reporting Layer**

After loading, the pipeline prints:
- **Total paid amount across all valid members**
- **Highest payment and member details**

Why?

- These metrics are often required by BI teams
- Quick sanity-checks on cleaned data
- Helps validate ETL correctness

## Design Decisions & Trade-offs

1. **Chose pandas over SQL or Spark**

Because:
- Input volume is small
- No distributed data processing required
- Simple to implement in a Python assignment

2. **Names are normalized for comparison**

Without lowercase + strip normalization:
- Leading/trailing spaces cause false errors

Normalization ensures robust validation.

3. **Invalid rows are excluded, not corrected**

This avoids making “guesses” about incorrect data.
Only missing names are auto-repaired because they are deterministic.

4. **Flat CSV output over database**

Chosen for simplicity:
- Easy to submit
- Easy for BI to load
- No external dependencies

## Potential Improvements (Production-Level)

If this pipeline were deployed at scale, we could extend it with:

1. **Logging & Error Handling**

Use Python’s `logging` module instead of `print()`.

2. **Unit Tests**

Testing validation logic using `pytest`.

3. **Schema Validation**

Using libraries like:
- `pandera`
- `pydantic`

To ensure column types and required fields.

4. **Incremental Processing**

Load only new payments instead of full file each time.

5. **Deployment Tools**

Using:
- Airflow
- Prefect
- AWS Lambda
- Docker containers

6. **Output to Data Warehouse**

Instead of CSV, write to:
- BigQuery
- Snowflake
- Redshift
- Databricks

## Conclusion

This ETL pipeline provides:

- A clean, trustworthy dataset
- Strong validation rules
- Clear transformation logic
- A simple but robust architecture
- Business-ready reporting metrics

It fulfills all requirements of **Assignment D0**, and the architecture is easily extendable for real-world production environments.


