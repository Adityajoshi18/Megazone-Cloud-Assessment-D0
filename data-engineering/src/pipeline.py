import pandas as pd

def load_data():
    """
    Load the two input CSV files into pandas DataFrames.
    """
    member_info = pd.read_csv('data/memberInfo.csv')
    member_paid = pd.read_csv('data/memberPaidInfo.csv')
    return member_info, member_paid

def validate_data(member_info, member_paid):
    """
    Step 1: Validate data - 
    1. Only keep rows where memberId exists in memberInfo.
    2. If fullName exists, it must match the constructed full name.
    """

    # Build the expected full name from memberInfo
    member_info["expectedFullName"] = (
        member_info["firstName"].str.strip() + " " + member_info["lastName"].str.strip()
    )

    # Merge paid file with expected full names
    merged = member_paid.merge(
        member_info[["memberId", "expectedFullName"]],
        on="memberId",
        how="left"
    )

    # Rule 1: memberId must exist
    valid_id_mask = merged["expectedFullName"].notna()

    # Normalize names
    paid_name_norm = merged["fullName"].fillna("").str.strip().str.lower()
    expected_name_norm = merged["expectedFullName"].fillna("").str.strip().str.lower()

    # Rule 2: fullName must match OR be missing
    no_conflict_mask = merged["fullName"].isna() | (paid_name_norm == expected_name_norm)

    # Final valid rows
    valid_rows = merged[valid_id_mask & no_conflict_mask].copy()
    invalid_rows = merged[~(valid_id_mask & no_conflict_mask)].copy()

    return valid_rows

def transform(valid_rows):
    """
    Step 2: Build the cleaned final dataset.
    - If fullName missing in paid file, use expectedFullName.
    """

    valid_rows["finalFullName"] = valid_rows.apply(
        lambda row: row["expectedFullName"] if pd.isna(row["fullName"]) else row["fullName"],
        axis=1
    )

    cleaned = valid_rows[["memberId", "finalFullName", "paidAmount"]]
    cleaned = cleaned.rename(columns={"finalFullName": "fullName"})

    return cleaned

def save_cleaned(cleaned_df):
    """
    Step 3: Save the cleaned dataset to a new CSV.
    """
    cleaned_df.to_csv('data/cleaned_member_payments.csv', index=False)
    print("\nSaved cleaned file to: data/cleaned_member_payments.csv")

def report(cleaned_df):
    """
    Step 4: Generate required reports.
    - Total paid amount
    - Highest paid member
    """
    highest_row = cleaned_df.loc[cleaned_df['paidAmount'].idxmax()]
    total_paid = cleaned_df['paidAmount'].sum()

    print("\n=== REPORT ===")
    print(f"Total Paid Amount: {total_paid}")
    print("\nHighest Paid Member:")
    print(highest_row)

if __name__ == "__main__":
    member_info, member_paid = load_data()
    valid_rows = validate_data(member_info, member_paid)
    cleaned = transform(valid_rows)
    save_cleaned(cleaned)
    report(cleaned)
