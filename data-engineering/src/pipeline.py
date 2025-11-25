import pandas as pd

def load_data():
    member_info = pd.read_csv('data/memberInfo.csv')
    member_paid = pd.read_csv('data/memberPaidInfo.csv')

    print(member_info.head())
    print(member_paid.head())

    return member_info, member_paid

def validate_data(member_info, member_paid):
    member_info["expectedFullName"] = (
        member_info["firstName"].str.strip() + " " + member_info["lastName"].str.strip()
    )

    merged = member_paid.merge(
        member_info[["memberId", "expectedFullName"]],
        on="memberId",
        how="left"
    )

    valid_id_condition = merged["expectedFullName"].notna()

    paid_name_clean = merged["fullName"].fillna("").str.strip().str.lower()
    expected_name_clean = merged["expectedFullName"].fillna("").str.strip().str.lower()

    no_conflict_condition = merged["fullName"].isna() | (paid_name_clean == expected_name_clean)

    valid_condition = valid_id_condition & no_conflict_condition

    valid_rows = merged[valid_condition].copy()
    invalid_rows = merged[~valid_condition].copy()

    print(f"VALID ROWS COUNT - {len(valid_rows)}")
    print(f"INVALID ROWS COUNT - {invalid_rows.head()}")
    print(f"INVALID ROWS COUNT - {len(invalid_rows)}")

    return valid_rows, invalid_rows



if __name__ == "__main__":
    member_info, member_paid = load_data()
    valid_rows, invalid_rows = validate_data(member_info, member_paid)


