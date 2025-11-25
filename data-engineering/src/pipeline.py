import pandas as pd

def load_data():
    member_info = pd.read_csv('data/memberInfo.csv')
    member_paid = pd.read_csv('data/memberPaidInfo.csv')

    print(member_info.head())
    print(member_paid.head())

    return member_info, member_paid

if __name__ == "__main__":
    load_data()

    
