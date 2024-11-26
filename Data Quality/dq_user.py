import json
import pandas as pd

## Run Data Quality Checks on user.json

## Location of the file
file = 'users.json'

df_users = pd.read_json(file, lines=True)

# Applying Transformation on json Data to Make it a Little Cleaner
## THis will then be loaded into Data Frame
## Just makes it easier to run Data Exploration on Top of this
df_users['_id'] = df_users['_id'].apply(lambda x: x['$oid'])
# Extract the `$date` and convert it to datetime for `createdDate` and `lastLogin`
df_users['createdDate'] = pd.to_datetime(df_users['createdDate'].apply(lambda x: x['$date']), unit='ms')
#df_users['lastLogin'] = pd.to_datetime(df_users['lastLogin'].apply(lambda x: x['$date']), unit='ms')
df_users['lastLogin'] = df_users['lastLogin'].apply(lambda x: x['$date'] if isinstance(x, dict) else None)
df_users['lastLogin'] = pd.to_datetime(df_users['lastLogin'], unit='ms', errors='coerce')



## Check for missing values
missing_data = df_users.isnull().sum()
missing_values_summary_data = pd.DataFrame({
    "Fields":missing_data.index,
    "Number of Values Missing":missing_data.values
})

pd.set_option('display.max_rows', None)
## Will Give Information on fields that are missing Data . Will be a good indicator to Start the Process of DAta Quality Testing
print(missing_values_summary_data)


## 1 ---------------- Validity Check for role column. 
## --- NOtes: Based on the Data Set information - looks like Consumers is the only value accepted in this column . Hence I am adding this test
valid_role = ['consumer']
roles_invalid_data = df_users[~df_users['role'].isin(valid_role)]
## Gives out Rows that dont fit the consumer category
print(roles_invalid_data)

## 2 ---------------- Validy Check on Last login Date vs Created
#3 Example Error Could be - Last Login Date cannot be earlier than createdDate
invalid_dates = df_users[df_users['lastLogin'] < df_users['createdDate']]
print("Invalid Date Relationships:\n", invalid_dates)


## 3 ---------------- Detect Duplicate oRows in the table
dup_data_df_count = df_users.duplicated().sum()
print(f"Number of duplicates: {dup_data_df_count}")
## If there is duplcaites - I would run this to see what the duplicates look like
dup_data_df_rows = df_users[df_users.duplicated()]
print(f"Number of duplicates: {dup_data_df_rows}")
## Once Spotted and verified - I would go ahead to clean it up
df_cleaned_users = df_users.drop_duplicates()
print(df_cleaned_users)

## 4 --------------- Detect If Duplicates Existing in the PrimaryKe of the table
check_column_duplicate = df_users['_id'].duplicated().any()
if check_column_duplicate:
    print("_id Coumn has duplicates ")
    duplicate_data = df_users['_id'][df_users['_id'].duplicated()]
    ## Duplicate Data
    print(duplicate_data.unique())
else:
    print('No Dups- good to go!!!!')





