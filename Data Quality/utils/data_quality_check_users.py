import json
import pandas as pd

class DataQuality_Users:
    """
    This class is designed to make data quality checks on user data easy and straightforward.

    Here’s what it does:

    - **__init__(file_path)**: Loads your JSON data and gets the class ready to work.
    - **transform_data(id_field, created_date_field, last_login_field)**: Cleans up key fields like user IDs and dates, making them consistent and easier to use.
    - **get_data_info()**: Gives you a quick summary of your dataset, including the number of rows and column names.
    - **check_missing_values(key_field)**: Finds any missing values in the dataset and gives details on where they occur.
    - **check_invalid_dates(created_date_field, last_login_field,e.g. key_field)**: Looks at date relationships to make sure they make sense (like last login happening after the account creation date) and checks for dates that are set in the future.
    - **check_duplicates()**: Identifies duplicate rows in the data.
    - **check_primary_key_duplicates(key_field)**: Checks for duplicate user IDs or any other primary key you specify.
    - **drop_duplicates()**: Cleans up duplicate rows from the dataset.
    - **get_cleaned_data()**: Returns the cleaned dataset after running all the set checks.
    - **show_data()**: Shows you a sample of the data so you can get a feel for its structure.
    - **display_summary()**: Wraps everything up in a clear, easy-to-read summary of all the data quality checks you’ve run.

    This class is built to save you time and give you confidence in the quality of your user data.
    """


    
    def __init__(self, file_path):
        # 1. Load the Data
        self.file_path = file_path
        self.df = pd.read_json(self.file_path, lines=True)
        
        # 2. Transform the Data
        self.transform_data()

        # 3. Get Shape of the Data
        self.data_info = self.get_data_info()

        # 4. Get Duplicates
        self.duplicate_count, self.duplicate_rows = self.check_duplicates()

        # 5. Get Duplicates on Primary Key
        self.primary_keys, self.duplicate_keys = self.check_primary_key_duplicates()

        # 6. Check Missing Values
        self.missing_values = self.check_missing_values()

        # 7. Check Invalid Dates
        self.invalid_dates = self.check_invalid_dates()

        # 8. Check Valid Roles
        self.invalid_roles = self.check_valid_roles()

        # Display all results
        self.display_summaries()

    # Step 2: Transform the Data
    def transform_data(self, id_field='_id', created_date_field='createdDate', last_login_field='lastLogin'):
        """Apply transformations to clean and standardize the data."""
        self.df[id_field] = self.df[id_field].apply(lambda x: str(x['$oid']) if isinstance(x, dict) else x)
        self.df[created_date_field] = pd.to_datetime(
            self.df[created_date_field].apply(lambda x: x['$date'] if isinstance(x, dict) else x), 
            unit='ms', errors='coerce'
        )
        self.df[last_login_field] = self.df[last_login_field].apply(
            lambda x: x['$date'] if isinstance(x, dict) else None
        )
        self.df[last_login_field] = pd.to_datetime(self.df[last_login_field], unit='ms', errors='coerce')

    # Step 3: Get Shape of the Data
    def get_data_info(self):
        """Provide information on the number of rows and the names of columns."""
        return {
            "Column List": self.df.columns.tolist(),
            "Number of Rows": len(self.df)
        }

    # Step 4: Get Duplicates
    def check_duplicates(self):
        """Check for duplicate rows in the dataset."""
        self.df = self.df.applymap(lambda x: str(x) if isinstance(x, dict) else x)
        duplicate_count = self.df.duplicated().sum()
        duplicate_rows = self.df[self.df.duplicated()]
        return duplicate_count, duplicate_rows

    # Step 5: Get Duplicates on Primary Key
    def check_primary_key_duplicates(self, key_field='_id'):
        """Check for duplicate entries in the specified primary key column."""
        self.df[key_field] = self.df[key_field].apply(lambda x: str(x) if isinstance(x, dict) else x)
        primary_key_list = self.df[key_field].tolist()
        duplicate_keys = self.df[key_field][self.df[key_field].duplicated()].unique().tolist()
        return primary_key_list, duplicate_keys

    # Step 6: Check Missing Values
    def check_missing_values(self, key_field='_id'):
        """
        Check for missing and non-missing values in the dataset.
        Returns a dictionary with:
        - 'summary': A DataFrame showing missing and non-missing counts for all columns.
        - 'details': A dictionary for columns with missing values, where keys are column names,
                    and values are lists of dictionaries containing the primary key.
        """
        # Generate a summary for all columns
        missing_summary = pd.DataFrame({
            "Column Name": self.df.columns,
            "Missing Count": self.df.isnull().sum(),
            "Non-Missing Count": self.df.notnull().sum()
        }).reset_index(drop=True)

        # Generate detailed breakdown for columns with missing values
        missing_details = {}
        for column in self.df.columns:
            missing_rows = self.df[self.df[column].isnull()]
            if not missing_rows.empty:
                missing_details[column] = missing_rows.apply(
                    lambda row: {key_field: row[key_field]}, axis=1
                ).tolist()

        return {
            "summary": missing_summary,
            "details": missing_details
        }


    # Step 7: Check Invalid Dates
    def check_invalid_dates(self, created_date_field='createdDate', last_login_field='lastLogin', key_field='_id'):
        """
        Check for invalid date relationships where:
        - Last login is earlier than created date.
        - Any date is in the future.
        Returns:
        - A dictionary with:
            - 'invalid_date_order': Rows where last login < created date, including primary key.
            - 'future_dates': Rows with dates in the future, including primary key.
        - A message if no issues are found.
        """
        # Check invalid date order: last login earlier than created date
        invalid_date_order = self.df[
            (self.df[last_login_field].notnull()) &
            (self.df[created_date_field].notnull()) &
            (self.df[last_login_field] < self.df[created_date_field])
        ]
        invalid_date_order_result = invalid_date_order[[key_field, created_date_field, last_login_field]] if not invalid_date_order.empty else "No Invalid Date Orders"

        # Check for future dates
        current_time = pd.Timestamp.now()
        future_dates = self.df[
            (self.df[created_date_field].notnull() & (self.df[created_date_field] > current_time)) |
            (self.df[last_login_field].notnull() & (self.df[last_login_field] > current_time))
        ]
        future_dates_result = future_dates[[key_field, created_date_field, last_login_field]] if not future_dates.empty else "No Future Dates"

        return {
            "invalid_date_order": invalid_date_order_result,
            "future_dates": future_dates_result
        }


    # Step 8: Check Valid Roles
    def check_valid_roles(self, role_field='role', valid_roles=['consumer'], key_field='_id'):
        """Check for invalid roles in the specified column."""
        self.df[role_field] = self.df[role_field].astype(str)
        invalid_roles = self.df[~self.df[role_field].isin(valid_roles)]
        return invalid_roles[[key_field, role_field]]

    def display_summaries(self):
        """Display summaries collected during initialization."""
        print("--- Data Info ---")
        print(self.data_info)
        print("\n--- Duplicate Rows ---")
        print(f"Count: {self.duplicate_count}")
        print(self.duplicate_rows)
        print("\n--- Primary Key Info ---")

        print(f"Distinct Duplicate Primary Keys: {self.duplicate_keys}")
        print("\n--- Missing Values Summary ---")
        print(self.missing_values["summary"])
        print("\n--- Missing Values Details (for columns with missing data) ---")
        print(self.missing_values["details"])
        print("\n--- Invalid Dates ---")
        print("Invalid Date Orders:")
        print(self.invalid_dates["invalid_date_order"])
        print("Future Dates:")
        print(self.invalid_dates["future_dates"])
        print("\n--- Invalid Roles ---")
        print(self.invalid_roles)


