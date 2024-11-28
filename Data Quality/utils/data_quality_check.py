import pandas as pd

class DataQualityChecker:
    """Class to perform data quality checks on JSON datasets."""

    def __init__(self, file_path):
        """
        Initialize the instance and load the JSON data.
        """
        self.file_path = file_path
        self.df = pd.read_json(self.file_path, lines=True)
        self.flatten_nested_fields()

    def flatten_nested_fields(self):
        """
        Flatten nested fields in the dataset to avoid 'unhashable type' errors.
        Specifically handles '_id' and other nested dictionaries.
        """
        for column in self.df.columns:
            # Flatten if column contains dictionaries
            if isinstance(self.df[column].iloc[0], dict):
                for key in self.df[column].iloc[0].keys():
                    self.df[f"{column}_{key}"] = self.df[column].apply(
                        lambda x: x[key] if isinstance(x, dict) else None
                    )
                self.df.drop(columns=[column], inplace=True)

    def get_data_info(self):
        """
        Provide information about the dataset, including the number of rows and column names.
        """
        return {
            "Column List": self.df.columns.tolist(),
            "Number of Rows": len(self.df)
        }

    def transform_data(self, id_field='_id_oid', created_date_field='createdDate_date', last_login_field='lastLogin_date'):
        """
        Apply transformations to clean and standardize the data.
        """
        # Transform `_id` field to string if not already done
        if id_field in self.df.columns:
            self.df[id_field] = self.df[id_field].astype(str)

        # Transform `createdDate` to datetime
        if created_date_field in self.df.columns:
            self.df[created_date_field] = pd.to_datetime(
                self.df[created_date_field],
                errors="coerce"
            )

        # Transform `lastLogin` to datetime
        if last_login_field in self.df.columns:
            self.df[last_login_field] = pd.to_datetime(
                self.df[last_login_field],
                errors="coerce"
            )

    def check_missing_values(self, key_field='_id_oid'):
        """
        Check for missing and non-missing values in the dataset.
        Returns a dictionary with:
        - 'summary': A DataFrame showing missing and non-missing counts for all columns.
        - 'details': A dictionary for columns with missing values, including primary key.
        """
        missing_summary = pd.DataFrame({
            "Column Name": self.df.columns,
            "Missing Count": self.df.isnull().sum(),
            "Non-Missing Count": self.df.notnull().sum()
        }).reset_index(drop=True)

        missing_details = {}
        for column in self.df.columns:
            missing_rows = self.df[self.df[column].isnull()]
            if not missing_rows.empty and key_field in self.df.columns:
                missing_details[column] = missing_rows[key_field].tolist()

        return {
            "summary": missing_summary,
            "details": missing_details
        }

    def check_invalid_dates(self, created_date_field='createdDate_date', last_login_field='lastLogin_date', key_field='_id_oid'):
        """
        Check for invalid date relationships and future dates.
        Returns:
        - 'invalid_date_order': Rows where last login < created date.
        - 'future_dates': Rows with dates in the future.
        """
        current_time = pd.Timestamp.now()

        # Check invalid date order: last login earlier than created date
        invalid_date_order = self.df[
            (self.df[last_login_field].notnull()) &
            (self.df[created_date_field].notnull()) &
            (self.df[last_login_field] < self.df[created_date_field])
        ]
        invalid_date_order_result = invalid_date_order[[key_field, created_date_field, last_login_field]] if not invalid_date_order.empty else "No Invalid Date Orders"

        # Check for future dates
        future_dates = self.df[
            (self.df[created_date_field].notnull() & (self.df[created_date_field] > current_time)) |
            (self.df[last_login_field].notnull() & (self.df[last_login_field] > current_time))
        ]
        future_dates_result = future_dates[[key_field, created_date_field, last_login_field]] if not future_dates.empty else "No Future Dates"

        return {
            "invalid_date_order": invalid_date_order_result,
            "future_dates": future_dates_result
        }

    def check_duplicates(self):
        """
        Check for duplicate rows in the dataset.
        """
        duplicate_rows = self.df[self.df.duplicated()]
        return {
            "duplicate_count": len(duplicate_rows),
            "duplicate_rows": duplicate_rows if not duplicate_rows.empty else "No duplicate rows found"
        }

    def check_primary_key_duplicates(self, key_field='_id_oid'):
        """
        Check for duplicate values in the primary key column.
        """
        if key_field in self.df.columns:
            duplicate_keys = self.df[key_field][self.df[key_field].duplicated()]
            return {
                "duplicates_exist": not duplicate_keys.empty,
                "duplicate_keys": duplicate_keys.tolist() if not duplicate_keys.empty else "No duplicate primary keys found"
            }
        return f"Field '{key_field}' not found in the dataset."

    def drop_duplicates(self):
        """
        Drop duplicate rows from the dataset.
        """
        self.df = self.df.drop_duplicates()

    def get_cleaned_data(self):
        """
        Return the cleaned DataFrame.
        """
        return self.df

    def show_data(self):
        """
        Display the dataset.
        """
        print(self.df.head())

    def display_summary(self):
        """
        Display a summary of the data quality checks.
        """
        summary = {
            "Data Info": self.get_data_info(),
            "Missing Values": self.check_missing_values(),
            "Duplicate Rows": self.check_duplicates(),
            "Primary Key Duplicates": self.check_primary_key_duplicates(),
            "Invalid Dates": self.check_invalid_dates()
        }

        print("--- Data Summary ---")
        for key, value in summary.items():
            print(f"\n--- {key} ---")
            print(value)
