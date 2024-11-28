import pandas as pd

class ReceiptsDataQualityChecker:
    """Class to perform data quality checks on receipts data."""

    def __init__(self, file_path):
        """
        Initialize the instance and load the JSON data.
        """
        self.file_path = file_path
        self.df = pd.read_json(self.file_path, lines=True)
        self.flatten_nested_fields()

    def flatten_nested_fields(self):
        """
        Flatten nested fields in the dataset.
        Specifically handles '_id', 'createDate', and 'rewardsReceiptItemList'.
        """
        # Flatten `_id` field
        if '_id' in self.df.columns:
            self.df['_id'] = self.df['_id'].apply(lambda x: x['$oid'] if isinstance(x, dict) else x)

        # Flatten date fields
        date_fields = ['createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate']
        for field in date_fields:
            if field in self.df.columns:
                self.df[field] = self.df[field].apply(lambda x: pd.to_datetime(x['$date'], unit='ms') if isinstance(x, dict) else None)

        # Convert lists in `rewardsReceiptItemList` to string representations to avoid unhashable errors
        if 'rewardsReceiptItemList' in self.df.columns:
            self.df['rewardsReceiptItemList'] = self.df['rewardsReceiptItemList'].apply(
                lambda x: str(x) if isinstance(x, list) else x
            )

    def get_data_info(self):
        """
        Provide information about the dataset, including the number of rows and column names.
        """
        return {
            "Column List": self.df.columns.tolist(),
            "Number of Rows": len(self.df)
        }

    def check_missing_values(self, key_field='_id'):
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
            if not missing_rows.empty:
                missing_details[column] = missing_rows[key_field].tolist()

        return {
            "summary": missing_summary,
            "details": missing_details
        }

    def check_invalid_dates(self, date_fields=None, key_field='_id'):
        """
        Check for invalid dates (e.g., future dates).
        Returns:
        - 'future_dates': Rows with dates in the future.
        """
        current_time = pd.Timestamp.now()
        future_dates_result = {}

        if not date_fields:
            date_fields = ['createDate', 'dateScanned', 'finishedDate', 'modifyDate', 'pointsAwardedDate', 'purchaseDate']

        for field in date_fields:
            if field in self.df.columns:
                future_dates = self.df[self.df[field] > current_time]
                future_dates_result[field] = future_dates[[key_field, field]].to_dict(orient='records') if not future_dates.empty else "No Future Dates"

        return future_dates_result

    def check_duplicates(self):
        """
        Check for duplicate rows in the dataset.
        """
        duplicate_rows = self.df[self.df.duplicated()]
        return {
            "duplicate_count": len(duplicate_rows),
            "duplicate_rows": duplicate_rows if not duplicate_rows.empty else "No duplicate rows found"
        }

    def check_primary_key_duplicates(self, key_field='_id'):
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

    def check_rewards_receipt_item_consistency(self):
        """
        Check for consistency in `rewardsReceiptItemList` structure.
        Ensures that each entry is a valid list and the item count matches `purchasedItemCount`.
        """
        if 'rewardsReceiptItemList' in self.df.columns and 'purchasedItemCount' in self.df.columns:
            inconsistent_rows = self.df[
                self.df['rewardsReceiptItemList'].apply(lambda x: not isinstance(eval(x), list) if isinstance(x, str) else True) |
                (self.df['rewardsReceiptItemList'].apply(lambda x: len(eval(x)) if isinstance(x, str) else 0) != self.df['purchasedItemCount'])
            ]
            return {
                "inconsistent_count": len(inconsistent_rows),
                "inconsistent_rows": inconsistent_rows if not inconsistent_rows.empty else "No inconsistencies found"
            }
        return "Fields 'rewardsReceiptItemList' or 'purchasedItemCount' not found in the dataset."

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
            "Invalid Dates": self.check_invalid_dates(),
            "Rewards Receipt Item Consistency": self.check_rewards_receipt_item_consistency()
        }

        print("--- Data Summary ---")
        for key, value in summary.items():
            print(f"\n--- {key} ---")
            print(value)
