import os
import sys
import pandas as pd
import hashlib
from datetime import datetime

from src.exception import MyException
from src.logger import logging
from src.constants import LOGIN_CREDENTIALS_COLLECTION_NAME
from src.data_access.proj1_data import Proj1Data


class CredentialUploader:
    """
    A class to upload login credentials to MongoDB with hashed passwords.
    """

    def __init__(self):
        """
        Initializes the Proj1Data instance for MongoDB operations.
        """
        try:
            self.data_access = Proj1Data()
            logging.info("CredentialUploader initialized successfully.")

        except Exception as e:
            raise MyException(e, sys)

    @staticmethod
    def hash_password(password: str) -> str:
        """
        Hashes a password using SHA-256.

        Parameters:
        ----------
        password : str
            Plain text password to hash.

        Returns:
        -------
        str
            Hashed password in hexadecimal format.
        """
        return hashlib.sha256(password.encode()).hexdigest()

    def create_credentials_dataframe(self, credentials: list) -> pd.DataFrame:
        """
        Creates a DataFrame from a list of credential dictionaries.

        Parameters:
        ----------
        credentials : list
            List of dictionaries containing 'user_id' and 'password' keys.

        Returns:
        -------
        pd.DataFrame
            DataFrame with user_id, hashed_password, and created_at columns.
        """
        try:
            if not credentials:
                raise ValueError("Credentials list is empty.")

            # Process credentials and hash passwords
            processed_credentials = []

            for cred in credentials:
                if 'user_id' not in cred or 'password' not in cred:
                    raise ValueError("Each credential must have 'user_id' and 'password' keys.")

                processed_credentials.append({
                    'user_id': cred['user_id'],
                    'hashed_password': self.hash_password(cred['password']),
                    'created_at': datetime.now()
                })

            df = pd.DataFrame(processed_credentials)
            logging.info(f"Created credentials DataFrame with {len(df)} records.")
            return df

        except Exception as e:
            raise MyException(e, sys)

    def upload_credentials(self, credentials: list) -> None:
        """
        Uploads login credentials to MongoDB.

        Parameters:
        ----------
        credentials : list
            List of dictionaries with 'user_id' and 'password' keys.
            Example: [
                {'user_id': 'admin', 'password': 'admin123'},
                {'user_id': 'trainer', 'password': 'train456'}
            ]
        """
        try:
            logging.info("Starting credential upload process...")

            # Create DataFrame from credentials
            df = self.create_credentials_dataframe(credentials)

            # Insert into MongoDB
            self.data_access.insert_dataframe(
                df = df,
                collection_name = LOGIN_CREDENTIALS_COLLECTION_NAME
            )

            logging.info(f"Successfully uploaded {len(df)} credentials to MongoDB.")
            print(f"✅ Successfully uploaded {len(df)} credentials to MongoDB collection '{LOGIN_CREDENTIALS_COLLECTION_NAME}'")

        except Exception as e:
            raise MyException(e, sys)

    def fetch_all_credentials(self) -> pd.DataFrame:
        """
        Fetches all credentials from MongoDB.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing all stored credentials.
        """
        try:
            logging.info("Fetching all credentials from MongoDB...")
            df = self.data_access.export_collection_as_dataframe(
                collection_name = LOGIN_CREDENTIALS_COLLECTION_NAME
            )
            logging.info(f"Fetched {len(df)} credentials from MongoDB.")
            return df

        except Exception as e:
            raise MyException(e, sys)