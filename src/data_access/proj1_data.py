import sys
import pandas as pd
import numpy as np
from typing import Optional

from src.logger import logging
from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME
from src.exception import MyException

class Proj1Data:
    """
    A class to export MongoDB records as a pandas DataFrame.
    """

    def __init__(self) -> None:
        """
        Initializes the MongoDB client connection.
        """
        try:
            self.mongo_client = MongoDBClient(database_name = DATABASE_NAME)
        except Exception as e:
            raise MyException(e, sys)

    def export_collection_as_dataframe(self, collection_name: str, database_name: Optional[str] = None) -> pd.DataFrame:
        """
        Exports an entire MongoDB collection as a pandas DataFrame.

        Parameters:
        ----------
        collection_name : str
            The name of the MongoDB collection to export.
        database_name : Optional[str]
            Name of the database (optional). Defaults to DATABASE_NAME.

        Returns:
        -------
        pd.DataFrame
            DataFrame containing the collection data, with '_id' column removed and 'na' values replaced with NaN.
        """
        try:
            # Access specified collection from the default or specified database
            if database_name is None:
                collection = self.mongo_client.database[collection_name]
            else:
                collection = self.mongo_client.client[database_name][collection_name]

            # Convert collection data to DataFrame and preprocess
            print("Fetching data from mongoDB")
            df = pd.DataFrame(list(collection.find()))
            print(f"Data fecthed with len: {len(df)}")
            if "id" in df.columns.to_list():
                df = df.drop(columns=["id"], axis=1)
            df.replace({"na":np.nan},inplace=True)
            return df

        except Exception as e:
            raise MyException(e, sys)
        
    def insert_dataframe(self, df: pd.DataFrame, collection_name: str) -> None:
        """
        Inserts a pandas DataFrame into MongoDB collection.

        Parameters:
        ----------
        df : pd.DataFrame
            DataFrame to be inserted into MongoDB.
        collection_name : str
            Target MongoDB collection name.
        """

        try:
            if df.empty:
                raise ValueError("DataFrame is empty. Nothing to insert.")

            # Replace NaN with None (MongoDB does not support NaN)
            df = df.where(pd.notnull(df), None)

            # Convert DataFrame to list of dictionaries
            records = df.to_dict(orient="records")

            # Get collection
            collection = self.mongo_client.database[collection_name]

            # Insert data
            result = collection.insert_many(records)

            logging.info(
                f"Inserted {len(result.inserted_ids)} records into MongoDB collection '{collection_name}'."
            )

        except Exception as e:
            raise MyException(e, sys)
