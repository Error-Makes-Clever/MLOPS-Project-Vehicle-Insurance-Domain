import boto3
from src.configuration.aws_connection import S3Client
from io import StringIO
from typing import Union,List
import os,sys
from src.logger import logging
from mypy_boto3_s3.service_resource import Bucket, Object
from src.exception import MyException
from botocore.exceptions import ClientError
from pandas import DataFrame, read_csv
import pickle


class SimpleStorageService:

    """
    A class for interacting with AWS S3 storage, providing methods for file management, 
    data uploads, and data retrieval in S3 buckets.
    """

    def __init__(self):

        """
        Initializes the SimpleStorageService instance with S3 resource and client
        from the S3Client class.
        """

        s3_client = S3Client()
        self.s3_resource = s3_client.s3_resource
        self.s3_client = s3_client.s3_client

    def s3_key_path_available(self, bucket_name, s3_key) -> bool:

        """
        Checks if a specified S3 key path (file path) is available in the specified bucket.

        Args:
            bucket_name (str): Name of the S3 bucket.
            s3_key (str): Key path of the file to check.

        Returns:
            bool: True if the file exists, False otherwise.
        """

        try:
            bucket = self.get_bucket(bucket_name)
            s3_objects = [s3_object for s3_object in bucket.objects.filter(Prefix = s3_key)]
            return len(s3_objects) > 0
        
        except Exception as e:
            raise MyException(e, sys)
        


    @staticmethod
    def read_object(s3_object : Object, decode : bool = True, make_readable : bool = False) -> Union[StringIO, bytes]:

        """
        Reads the specified S3 object with optional decoding and formatting.

        Args:
            object_name (str): The S3 object name.
            decode (bool): Whether to decode the object content as a string.
            make_readable (bool): Whether to convert content to StringIO for DataFrame usage.

        Returns:
            Union[StringIO, str]: The content of the object, as a StringIO or decoded string.
        """

        logging.info("Entered the read_object method of SimpleStorageService class")

        try:
            # Read and decode the object content if decode=True
            func = (
                lambda : s3_object.get()["Body"].read().decode()
                if decode else s3_object.get()["Body"].read()
            )

            # Convert to StringIO if make_readable=True
            conv_func = lambda: StringIO(func()) if make_readable else func()
            # logging.info("Exited the read_object method of SimpleStorageService class")
            return conv_func()
        
        except Exception as e:
            raise MyException(e, sys) from e
        

    def read_objects_safe(self, s3_object: Union[Object, List[Object]], decode: bool = True, make_readable: bool = False) -> Union[StringIO, List[StringIO]]:
        """
        Safely reads one or multiple S3 objects and returns their contents.

        This method acts as a wrapper around `read_object()` and automatically
        handles both single S3 objects and lists of S3 objects.

        Parameters:
            s3_object (Union[Object, List[Object]]):
                A single S3 Object or a list of S3 Objects returned from boto3.

            decode (bool):
                If True, decodes the object content from bytes to string.

            make_readable (bool):
                If True, converts the content into a StringIO object
                (useful for pandas read operations).

        Returns:
            Union[StringIO, List[StringIO]]:
                - A single StringIO / string if one object is provided
                - A list of StringIO / string if multiple objects are provided
        """

        logging.info("Entered read_objects_safe method")

        try:
            # Case 1: Multiple S3 objects
            if isinstance(s3_object, list):
                logging.info(f"Reading {len(s3_object)} S3 objects")
                return [
                    self.read_object(obj, decode=decode, make_readable=make_readable)
                    for obj in s3_object
                ]

            # Case 2: Single S3 object
            logging.info("Reading single S3 object")
            return self.read_object(s3_object, decode=decode, make_readable=make_readable)

        except Exception as e:
            logging.error("Error occurred while reading S3 object(s)", exc_info=True)
            raise MyException(e, sys) from e


    def get_bucket(self, bucket_name: str) -> Bucket:

        """
        Retrieves the S3 bucket object based on the provided bucket name.

        Args:
            bucket_name (str): The name of the S3 bucket.

        Returns:
            Bucket: S3 bucket object.
        """

        logging.info("Entered the get_bucket method of SimpleStorageService class")

        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            logging.info("Exited the get_bucket method of SimpleStorageService class")
            return bucket
        
        except Exception as e:
            raise MyException(e, sys) from e


    def get_objects_by_prefix(self, filename: str, bucket_name: str) -> Union[List[object], object]:

        """
        Retrieves the file object(s) from the specified bucket based on the filename.

        Args:
            filename (str): The name of the file to retrieve.
            bucket_name (str): The name of the S3 bucket.

        Returns:
            Union[List[object], object]: The S3 file object or list of file objects.
        """

        logging.info("Entered the get_objects_by_prefix method of SimpleStorageService class")

        try:
            bucket = self.get_bucket(bucket_name)
            s3_objects = [s3_object for s3_object in bucket.objects.filter(Prefix=filename)]

            func = lambda x: x[0] if len(x) == 1 else x
            file_objs = func(s3_objects)
            logging.info("Exited the get_objects_by_prefix method of SimpleStorageService class")
            return file_objs
        
        except Exception as e:
            raise MyException(e, sys) from e


    def load_model(self, model_name: str, bucket_name: str, model_dir: str = None) -> object:
        """
        Loads a serialized ML model from S3.

        Ensures exactly one model file exists.
        """

        try:
            model_key = f"{model_dir}/{model_name}" if model_dir else model_name

            s3_object = self.get_objects_by_prefix(model_key, bucket_name)

            # Ensure exactly one model exists
            if isinstance(s3_object, list):
                if len(s3_object) == 0:
                    raise FileNotFoundError("No model found in S3.")
                elif len(s3_object) > 1:
                    raise ValueError(
                        f"Multiple models found for prefix '{model_key}'. "
                        "Please ensure only one model exists."
                    )
                s3_object = s3_object[0]

            # Read binary model file
            model_bytes = self.read_object(s3_object, decode=False)

            # Deserialize model
            model = pickle.loads(model_bytes)

            logging.info("Model successfully loaded from S3.")
            return model

        except Exception as e:
            raise MyException(e, sys) from e



    def create_folder(self, folder_name: str, bucket_name: str) -> None:

        """
        Creates a folder in the specified S3 bucket.

        Args:
            folder_name (str): Name of the folder to create.
            bucket_name (str): Name of the S3 bucket.
        """

        logging.info("Entered the create_folder method of SimpleStorageService class")

        try:
            # Check if folder exists by attempting to load it
            self.s3_resource.Object(bucket_name, folder_name).load()

        except ClientError as e:
            # If folder does not exist, create it
            if e.response["Error"]["Code"] == "404":
                folder_obj = folder_name + "/"
                self.s3_client.put_object(Bucket = bucket_name, Key = folder_obj)

            logging.info("Exited the create_folder method of SimpleStorageService class")


    def upload_file(self, from_filename: str, to_filename: str, bucket_name: str, remove: bool = True):

        """
        Uploads a local file to the specified S3 bucket with an optional file deletion.

        Args:
            from_filename (str): Path of the local file.
            to_filename (str): Target file path in the bucket.
            bucket_name (str): Name of the S3 bucket.
            remove (bool): If True, deletes the local file after upload.
        """

        logging.info("Entered the upload_file method of SimpleStorageService class")

        try:
            logging.info(f"Uploading {from_filename} to {to_filename} in {bucket_name}")
            self.s3_resource.meta.client.upload_file(from_filename, bucket_name, to_filename)
            logging.info(f"Uploaded {from_filename} to {to_filename} in {bucket_name}")

            # Delete the local file if remove is True
            if remove:
                os.remove(from_filename)
                logging.info(f"Removed local file {from_filename} after upload")

            logging.info("Exited the upload_file method of SimpleStorageService class")

        except Exception as e:
            raise MyException(e, sys) from e


    def upload_df_as_csv(self, data_frame: DataFrame, local_filename: str, bucket_filename: str, bucket_name: str) -> None:

        """
        Uploads a DataFrame as a CSV file to the specified S3 bucket.

        Args:
            data_frame (DataFrame): DataFrame to be uploaded.
            local_filename (str): Temporary local filename for the DataFrame.
            bucket_filename (str): Target filename in the bucket.
            bucket_name (str): Name of the S3 bucket.
        """

        logging.info("Entered the upload_df_as_csv method of SimpleStorageService class")

        try:
            # Save DataFrame to CSV locally and then upload it
            data_frame.to_csv(local_filename, index=None, header=True)
            self.upload_file(local_filename, bucket_filename, bucket_name)
            logging.info("Exited the upload_df_as_csv method of SimpleStorageService class")

        except Exception as e:
            raise MyException(e, sys) from e


    def get_df_from_object(self, object_: Union[Object, List[Object]]) -> Union[DataFrame, List[DataFrame]]:
        """
        Converts one or more S3 objects into Pandas DataFrames.

        Args:
            object_ (Object | List[Object]): S3 object(s)

        Returns:
            DataFrame | List[DataFrame]: Parsed DataFrame(s)
        """

        logging.info("Entered the get_df_from_object method of SimpleStorageService class")

        try:
            # Case 1: Multiple S3 objects
            if isinstance(object_, list):
                dataframes = []
                for obj in object_:
                    content = self.read_objects_safe(obj, make_readable=True)
                    df = read_csv(content)
                    dataframes.append(df)

                logging.info("Converted multiple S3 objects to DataFrames")
                return dataframes

            # Case 2: Single S3 object
            content = self.read_objects_safe(object_, make_readable=True)
            df = read_csv(content)

            logging.info("Converted single S3 object to DataFrame")
            return df

        except Exception as e:
            raise MyException(e, sys) from e


    def read_csv(self, filename: str, bucket_name: str) -> Union[DataFrame, List[DataFrame]]:
        """
        Reads one or more CSV files from the specified S3 bucket and converts them into DataFrame(s).

        Args:
            filename (str): The object key or prefix in the S3 bucket.
            bucket_name (str): Name of the S3 bucket.

        Returns:
            DataFrame | List[DataFrame]: 
                - A single DataFrame if one file is found
                - A list of DataFrames if multiple files match the prefix
        """

        logging.info("Entered the read_csv method of SimpleStorageService class")

        try:
            # Get S3 object(s) matching the prefix
            s3_objects = self.get_objects_by_prefix(filename, bucket_name)

            # Case 1: Multiple objects returned
            if isinstance(s3_objects, list):
                dataframes = []
                for obj in s3_objects:
                    content = self.read_objects_safe(obj, make_readable=True)
                    df = read_csv(content)
                    dataframes.append(df)

                logging.info(f"Loaded {len(dataframes)} CSV files from S3")
                return dataframes

            # Case 2: Single object returned
            content = self.read_objects_safe(s3_objects, make_readable=True)
            df = read_csv(content)

            logging.info("Loaded single CSV file from S3")
            return df

        except Exception as e:
            raise MyException(e, sys) from e
