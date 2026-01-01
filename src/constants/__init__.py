import os
from datetime import date
from dotenv import load_dotenv

# Loading environment variables
load_dotenv()

mongodb_username = os.getenv('MONGODB_USERNAME')
mongodb_password = os.getenv('MONGODB_PASSWORD')

if not mongodb_username or not mongodb_password:
    raise ValueError("MongoDB username or password is not set.")


AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    raise ValueError("AWS credentials are not set.")

AWS_REGION_NAME = "us-east-1"

# For MongoDB connection
DATABASE_NAME = "Vehicle-Insurance-DB"
COLLECTION_NAME = "Vehicle-Insurance-Data"
LOGIN_CREDENTIALS_COLLECTION_NAME = "Login-Credentials-Data"
MONGODB_URL_KEY = f"mongodb+srv://{mongodb_username}:{mongodb_password}@vehicle-insurance-clust.3qswxda.mongodb.net/?appName=Vehicle-Insurance-Cluster"

PIPELINE_NAME: str = ""
ARTIFACT_DIR: str = "artifact"

FILE_NAME: str = "data.csv"
TRAIN_FILE_NAME: str = "train.csv"
TEST_FILE_NAME: str = "test.csv"
SCHEMA_FILE_PATH = os.path.join("config", "schema.yaml")

TARGET_COLUMN = "Response"
PREPROCSSING_OBJECT_FILE_NAME = "preprocessing.pkl"

"""
Data Ingestion related constant start with DATA_INGESTION VAR NAME
"""
DATA_INGESTION_COLLECTION_NAME: str = "Vehicle-Insurance-Data"
DATA_INGESTION_DIR_NAME: str = "data_ingestion"
DATA_INGESTION_FEATURE_STORE_DIR: str = "feature_store"
DATA_INGESTION_INGESTED_DIR: str = "ingested"
DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO: float = 0.25


"""
Data Validation realted contant start with DATA_VALIDATION VAR NAME
"""
DATA_VALIDATION_DIR_NAME: str = "data_validation"
DATA_VALIDATION_REPORT_FILE_NAME: str = "report.yaml"


"""
Data Transformation ralated constant start with DATA_TRANSFORMATION VAR NAME
"""
DATA_TRANSFORMATION_DIR_NAME: str = "data_transformation"
DATA_TRANSFORMATION_TRANSFORMED_DATA_DIR: str = "transformed"
DATA_TRANSFORMATION_TRANSFORMED_OBJECT_DIR: str = "transformed_object"


"""
MODEL TRAINER related constant start with MODEL_TRAINER var name
"""
MODEL_TRAINER_DIR_NAME: str = "model_trainer"
MODEL_FILE_NAME = "model.pkl"
MODEL_TRAINER_METRICS_FILE_NAME = "metrics.yaml"
MODEL_TRAINER_EXPECTED_SCORE: float = 0.6
MODEL_TRAINER_N_ESTIMATORS = 300
MODEL_TRAINER_MIN_SAMPLES_SPLIT: int = 7
MODEL_TRAINER_MIN_SAMPLES_LEAF: int = 8
MIN_SAMPLES_SPLIT_MAX_DEPTH: int = 3
MIN_SAMPLES_SPLIT_CRITERION: str = 'gini'
RANDOM_STATE: int = 101

"""
MODEL Evaluation related constants
"""

MODEL_EVALUATION_DIR_NAME: str = "model_evaluation"
MODEL_EVALUATION_BEST_MODEL_COMPARISION_REPORT_FILE_NAME: str = "report.yaml"
MODEL_EVALUATION_CHANGED_THRESHOLD_SCORE: float = 0.02

"""
MODEL Pusher related constant
"""

S3_MODEL_FILE_NAME = "model.pkl"
S3_MODEL_METRICS_FILE_NAME = "metrics.yaml"
MODEL_BUCKET_NAME = "vehicle-insurance-model-store"

"""
Application Config
"""

APP_HOST = "0.0.0.0"
APP_PORT = 5000