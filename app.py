from fastapi import FastAPI, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from uvicorn import run as app_run
from typing import Optional
import hashlib
import sys

# Importing constants and pipeline modules from the project
from src.constants import APP_HOST, APP_PORT, LOGIN_CREDENTIALS_COLLECTION_NAME
from src.pipline.prediction_pipeline import VehicleData, VehicleDataClassifier
from src.pipline.training_pipeline import TrainPipeline
from src.data_access.proj1_data import Proj1Data
from src.exception import MyException
from src.logger import logging

# Initialize FastAPI application
app = FastAPI()

# Mount the 'static' directory for serving static files (like CSS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Set up Jinja2 template engine for rendering HTML templates
templates = Jinja2Templates(directory='templates')

# Allow all origins for Cross-Origin Resource Sharing (CORS)
origins = ["*"]

# Configure middleware to handle CORS, allowing requests from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==================== USER AUTHENTICATION FROM MONGODB ====================

class CredentialManager:
    """
    Manages user credential verification by loading from MongoDB.
    """
    
    def __init__(self):
        """
        Initialize the credential manager and load credentials from MongoDB.
        """
        try:
            self.data_access = Proj1Data()
            self.credentials_cache = {}
            self.load_credentials()
            
        except Exception as e:
            logging.error(f"Error initializing CredentialManager: {str(e)}")
            raise MyException(e, sys)
    
    def load_credentials(self):
        """
        Load all credentials from MongoDB into cache.
        """
        try:
            logging.info("Loading credentials from MongoDB...")
            df = self.data_access.export_collection_as_dataframe(
                collection_name = LOGIN_CREDENTIALS_COLLECTION_NAME
            )
            
            if df.empty:
                logging.warning("No credentials found in MongoDB. Using default credentials.")
                # Fallback to default credentials if MongoDB is empty
                self.credentials_cache = {
                    "admin": hashlib.sha256("admin123".encode()).hexdigest(),
                    "trainer": hashlib.sha256("train456".encode()).hexdigest(),
                }
            else:
                # Load credentials from MongoDB
                for _, row in df.iterrows():
                    self.credentials_cache[row['user_id']] = row['hashed_password']
                
                logging.info(f"Loaded {len(self.credentials_cache)} credentials from MongoDB.")
                
        except Exception as e:
            logging.error(f"Error loading credentials from MongoDB: {str(e)}")
            logging.info("Falling back to default credentials.")
            # Fallback to default credentials
            self.credentials_cache = {
                "admin": hashlib.sha256("admin123".encode()).hexdigest(),
                "trainer": hashlib.sha256("train456".encode()).hexdigest(),
            }
    
    def verify_credentials(self, user_id: str, password: str) -> bool:
        """
        Verify user credentials against cached credentials from MongoDB.
        
        Args:
            user_id: Username to verify
            password: Plain text password to verify
            
        Returns:
            bool: True if credentials are valid, False otherwise
        """
        try:
            if user_id not in self.credentials_cache:
                logging.warning(f"Login attempt with unknown user_id: {user_id}")
                return False
            
            # Hash the provided password and compare with stored hash
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            is_valid = self.credentials_cache[user_id] == hashed_password
            
            if is_valid:
                logging.info(f"Successful login for user: {user_id}")
            else:
                logging.warning(f"Failed login attempt for user: {user_id}")
            
            return is_valid
            
        except Exception as e:
            logging.error(f"Error verifying credentials: {str(e)}")
            return False
    
    def reload_credentials(self):
        """
        Reload credentials from MongoDB (useful after updates).
        """
        self.load_credentials()


# Initialize credential manager globally
try:
    credential_manager = CredentialManager()
except Exception as e:
    logging.error(f"Failed to initialize credential manager: {str(e)}")
    credential_manager = None


class DataForm:
    """
    DataForm class to handle and process incoming form data.
    This class defines the vehicle-related attributes expected from the form.
    """
    def __init__(self, request: Request):
        self.request: Request = request
        self.Gender: Optional[int] = None
        self.Age: Optional[int] = None
        self.Driving_License: Optional[int] = None
        self.Region_Code: Optional[float] = None
        self.Previously_Insured: Optional[int] = None
        self.Annual_Premium: Optional[float] = None
        self.Policy_Sales_Channel: Optional[float] = None
        self.Vintage: Optional[int] = None
        self.Vehicle_Age_lt_1_Year: Optional[int] = None
        self.Vehicle_Age_gt_2_Years: Optional[int] = None
        self.Vehicle_Damage_Yes: Optional[int] = None

    async def get_vehicle_data(self):
        """
        Method to retrieve and assign form data to class attributes.
        This method is asynchronous to handle form data fetching without blocking.
        """
        form = await self.request.form()
        self.Gender = form.get("Gender")
        self.Age = form.get("Age")
        self.Driving_License = form.get("Driving_License")
        self.Region_Code = form.get("Region_Code")
        self.Previously_Insured = form.get("Previously_Insured")
        self.Annual_Premium = form.get("Annual_Premium")
        self.Policy_Sales_Channel = form.get("Policy_Sales_Channel")
        self.Vintage = form.get("Vintage")
        self.Vehicle_Age_lt_1_Year = form.get("Vehicle_Age_lt_1_Year")
        self.Vehicle_Age_gt_2_Years = form.get("Vehicle_Age_gt_2_Years")
        self.Vehicle_Damage_Yes = form.get("Vehicle_Damage_Yes")


# Route to render the main page with the form
@app.get("/", tags=["authentication"])
async def index(request: Request):
    """
    Renders the main HTML form page for vehicle data input.
    """
    return templates.TemplateResponse(
        "vehicle_data.html", {"request": request, "context": None}
    )


# Route to trigger the model training process with authentication
@app.post("/train")
async def trainRouteClient(
    request: Request,
    user_id: str = Form(...),
    password: str = Form(...)
):
    """
    Endpoint to initiate the model training pipeline with user authentication from MongoDB.
    
    Args:
        request: FastAPI request object
        user_id: Username from the login form
        password: Password from the login form
        
    Returns:
        Template response with success/error message
    """
    try:
        # Check if credential manager is initialized
        if credential_manager is None:
            return templates.TemplateResponse(
                "vehicle_data.html",
                {
                    "request": request,
                    "error_message": "Authentication system is not available. Please contact administrator.",
                    "context": None
                }
            )
        
        # Verify user credentials from MongoDB
        if not credential_manager.verify_credentials(user_id, password):
            return templates.TemplateResponse(
                "vehicle_data.html",
                {
                    "request": request,
                    "error_message": "Invalid User ID or Password!",
                    "context": None
                }
            )
        
        # If credentials are valid, start training
        logging.info(f"Starting model training initiated by user: {user_id}")
        train_pipeline = TrainPipeline()
        train_pipeline.run_pipeline()
        
        return templates.TemplateResponse(
            "vehicle_data.html",
            {
                "request": request,
                "train_success": f"Training successful! Initiated by user: {user_id}",
                "context": None
            }
        )
    
    except Exception as e:
        logging.error(f"Training error: {str(e)}")
        return templates.TemplateResponse(
            "vehicle_data.html",
            {
                "request": request,
                "error_message": "Training Error: An error occurred during model training. Please check the logs.",
                "context": None
            }
        )


# Route to handle form submission and make predictions
@app.post("/")
async def predictRouteClient(request: Request):
    """
    Endpoint to receive form data, process it, and make a prediction.
    """
    try:
        form = DataForm(request)
        await form.get_vehicle_data()

        vehicle_data = VehicleData(
            Gender = form.Gender,
            Age = form.Age,
            Driving_License = form.Driving_License,
            Region_Code = form.Region_Code,
            Previously_Insured = form.Previously_Insured,
            Annual_Premium = form.Annual_Premium,
            Policy_Sales_Channel = form.Policy_Sales_Channel,
            Vintage = form.Vintage,
            Vehicle_Age_lt_1_Year = form.Vehicle_Age_lt_1_Year,
            Vehicle_Age_gt_2_Years = form.Vehicle_Age_gt_2_Years,
            Vehicle_Damage_Yes = form.Vehicle_Damage_Yes
        )

        # Convert form data into a DataFrame for the model
        vehicle_df = vehicle_data.get_vehicle_input_data_frame()

        # Initialize the prediction pipeline
        model_predictor = VehicleDataClassifier()

        # Make a prediction and retrieve the result
        value = model_predictor.predict(dataframe=vehicle_df)[0]

        # Interpret the prediction result as 'Response-Yes' or 'Response-No'
        status = "Response - Yes" if value == 1 else "Response - No"

        # Render the same HTML page with the prediction result
        return templates.TemplateResponse(
            "vehicle_data.html",
            {"request": request, "context": status}
        )

    except Exception as e:
        logging.error(f"Prediction error: {str(e)}")
        return templates.TemplateResponse(
            "vehicle_data.html",
            {"request": request, "error_message": "Prediction Error: An error occurred during prediction. Please check your input values."}
        )


# Optional: Endpoint to reload credentials (useful for development)
@app.get("/reload-credentials")
async def reload_credentials():
    """
    Reload credentials from MongoDB without restarting the server.
    """
    try:
        if credential_manager:
            credential_manager.reload_credentials()
            return {"status": "success", "message": "Credentials reloaded from MongoDB"}
        else:
            return {"status": "error", "message": "Credential manager not initialized"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


# Main entry point to start the FastAPI server
if __name__ == "__main__":
    app_run(app, host = APP_HOST, port = APP_PORT)