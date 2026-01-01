# # Below code is to check the logging config

# from src.logger import logging

# logging.debug("This is a debug message.")
# logging.info("This is an info message.")
# logging.warning("This is a warning message.")
# logging.error("This is an error message.")
# logging.critical("This is a critical message.")



# # Below code is to check the exception config

# from src.logger import logging
# from src.exception import MyException
# import sys

# try:
#     a = 1+'Z'
# except Exception as e:
#     logging.info(e)
#     raise MyException(e, sys) from e


# from src.pipline.training_pipeline import TrainPipeline

# pipline = TrainPipeline()
# pipline.run_pipeline()

import sys
from Upload_Training_Credential import CredentialUploader
from src.exception import MyException
from src.logger import logging


def upload_demo_credentials():
    """
    Uploads demo/sample credentials to MongoDB for testing purposes.
    """
    try:
        print("=" * 60)
        print("Starting Demo Credential Upload Process")
        print("=" * 60)
        
        # Initialize the credential uploader
        uploader = CredentialUploader()
        
        # Define demo credentials to upload
        demo_credentials = [
            {'user_id': 'Manoj_S', 'password': 'Manoj@2006'}
        ]
        
        print(f"\n📤 Uploading {len(demo_credentials)} credentials to MongoDB...")
        print("\nCredentials to be uploaded:")
        for i, cred in enumerate(demo_credentials, 1):
            print(f"  {i}. User ID: {cred['user_id']}")
        
        # Upload credentials to MongoDB
        uploader.upload_credentials(demo_credentials)
        
        print("\n" + "=" * 60)
        print("Verifying uploaded credentials...")
        print("=" * 60)
        
        # Fetch and display uploaded credentials (without passwords)
        df = uploader.fetch_all_credentials()
        
        if not df.empty:
            print(f"\n✅ Total credentials in database: {len(df)}")
            print("\nCredentials Summary:")
            print("-" * 60)
            
            # Display user_id and created_at timestamp
            for idx, row in df.iterrows():
                print(f"User ID: {row['user_id']:15} | Created: {row['created_at']}")
            
            print("-" * 60)
        else:
            print("\n⚠️ No credentials found in database.")
        
        print("\n" + "=" * 60)
        print("✅ Demo Credential Upload Completed Successfully!")
        print("=" * 60)
        
        print("\n📝 You can now use these credentials to login:")
        for cred in demo_credentials:
            print(f"   - User ID: {cred['user_id']:15} | Password: {cred['password']}")
        
    except Exception as e:
        print(f"\n❌ Error occurred during credential upload: {str(e)}")
        logging.error(f"Error in upload_demo_credentials: {str(e)}")
        raise MyException(e, sys)


if __name__ == "__main__":
    try:
        upload_demo_credentials()
    except Exception as e:
        print(f"\n❌ Fatal Error: {str(e)}")
        sys.exit(1)