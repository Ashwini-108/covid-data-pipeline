import boto3
import os
from datetime import datetime

# Configuration
BUCKET_NAME = 'covid-data-pipeline-ashwini'
FILE_NAME = 'covid_data.csv'

def upload_to_s3():
    """
    Upload existing CSV file to S3 bucket
    """
    print("🚀 Uploading to S3...")
    
    # Check if file exists
    if not os.path.exists(FILE_NAME):
        print(f"❌ File '{FILE_NAME}' not found. Please run main.py first.")
        return
    
    # Show file info
    file_size = os.path.getsize(FILE_NAME)
    print(f"📁 File: {FILE_NAME} ({file_size:,} bytes)")
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    try:
        # Upload file to S3 (this will OVERWRITE the existing file)
        s3.upload_file(
            FILE_NAME, 
            BUCKET_NAME, 
            FILE_NAME,
            ExtraArgs={
                'Metadata': {
                    'data-source': 'MySQL',
                    'upload-date': datetime.utcnow().strftime('%Y-%m-%d-%H-%M')
                }
            }
        )
        
        print("✅ SUCCESS! File uploaded to S3!")
        print(f"📍 S3 Location: s3://{BUCKET_NAME}/{FILE_NAME}")
        
        # Show public URL
        s3_url = f"https://{BUCKET_NAME}.s3.ap-south-1.amazonaws.com/{FILE_NAME}"
        print(f"🌐 Public URL: {s3_url}")
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        print("💡 Check your AWS credentials and bucket permissions")

if __name__ == "__main__":
    upload_to_s3()