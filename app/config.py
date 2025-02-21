import os
import urllib.parse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    # Database configuration
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    if DB_PASSWORD is not None:
        DB_PASSWORD = urllib.parse.quote_plus(DB_PASSWORD)
    else:
        # Handle missing password - either set a default or raise an error
        raise ValueError("DB_PASSWORD environment variable is not set")
    DB_HOST = os.getenv('DB_HOST')
    
    SQLALCHEMY_DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # JWT configuration
    SECRET_KEY = os.getenv('SECRET_KEY', 's3creeer2312k3wewad!')
    JWT_SECRET_KEY = os.getenv('JWT_SECRET_KEY', 's3creeer2312k3wewad!')
    
    # File uploads configuration
    UPLOAD_FOLDER = 'uploads'
    RESULT_FOLDER = 'results'
    MODELS_FOLDER = 'models'
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max upload
    
    # ML Model configuration
    ML_EPOCHS = int(os.getenv('ML_EPOCHS', '5'))
    ML_WINDOW_LENGTH = int(os.getenv('ML_WINDOW_LENGTH', '6'))
    ML_BATCH_SIZE = int(os.getenv('ML_BATCH_SIZE', '16'))