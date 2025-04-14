from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_jwt_extended import JWTManager
from flask_cors import CORS
from flask_swagger_ui import get_swaggerui_blueprint
import os

# Initialize SQLAlchemy
db = SQLAlchemy()
jwt = JWTManager()

def create_app():
    # Initialize Flask application
    app = Flask(__name__)
    
    # Load configuration
    from app.config import Config
    app.config.from_object(Config)
    
    # Create required directories
    for folder in [app.config['UPLOAD_FOLDER'], app.config['RESULT_FOLDER'], app.config['MODELS_FOLDER']]:
        os.makedirs(folder, exist_ok=True)
    
    # Initialize extensions
    db.init_app(app)
    jwt.init_app(app)
    CORS(app, resources={r"/*": {"origins": "*"}})
    
    # Register Swagger UI blueprint
    SWAGGER_URL = '/swagger'
    API_URL = '/static/swagger.yaml'
    swaggerui_blueprint = get_swaggerui_blueprint(SWAGGER_URL, API_URL, config={'app_name': "CSI Korika"})
    app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)
    
    # Register routes
    with app.app_context():
        # Import routes
        from app.routes import auth,data
        
        # Register blueprints
        app.register_blueprint(auth.bp)
        app.register_blueprint(data.bp)
        # app.register_blueprint(ml.bp)
        # app.register_blueprint(admin.malaria_bp)
        # app.register_blueprint(admin.facility_bp)
        # app.register_blueprint(admin.user_bp)
        # app.register_blueprint(admin.env_bp)
        
        # You can use db.create_all() again if you're not using migrations
        db.create_all()
    
    return app