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
        from app.routes import auth,data,user,dbd,lepto,climate,master_prov,master_kab,master_kec
        
        # Register blueprints
        app.register_blueprint(auth.bp)
        app.register_blueprint(data.bp)
        app.register_blueprint(master_prov.masterprov_bp)
        app.register_blueprint(master_kab.masterkab_bp)
        app.register_blueprint(master_kec.masterkec_bp)
        app.register_blueprint(dbd.dbd_bp)
        app.register_blueprint(lepto.lepto_bp)
        app.register_blueprint(climate.climate_bp)
        app.register_blueprint(user.user_bp)
        db.create_all()
    
    return app