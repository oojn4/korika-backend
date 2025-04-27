from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
from flask_jwt_extended.exceptions import JWTExtendedException
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import timedelta
from app.models.db_models import User
from app import db, jwt

bp = Blueprint('auth', __name__, url_prefix='')

@bp.route('/signup', methods=['POST'])
def signup():
    try:
        data = request.json
        email = data.get('email')
        password = data.get('password')
        full_name = data.get('full_name')
        phone_number = data.get('phone_number')
        address_1 = data.get('address_1')
        address_2 = data.get('address_2')
        access_level = data.get('access_level')
        
        if not email or not password:
            return jsonify({
                "status": 400,
                "message": "Email dan password dibutuhkan",
                "success": False
            }), 400
        
        # Check if user already exists
        existing_user = User.query.filter_by(email=email).first()
        if existing_user:
            return jsonify({
                "status": 400,
                "message": "User dengan email tersebut sudah terdaftar",
                "success": False
            }), 400
        
        # Hash password
        hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
        
        # Create new user
        new_user = User(
            email=email,
            password=hashed_password,
            full_name=full_name,
            phone_number=phone_number,
            address_1=address_1,
            address_2=address_2,
            access_level=access_level,
        )
        db.session.add(new_user)
        db.session.flush()  # Ensure user ID is available before saving relations
        
        db.session.commit()
        
        # Create access token
        access_token = create_access_token(
            identity={'email': new_user.email},
            expires_delta=timedelta(hours=1)
        )
        
        return jsonify({
            "message": "Pendaftaran berhasil",
            "access_token": access_token,
            "user": new_user.to_dict(),
            "success": True
        }), 201
        
    except Exception as e:
        db.session.rollback()
        return jsonify({
            "status": 500,
            "message": "Terjadi kesalahan, silahkan coba lagi",
            "success": False
        }), 500
        
    finally:
        db.session.close()
        
@bp.route('/signin', methods=['POST'])
def signin():
    try:
        data = request.json
        email = data.get('email')
        password = data.get('password')
        
        user = User.query.filter_by(email=email).first()
        if not user or not check_password_hash(user.password, password):
            return jsonify({
                "status": 401,
                "message": "Email atau Password salah",
                "success": False
            }), 401
        
        # Create JWT token
        access_token = create_access_token(
            identity={'username': user.email, 'access_level': user.access_level},
            expires_delta=timedelta(hours=1)
        )
        
        return jsonify({
            "message": "Login berhasil",
            "access_token": access_token,
            "user": user.to_dict(),
            "success": True
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": 500,
            "message": "Terjadi kesalahan, silahkan coba lagi",
            "success": False
        }), 500

# Register error handler for expired tokens
@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    return jsonify({
        "status": 401,
        "message": "Session has expired. Please log in again.",
        "success": False
    }), 401

# Register error handler for invalid tokens
@jwt.invalid_token_loader
def invalid_token_callback(error_string):
    return jsonify({
        "status": 401,
        "message": "Invalid token. Please log in again.",
        "success": False
    }), 401

# Middleware to check access based on role
def role_required(required_role):
    def decorator(func):
        @jwt_required()
        def wrapper(*args, **kwargs):
            try:
                identity = get_jwt_identity()
                if identity.get('access_level') != required_role:
                    return jsonify({
                        "status": 403,
                        "message": "Access denied",
                        "success": False
                    }), 403
                return func(*args, **kwargs)
            except ExpiredSignatureError:
                return jsonify({
                    "status": 401,
                    "message": "Token has expired. Please log in again.",
                    "success": False
                }), 401
            except InvalidTokenError:
                return jsonify({
                    "status": 401,
                    "message": "Invalid token. Please log in again.",
                    "success": False
                }), 401
            except Exception as e:
                return jsonify({
                    "status": 500,
                    "message": str(e),
                    "success": False
                }), 500
        return wrapper
    return decorator