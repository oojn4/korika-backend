from flask import Blueprint, request, jsonify
from flask_jwt_extended import create_access_token, jwt_required, get_jwt_identity
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import timedelta
from app.models.db_models import User
from app import db

bp = Blueprint('auth', __name__, url_prefix='')

@bp.route('/signup', methods=['POST'])
def signup():
    data = request.json
    email = data.get('email')
    password = data.get('password')
    full_name = data.get('full_name')
    phone_number = data.get('phone_number')
    address_1 = data.get('address_1')
    address_2 = data.get('address_2')
    access_level = data.get('access_level')
    
    if not email or not password:
        return jsonify({"error": "Email and password are required"}), 400

    # Cek apakah user sudah ada
    existing_user = User.query.filter_by(email=email).first()
    if existing_user:
        return jsonify({"error": "User already exists"}), 400

    # Hash password
    hashed_password = generate_password_hash(password, method='pbkdf2:sha256')
    
    # Buat user baru
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
    db.session.flush()  # Pastikan ID user tersedia sebelum menyimpan relasi

    db.session.commit()

    # Buat access_token
    access_token = create_access_token(identity={'email': new_user.email}, expires_delta=timedelta(hours=1))

    return jsonify({"message": "Login successful", "access_token": access_token,"user":new_user.to_dict(),"success":True}), 201

@bp.route('/signin', methods=['POST'])
def signin():
    data = request.json
    email = data.get('email')
    password = data.get('password')

    user = User.query.filter_by(email=email).first()
    if not user or not check_password_hash(user.password, password):
        return jsonify({"error": "Invalid credentials"}), 401

    # Buat token JWT
    access_token = create_access_token(
        identity={'username': user.email, 'access_level': user.access_level}, 
        expires_delta=timedelta(hours=1)
    )
    
    return jsonify({
        "message": "Login successful", 
        "access_token": access_token,
        "user": user.to_dict(),
        "success": True
    }), 200

# Middleware untuk memeriksa akses berdasarkan role
def role_required(required_role):
    def decorator(func):
        @jwt_required()
        def wrapper(*args, **kwargs):
            identity = get_jwt_identity()
            if identity.get('access_level') != required_role:
                return jsonify({"error": "Access denied"}), 403
            return func(*args, **kwargs)
        return wrapper
    return decorator