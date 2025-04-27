from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.admin import UserService

from app.models import User
from werkzeug.security import generate_password_hash, check_password_hash

# Blueprint dan routing untuk User
user_bp = Blueprint('user_api', __name__, url_prefix='')
user_service = UserService()

@user_bp.route('/users', methods=['GET'])
@jwt_required()
def get_all_users():
    data, error = user_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@user_bp.route('/users/<int:user_id>', methods=['GET'])
@jwt_required()
def get_user_by_id(user_id):
    data, error = user_service.get_by_id(user_id)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@user_bp.route('/users/<int:user_id>', methods=['PUT'])
@jwt_required()
def update_user(user_id):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400

    # Jika ada password, hash terlebih dahulu
    if 'password' in data and data['password']:
        data['password'] = generate_password_hash(data['password'], method='pbkdf2:sha256')
    elif 'password' in data:
        # Jika password kosong, hapus dari data sehingga tidak mengubah password yang ada
        del data['password']
    
    result, error = user_service.update_user(user_id, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@user_bp.route('/users/<int:user_id>', methods=['DELETE'])
@jwt_required()
def delete_user(user_id):
    success, error = user_service.delete_user(user_id)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@user_bp.route('/users/email/<string:email>', methods=['GET'])
@jwt_required()
def get_user_by_email(email):
    data, error = user_service.get_by_email(email)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@user_bp.route('/users/access-level/<string:access_level>', methods=['GET'])
@jwt_required()
def get_users_by_access_level(access_level):
    data, error = user_service.get_by_access_level(access_level)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200
@user_bp.route('/users/paginated', methods=['GET'])
@jwt_required()
def get_paginated_users():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'id')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Get filter parameters
    filter_params = {}
    for key in request.args:
        if key in ['name', 'email', 'access_level']:
            filter_params[key] = request.args.get(key)
    
    data, meta, error = user_service.get_paginated(
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        filters=filter_params
    )
    
    if error:
        return jsonify({"success": False, "error": error}), 400
    
    return jsonify({
        "success": True, 
        "data": data,
        "meta": meta
    }), 200
