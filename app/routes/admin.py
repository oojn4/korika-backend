from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.admin import MalariaHealthFacilityMonthlyService, HealthFacilityIdService, UserService

from app.models import MalariaHealthFacilityMonthly, HealthFacilityId, User
from werkzeug.security import generate_password_hash, check_password_hash
# Blueprint dan routing untuk Malaria Health Facility Monthly
malaria_bp = Blueprint('malaria_api', __name__, url_prefix='')
malaria_service = MalariaHealthFacilityMonthlyService()

@malaria_bp.route('/malaria', methods=['GET'])
@jwt_required()
def get_all_malaria():
    data, error = malaria_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@malaria_bp.route('/malaria/paginated', methods=['GET'])
@jwt_required()
def get_paginated_malaria():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    month = request.args.get('month', type=int)
    year = request.args.get('year', type=int)
    status = request.args.get('status', type=str)
    sort_by = request.args.get('sort_by', 'id_mhfm')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Get filter parameters (can be extended based on your needs)
    filter_params = {}
    for key in request.args:
        if key in ['id_faskes', 'provinsi', 'kabupaten', 'kecamatan', 'tipe_faskes','status']:
            filter_params[key] = request.args.get(key)
    
    data, meta, error = malaria_service.get_paginated(
        page=page,
        per_page=per_page,
        month=month,
        year=year,
        status=status,
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

@malaria_bp.route('/malaria/<int:id_mhfm>', methods=['GET'])
@jwt_required()
def get_malaria_by_id(id_mhfm):
    data, error = malaria_service.get_by_id_mhfm(id_mhfm)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@malaria_bp.route('/malaria', methods=['POST'])
@jwt_required()
def create_malaria():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = malaria_service.create_record(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@malaria_bp.route('/malaria/<int:id_mhfm>', methods=['PUT'])
@jwt_required()
def update_malaria(id_mhfm):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = malaria_service.update_record(id_mhfm, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@malaria_bp.route('/malaria/<int:id_mhfm>', methods=['DELETE'])
@jwt_required()
def delete_malaria(id_mhfm):
    success, error = malaria_service.delete_record(id_mhfm)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@malaria_bp.route('/malaria/facility/<int:id_faskes>/period/<int:bulan>/<int:tahun>', methods=['GET'])
@jwt_required()
def get_malaria_by_facility_period(id_faskes, bulan, tahun):
    data, error = malaria_service.get_by_facility_and_period(id_faskes, bulan, tahun)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@malaria_bp.route('/malaria/facility/<int:id_faskes>/year/<int:tahun>', methods=['GET'])
@jwt_required()
def get_malaria_annual(id_faskes, tahun):
    data, error = malaria_service.get_annual_data(id_faskes, tahun)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

# Blueprint dan routing untuk Health Facility ID
facility_bp = Blueprint('facility_api', __name__, url_prefix='')
facility_service = HealthFacilityIdService()

@facility_bp.route('/facility', methods=['GET'])
@jwt_required()
def get_all_facilities():
    data, error = facility_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@facility_bp.route('/facility/<int:id_faskes>', methods=['GET'])
@jwt_required()
def get_facility_by_id(id_faskes):
    data, error = facility_service.get_by_id_faskes(id_faskes)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@facility_bp.route('/facility', methods=['POST'])
@jwt_required()
def create_facility():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = facility_service.create_record(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@facility_bp.route('/facility/<int:id_faskes>', methods=['PUT'])
@jwt_required()
def update_facility(id_faskes):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = facility_service.update_record(id_faskes, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@facility_bp.route('/facility/<int:id_faskes>', methods=['DELETE'])
@jwt_required()
def delete_facility(id_faskes):
    success, error = facility_service.delete_record(id_faskes)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@facility_bp.route('/location', methods=['GET'])
@jwt_required()
def get_facility_by_location():
    provinsi = request.args.get('provinsi')
    kabupaten = request.args.get('kabupaten')
    kecamatan = request.args.get('kecamatan')
    
    data, error = facility_service.get_by_location(provinsi, kabupaten, kecamatan)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@facility_bp.route('/facility/type/<string:tipe_faskes>', methods=['GET'])
@jwt_required()
def get_facility_by_type(tipe_faskes):
    data, error = facility_service.get_by_facility_type(tipe_faskes)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@facility_bp.route('/facility/paginated', methods=['GET'])
@jwt_required()
def get_paginated_facilities():
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'id_faskes')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Get filter parameters
    filter_params = {}
    for key in request.args:
        if key in ['nama_faskes', 'provinsi', 'kabupaten', 'kecamatan', 'tipe_faskes']:
            filter_params[key] = request.args.get(key)
    
    data, meta, error = facility_service.get_paginated(
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