from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.master_kab import MasterKabService
from app.models.db_models import MasterProv

# Blueprint untuk Master Kabupaten
masterkab_bp = Blueprint('masterkab_api', __name__, url_prefix='')
masterkab_service = MasterKabService()

@masterkab_bp.route('/cities', methods=['GET'])
@jwt_required()
def get_all_cities():
    data, error = masterkab_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkab_bp.route('/cities/<string:kd_kab>', methods=['GET'])
@jwt_required()
def get_city_by_id(kd_kab):
    data, error = masterkab_service.get_by_id(kd_kab)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkab_bp.route('/cities', methods=['POST'])
@jwt_required()
def create_city():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = masterkab_service.create_city(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@masterkab_bp.route('/cities/<string:kd_kab>', methods=['PUT'])
@jwt_required()
def update_city(kd_kab):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = masterkab_service.update_city(kd_kab, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@masterkab_bp.route('/cities/<string:kd_kab>', methods=['DELETE'])
@jwt_required()
def delete_city(kd_kab):
    success, error = masterkab_service.delete_city(kd_kab)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@masterkab_bp.route('/cities/delete-many', methods=['POST'])
@jwt_required()
def delete_many_cities():
    data = request.get_json()
    if not data or 'ids' not in data:
        return jsonify({"success": False, "error": "No IDs provided"}), 400
    
    result, error = masterkab_service.delete_many_cities(data['ids'])
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@masterkab_bp.route('/cities/province/<string:kd_prov>', methods=['GET'])
@jwt_required()
def get_cities_by_province(kd_prov):
    data, error = masterkab_service.get_by_province(kd_prov)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkab_bp.route('/cities/bmkg/<string:kd_bmkg>', methods=['GET'])
@jwt_required()
def get_city_by_bmkg(kd_bmkg):
    data, error = masterkab_service.get_by_bmkg_code(kd_bmkg)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkab_bp.route('/cities/endemic/<string:status_endemis>', methods=['GET'])
@jwt_required()
def get_cities_by_endemic_status(status_endemis):
    data, error = masterkab_service.get_by_endemic_status(status_endemis)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkab_bp.route('/cities/paginated', methods=['GET'])
@jwt_required()
def get_paginated_cities():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'kd_kab')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Validate pagination parameters
    if page < 1:
        return jsonify({"success": False, "error": "Page must be greater than 0"}), 400
    if per_page < 1 or per_page > 100:
        return jsonify({"success": False, "error": "Per page must be between 1 and 100"}), 400
    
    filter_params = {}
    for key in request.args:
        if key in ['kabkot', 'kd_kab', 'kd_prov', 'status_endemis', 'kd_bmkg']:
            filter_params[key] = request.args.get(key)
    
    data, meta, error = masterkab_service.get_paginated(
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        filters=filter_params
    )
    
    if error:
        return jsonify({"success": False, "error": error}), 400
    
    # Enhance data with province objects
    enhanced_data = []
    for item in data:
        # Create enhanced item with additional province info
        enhanced_item = item.copy()
        
        # Add province object
        if 'kd_prov' in item and item['kd_prov']:
            try:
                province = MasterProv.query.filter_by(kd_prov=item['kd_prov']).first()
                if province:
                    enhanced_item['province'] = {
                        'kd_prov': province.kd_prov,
                        'provinsi': province.provinsi,
                        'kd_bmkg': province.kd_bmkg
                    }
                else:
                    enhanced_item['province'] = None
            except Exception as e:
                enhanced_item['province'] = None
        else:
            enhanced_item['province'] = None
        
        enhanced_data.append(enhanced_item)
    
    return jsonify({
        "success": True, 
        "data": enhanced_data,
        "meta": meta
    }), 200