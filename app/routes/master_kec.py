from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.master_kec import MasterKecService

from app.models.db_models import MasterKab, MasterProv
# Blueprint untuk Master Kecamatan
masterkec_bp = Blueprint('masterkec_api', __name__, url_prefix='')
masterkec_service = MasterKecService()

@masterkec_bp.route('/districts', methods=['GET'])
@jwt_required()
def get_all_districts():
    data, error = masterkec_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkec_bp.route('/districts/<string:kd_kec>', methods=['GET'])
@jwt_required()
def get_district_by_id(kd_kec):
    data, error = masterkec_service.get_by_id(kd_kec)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkec_bp.route('/districts', methods=['POST'])
@jwt_required()
def create_district():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = masterkec_service.create_district(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@masterkec_bp.route('/districts/<string:kd_kec>', methods=['PUT'])
@jwt_required()
def update_district(kd_kec):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = masterkec_service.update_district(kd_kec, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@masterkec_bp.route('/districts/<string:kd_kec>', methods=['DELETE'])
@jwt_required()
def delete_district(kd_kec):
    success, error = masterkec_service.delete_district(kd_kec)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@masterkec_bp.route('/districts/delete-many', methods=['POST'])
@jwt_required()
def delete_many_districts():
    data = request.get_json()
    if not data or 'ids' not in data:
        return jsonify({"success": False, "error": "No IDs provided"}), 400
    
    result, error = masterkec_service.delete_many_districts(data['ids'])
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@masterkec_bp.route('/districts/city/<string:kd_kab>', methods=['GET'])
@jwt_required()
def get_districts_by_city(kd_kab):
    data, error = masterkec_service.get_by_city(kd_kab)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkec_bp.route('/districts/province/<string:kd_prov>', methods=['GET'])
@jwt_required()
def get_districts_by_province(kd_prov):
    data, error = masterkec_service.get_by_province(kd_prov)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkec_bp.route('/districts/bmkg/<string:kd_bmkg>', methods=['GET'])
@jwt_required()
def get_district_by_bmkg(kd_bmkg):
    data, error = masterkec_service.get_by_bmkg_code(kd_bmkg)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterkec_bp.route('/districts/paginated', methods=['GET'])
@jwt_required()
def get_paginated_districts():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'kd_kec')
    sort_order = request.args.get('sort_order', 'asc')
    
    # Validate pagination parameters
    if page < 1:
        return jsonify({"success": False, "error": "Page must be greater than 0"}), 400
    if per_page < 1 or per_page > 100:
        return jsonify({"success": False, "error": "Per page must be between 1 and 100"}), 400
    
    filter_params = {}
    for key in request.args:
        if key in ['kecamatan', 'kd_kec', 'kd_kab', 'kd_prov', 'kd_bmkg']:
            filter_params[key] = request.args.get(key)
    
    data, meta, error = masterkec_service.get_paginated(
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        filters=filter_params
    )
    
    if error:
        return jsonify({"success": False, "error": error}), 400
    
    # Enhance data with province and city objects
    enhanced_data = []
    for item in data:
        # Create enhanced item with additional province and city info
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
        
        # Add city object
        if 'kd_kab' in item and item['kd_kab']:
            try:
                city = MasterKab.query.filter_by(kd_kab=item['kd_kab']).first()
                if city:
                    enhanced_item['city'] = {
                        'kd_kab': city.kd_kab,
                        'kabkot': city.kabkot,
                        'kd_bmkg': city.kd_bmkg,
                        'status_endemis': city.status_endemis
                    }
                else:
                    enhanced_item['city'] = None
            except Exception as e:
                enhanced_item['city'] = None
        else:
            enhanced_item['city'] = None
        
        enhanced_data.append(enhanced_item)
    
    return jsonify({
        "success": True, 
        "data": enhanced_data,
        "meta": meta
    }), 200