from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.master_prov import MasterProvService
# Blueprint untuk Master Province
masterprov_bp = Blueprint('masterprov_api', __name__, url_prefix='')
masterprov_service = MasterProvService()

@masterprov_bp.route('/provinces', methods=['GET'])
@jwt_required()
def get_all_provinces():
    data, error = masterprov_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterprov_bp.route('/provinces/<string:kd_prov>', methods=['GET'])
@jwt_required()
def get_province_by_id(kd_prov):
    data, error = masterprov_service.get_by_id(kd_prov)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterprov_bp.route('/provinces', methods=['POST'])
@jwt_required()
def create_province():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = masterprov_service.create_province(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@masterprov_bp.route('/provinces/<string:kd_prov>', methods=['PUT'])
@jwt_required()
def update_province(kd_prov):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    result, error = masterprov_service.update_province(kd_prov, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@masterprov_bp.route('/provinces/<string:kd_prov>', methods=['DELETE'])
@jwt_required()
def delete_province(kd_prov):
    success, error = masterprov_service.delete_province(kd_prov)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@masterprov_bp.route('/provinces/delete-many', methods=['POST'])
@jwt_required()
def delete_many_provinces():
    data = request.get_json()
    if not data or 'ids' not in data:
        return jsonify({"success": False, "error": "No IDs provided"}), 400
    
    result, error = masterprov_service.delete_many_provinces(data['ids'])
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@masterprov_bp.route('/provinces/bmkg/<string:kd_bmkg>', methods=['GET'])
@jwt_required()
def get_province_by_bmkg(kd_bmkg):
    data, error = masterprov_service.get_by_bmkg_code(kd_bmkg)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@masterprov_bp.route('/provinces/paginated', methods=['GET'])
@jwt_required()
def get_paginated_provinces():
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'kd_prov')
    sort_order = request.args.get('sort_order', 'asc')
    
    filter_params = {}
    for key in request.args:
        if key in ['provinsi', 'kd_prov', 'kd_bmkg']:
            filter_params[key] = request.args.get(key)
    
    data, meta, error = masterprov_service.get_paginated(
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
