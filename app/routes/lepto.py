from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.lepto import LeptoService
from datetime import datetime
from app.models.db_models import MasterKab, MasterProv

# Blueprint untuk Lepto
lepto_bp = Blueprint('lepto_api', __name__, url_prefix='')
lepto_service = LeptoService()

@lepto_bp.route('/lepto', methods=['GET'])
@jwt_required()
def get_all_lepto():
    data, error = lepto_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto/<int:id_lepto>', methods=['GET'])
@jwt_required()
def get_lepto_by_id(id_lepto):
    data, error = lepto_service.get_by_id(id_lepto)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto', methods=['POST'])
@jwt_required()
def create_lepto():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    # Validate required fields
    required_fields = ['kd_prov', 'kd_kab', 'tahun', 'bulan']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None]
    if missing_fields:
        return jsonify({"success": False, "error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
    
    # Validate month and year
    if data.get('bulan') and (data['bulan'] < 1 or data['bulan'] > 12):
        return jsonify({"success": False, "error": "Month must be between 1 and 12"}), 400
    
    if data.get('tahun') and (data['tahun'] < 1900 or data['tahun'] > 2100):
        return jsonify({"success": False, "error": "Invalid year"}), 400
    
    # Add created_by from JWT token
    current_user = get_jwt_identity()
    if current_user:
        data['created_by'] = current_user.get('id') if isinstance(current_user, dict) else current_user
    
    result, error = lepto_service.create_lepto(data)
    print(result, error)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@lepto_bp.route('/lepto/<int:id_lepto>', methods=['PUT'])
@jwt_required()
def update_lepto(id_lepto):
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    # Validate month and year if provided
    if 'bulan' in data and data['bulan'] is not None:
        if data['bulan'] < 1 or data['bulan'] > 12:
            return jsonify({"success": False, "error": "Month must be between 1 and 12"}), 400
    
    if 'tahun' in data and data['tahun'] is not None:
        if data['tahun'] < 1900 or data['tahun'] > 2100:
            return jsonify({"success": False, "error": "Invalid year"}), 400
    
    # Add updated_by from JWT token
    current_user = get_jwt_identity()
    if current_user:
        data['updated_by'] = current_user.get('id') if isinstance(current_user, dict) else current_user
    
    result, error = lepto_service.update_lepto(id_lepto, data)
    print(result, error)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@lepto_bp.route('/lepto/<int:id_lepto>', methods=['DELETE'])
@jwt_required()
def delete_lepto(id_lepto):
    success, error = lepto_service.delete_lepto(id_lepto)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@lepto_bp.route('/lepto/delete-many', methods=['POST'])
@jwt_required()
def delete_many_lepto():
    data = request.get_json()
    if not data or 'ids' not in data:
        return jsonify({"success": False, "error": "No IDs provided"}), 400
    
    if not isinstance(data['ids'], list) or len(data['ids']) == 0:
        return jsonify({"success": False, "error": "IDs must be a non-empty list"}), 400
    
    # Validate all IDs are integers
    try:
        ids = [int(id_val) for id_val in data['ids']]
    except ValueError:
        return jsonify({"success": False, "error": "All IDs must be valid integers"}), 400
    
    result, error = lepto_service.delete_many_lepto(ids)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@lepto_bp.route('/lepto/province/<string:kd_prov>', methods=['GET'])
@jwt_required()
def get_lepto_by_province(kd_prov):
    data, error = lepto_service.get_by_province(kd_prov)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto/city/<string:kd_kab>', methods=['GET'])
@jwt_required()
def get_lepto_by_city(kd_kab):
    data, error = lepto_service.get_by_city(kd_kab)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto/year/<int:tahun>', methods=['GET'])
@jwt_required()
def get_lepto_by_year(tahun):
    if tahun < 1900 or tahun > 2100:
        return jsonify({"success": False, "error": "Invalid year"}), 400
    
    data, error = lepto_service.get_by_year(tahun)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto/month-year/<int:bulan>/<int:tahun>', methods=['GET'])
@jwt_required()
def get_lepto_by_month_year(bulan, tahun):
    if bulan < 1 or bulan > 12:
        return jsonify({"success": False, "error": "Month must be between 1 and 12"}), 400
    if tahun < 1900 or tahun > 2100:
        return jsonify({"success": False, "error": "Invalid year"}), 400
    
    data, error = lepto_service.get_by_month_year(bulan, tahun)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto/status/<string:status>', methods=['GET'])
@jwt_required()
def get_lepto_by_status(status):
    data, error = lepto_service.get_by_status(status)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@lepto_bp.route('/lepto/paginated', methods=['GET'])
@jwt_required()
def get_paginated_lepto():
    # Get pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'id_lepto')
    sort_order = request.args.get('sort_order', 'desc')
    
    # Validate pagination parameters
    if page < 1:
        return jsonify({"success": False, "error": "Page must be greater than 0"}), 400
    if per_page < 1 or per_page > 100:
        return jsonify({"success": False, "error": "Per page must be between 1 and 100"}), 400
    
    # Get filter parameters
    filter_params = {}
    allowed_filters = ['kd_prov', 'kd_kab', 'tahun', 'bulan', 'status', 'year_range']
    for key in request.args:
        if key in allowed_filters:
            filter_params[key] = request.args.get(key)
    
    # Validate year and month if provided
    if 'tahun' in filter_params:
        try:
            year = int(filter_params['tahun'])
            if year < 1900 or year > 2100:
                return jsonify({"success": False, "error": "Invalid year"}), 400
        except ValueError:
            return jsonify({"success": False, "error": "Year must be a number"}), 400
    
    if 'bulan' in filter_params:
        try:
            month = int(filter_params['bulan'])
            if month < 1 or month > 12:
                return jsonify({"success": False, "error": "Month must be between 1 and 12"}), 400
        except ValueError:
            return jsonify({"success": False, "error": "Month must be a number"}), 400
    
    data, meta, error = lepto_service.get_paginated(
        page=page,
        per_page=per_page,
        sort_by=sort_by,
        sort_order=sort_order,
        filters=filter_params
    )
    
    if error:
        return jsonify({"success": False, "error": error}), 400
    
    # Enhance data with province and district objects
    enhanced_data = []
    for item in data:
        # Create enhanced item with additional province and district info
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
        
        enhanced_data.append(enhanced_item)
    
    return jsonify({
        "success": True, 
        "data": enhanced_data,
        "meta": meta
    }), 200