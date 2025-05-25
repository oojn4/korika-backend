from flask import Blueprint, request, jsonify
from flask_jwt_extended import jwt_required, get_jwt_identity
from app.services.climate import ClimateMonthlyService
from datetime import datetime

# Blueprint untuk Climate Monthly
climate_bp = Blueprint('climate_api', __name__, url_prefix='')
climate_service = ClimateMonthlyService()

@climate_bp.route('/climate-monthly', methods=['GET'])
@jwt_required()
def get_all_climate():
    data, error = climate_service.get_all()
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/<int:id_climate>', methods=['GET'])
@jwt_required()
def get_climate_by_id(id_climate):
    data, error = climate_service.get_by_id(id_climate)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly', methods=['POST'])
@jwt_required()
def create_climate():
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    # Validate required fields
    required_fields = ['kd_prov', 'kd_kab', 'kd_kec', 'tahun', 'bulan']
    missing_fields = [field for field in required_fields if field not in data or data[field] is None]
    if missing_fields:
        return jsonify({"success": False, "error": f"Missing required fields: {', '.join(missing_fields)}"}), 400
    
    # Validate month and year
    if data.get('bulan') and (data['bulan'] < 1 or data['bulan'] > 12):
        return jsonify({"success": False, "error": "Month must be between 1 and 12"}), 400
    
    if data.get('tahun') and (data['tahun'] < 1900 or data['tahun'] > 2100):
        return jsonify({"success": False, "error": "Invalid year"}), 400
    
    # Validate climate data ranges (optional but recommended)
    climate_validations = {
        'tm_tm_mean': (-50, 60),  # Temperature in Celsius
        'rh_rh_mean': (0, 100),   # Humidity percentage
        'hujan_hujan_mean': (0, 1000)  # Rainfall in mm
    }
    
    for field, (min_val, max_val) in climate_validations.items():
        if field in data and data[field] is not None:
            try:
                value = float(data[field])
                if value < min_val or value > max_val:
                    return jsonify({"success": False, "error": f"{field} must be between {min_val} and {max_val}"}), 400
            except (ValueError, TypeError):
                return jsonify({"success": False, "error": f"{field} must be a valid number"}), 400
    
    # Add created_by from JWT token
    current_user = get_jwt_identity()
    if current_user:
        data['created_by'] = current_user.get('id') if isinstance(current_user, dict) else current_user
    
    result, error = climate_service.create_climate(data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 201

@climate_bp.route('/climate-monthly/<int:id_climate>', methods=['PUT'])
@jwt_required()
def update_climate(id_climate):
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
    
    # Validate climate data ranges if provided
    climate_validations = {
        'tm_tm_mean': (-50, 60),  # Temperature in Celsius
        'rh_rh_mean': (0, 100),   # Humidity percentage
        'hujan_hujan_mean': (0, 1000)  # Rainfall in mm
    }
    
    for field, (min_val, max_val) in climate_validations.items():
        if field in data and data[field] is not None:
            try:
                value = float(data[field])
                if value < min_val or value > max_val:
                    return jsonify({"success": False, "error": f"{field} must be between {min_val} and {max_val}"}), 400
            except (ValueError, TypeError):
                return jsonify({"success": False, "error": f"{field} must be a valid number"}), 400
    
    # Add updated_by from JWT token
    current_user = get_jwt_identity()
    if current_user:
        data['updated_by'] = current_user.get('id') if isinstance(current_user, dict) else current_user
    
    result, error = climate_service.update_climate(id_climate, data)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@climate_bp.route('/climate-monthly/<int:id_climate>', methods=['DELETE'])
@jwt_required()
def delete_climate(id_climate):
    success, error = climate_service.delete_climate(id_climate)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": {"deleted": success}}), 200

@climate_bp.route('/climate-monthly/delete-many', methods=['POST'])
@jwt_required()
def delete_many_climate():
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
    
    result, error = climate_service.delete_many_climate(ids)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": result}), 200

@climate_bp.route('/climate-monthly/province/<string:kd_prov>', methods=['GET'])
@jwt_required()
def get_climate_by_province(kd_prov):
    data, error = climate_service.get_by_province(kd_prov)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/city/<string:kd_kab>', methods=['GET'])
@jwt_required()
def get_climate_by_city(kd_kab):
    data, error = climate_service.get_by_city(kd_kab)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/district/<string:kd_kec>', methods=['GET'])
@jwt_required()
def get_climate_by_district(kd_kec):
    data, error = climate_service.get_by_district(kd_kec)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/year/<int:tahun>', methods=['GET'])
@jwt_required()
def get_climate_by_year(tahun):
    if tahun < 1900 or tahun > 2100:
        return jsonify({"success": False, "error": "Invalid year"}), 400
    
    data, error = climate_service.get_by_year(tahun)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/month-year/<int:bulan>/<int:tahun>', methods=['GET'])
@jwt_required()
def get_climate_by_month_year(bulan, tahun):
    if bulan < 1 or bulan > 12:
        return jsonify({"success": False, "error": "Month must be between 1 and 12"}), 400
    if tahun < 1900 or tahun > 2100:
        return jsonify({"success": False, "error": "Invalid year"}), 400
    
    data, error = climate_service.get_by_month_year(bulan, tahun)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/status/<string:status>', methods=['GET'])
@jwt_required()
def get_climate_by_status(status):
    data, error = climate_service.get_by_status(status)
    if error:
        return jsonify({"success": False, "error": error}), 400
    return jsonify({"success": True, "data": data}), 200

@climate_bp.route('/climate-monthly/stats/summary', methods=['GET'])
@jwt_required()
def get_climate_summary():
    """Get summary statistics for Climate data"""
    try:
        kd_prov = request.args.get('kd_prov')
        kd_kab = request.args.get('kd_kab')
        kd_kec = request.args.get('kd_kec')
        tahun = request.args.get('tahun', type=int)
        
        if tahun and (tahun < 1900 or tahun > 2100):
            return jsonify({"success": False, "error": "Invalid year"}), 400
        
        summary, error = climate_service.get_summary_stats(kd_prov, kd_kab, kd_kec, tahun)
        if error:
            return jsonify({"success": False, "error": error}), 400
        return jsonify({"success": True, "data": summary}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@climate_bp.route('/climate-monthly/analyze/correlation', methods=['GET'])
@jwt_required()
def analyze_climate_correlation():
    """Analyze correlation between climate variables"""
    try:
        kd_prov = request.args.get('kd_prov')
        kd_kab = request.args.get('kd_kab')
        tahun = request.args.get('tahun', type=int)
        
        correlation, error = climate_service.analyze_correlation(kd_prov, kd_kab, tahun)
        if error:
            return jsonify({"success": False, "error": error}), 400
        return jsonify({"success": True, "data": correlation}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@climate_bp.route('/climate-monthly/analyze/trends', methods=['GET'])
@jwt_required()
def analyze_climate_trends():
    """Analyze climate trends over time"""
    try:
        kd_prov = request.args.get('kd_prov')
        kd_kab = request.args.get('kd_kab')
        kd_kec = request.args.get('kd_kec')
        start_year = request.args.get('start_year', type=int)
        end_year = request.args.get('end_year', type=int)
        
        trends, error = climate_service.analyze_trends(kd_prov, kd_kab, kd_kec, start_year, end_year)
        if error:
            return jsonify({"success": False, "error": error}), 400
        return jsonify({"success": True, "data": trends}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@climate_bp.route('/climate-monthly/compare/locations', methods=['POST'])
@jwt_required()
def compare_climate_locations():
    """Compare climate data between multiple locations"""
    try:
        data = request.get_json()
        if not data or 'locations' not in data:
            return jsonify({"success": False, "error": "Locations list required"}), 400
        
        locations = data['locations']
        tahun = data.get('tahun')
        variables = data.get('variables', ['tm_tm_mean', 'rh_rh_mean', 'hujan_hujan_mean'])
        
        comparison, error = climate_service.compare_locations(locations, tahun, variables)
        if error:
            return jsonify({"success": False, "error": error}), 400
        return jsonify({"success": True, "data": comparison}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@climate_bp.route('/climate-monthly/paginated', methods=['GET'])
@jwt_required()
def get_paginated_climate():
    # Get pagination parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    sort_by = request.args.get('sort_by', 'id_climate')
    sort_order = request.args.get('sort_order', 'desc')
    
    # Validate pagination parameters
    if page < 1:
        return jsonify({"success": False, "error": "Page must be greater than 0"}), 400
    if per_page < 1 or per_page > 100:
        return jsonify({"success": False, "error": "Per page must be between 1 and 100"}), 400
    
    # Get filter parameters
    filter_params = {}
    allowed_filters = [
        'kd_prov', 'kd_kab', 'kd_kec', 'tahun', 'bulan', 'status', 'year_range',
        'temp_min', 'temp_max', 'humidity_min', 'humidity_max', 'rainfall_min', 'rainfall_max'
    ]
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
    
    # Validate range filters
    range_filters = ['temp_min', 'temp_max', 'humidity_min', 'humidity_max', 'rainfall_min', 'rainfall_max']
    for filter_key in range_filters:
        if filter_key in filter_params:
            try:
                float(filter_params[filter_key])
            except ValueError:
                return jsonify({"success": False, "error": f"{filter_key} must be a valid number"}), 400
    
    data, meta, error = climate_service.get_paginated(
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

@climate_bp.route('/climate-monthly/export', methods=['GET'])
@jwt_required()
def export_climate():
    """Export Climate data to CSV"""
    try:
        # Get filter parameters for export
        filter_params = {}
        allowed_filters = ['kd_prov', 'kd_kab', 'kd_kec', 'tahun', 'bulan', 'status']
        for key in request.args:
            if key in allowed_filters:
                filter_params[key] = request.args.get(key)
        
        # Get format (csv, json, excel)
        export_format = request.args.get('format', 'csv').lower()
        if export_format not in ['csv', 'json', 'excel']:
            return jsonify({"success": False, "error": "Format must be csv, json, or excel"}), 400
        
        exported_data, error = climate_service.export_data(filter_params, export_format)
        if error:
            return jsonify({"success": False, "error": error}), 400
        
        file_extension = export_format if export_format != 'excel' else 'xlsx'
        
        return jsonify({
            "success": True,
            "data": {
                "content": exported_data,
                "filename": f"climate_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.{file_extension}",
                "format": export_format
            }
        }), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@climate_bp.route('/climate-monthly/bulk-import', methods=['POST'])
@jwt_required()
def bulk_import_climate():
    """Bulk import climate data from CSV or Excel"""
    try:
        if 'file' not in request.files:
            return jsonify({"success": False, "error": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"success": False, "error": "No file selected"}), 400
        
        allowed_extensions = ['.csv', '.xlsx', '.xls']
        if not any(file.filename.lower().endswith(ext) for ext in allowed_extensions):
            return jsonify({"success": False, "error": "File must be CSV or Excel format"}), 400
        
        # Additional import options
        skip_duplicates = request.form.get('skip_duplicates', 'true').lower() == 'true'
        validate_data = request.form.get('validate_data', 'true').lower() == 'true'
        
        current_user = get_jwt_identity()
        created_by = current_user.get('id') if isinstance(current_user, dict) else current_user
        
        result, error = climate_service.bulk_import_file(
            file, 
            created_by, 
            skip_duplicates=skip_duplicates,
            validate_data=validate_data
        )
        if error:
            return jsonify({"success": False, "error": error}), 400
        
        return jsonify({"success": True, "data": result}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400

@climate_bp.route('/climate-monthly/validate', methods=['POST'])
@jwt_required()
def validate_climate_data():
    """Validate climate data before import"""
    try:
        data = request.get_json()
        if not data or 'records' not in data:
            return jsonify({"success": False, "error": "Records list required"}), 400
        
        validation_result, error = climate_service.validate_bulk_data(data['records'])
        if error:
            return jsonify({"success": False, "error": error}), 400
        
        return jsonify({"success": True, "data": validation_result}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 400