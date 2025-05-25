from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import ClimateMonthly
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
from app.services.base import BaseCRUDService
class ClimateMonthlyService(BaseCRUDService):
    """
    Service for CRUD operations on ClimateMonthly model.
    """
    def __init__(self):
        super().__init__(ClimateMonthly)
    
    def get_by_id(self, id_climate):
        return super().get_by_id('id_climate', id_climate)
    
    def create_climate(self, data):
        return self.create(data)
    
    def update_climate(self, id_climate, data):
        return self.update('id_climate', id_climate, data)
    
    def delete_climate(self, id_climate):
        return self.delete('id_climate', id_climate)
    
    def delete_many_climate(self, id_climate_list):
        return self.delete_many('id_climate', id_climate_list)
    
    def get_by_province(self, kd_prov):
        """Get Climate records by province code."""
        try:
            records = ClimateMonthly.query.filter_by(kd_prov=kd_prov).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_district(self, kd_kab):
        """Get Climate records by district code."""
        try:
            records = ClimateMonthly.query.filter_by(kd_kab=kd_kab).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_subdistrict(self, kd_kec):
        """Get Climate records by subdistrict code."""
        try:
            records = ClimateMonthly.query.filter_by(kd_kec=kd_kec).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_year(self, tahun):
        """Get Climate records by year."""
        try:
            records = ClimateMonthly.query.filter_by(tahun=tahun).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_month_year(self, bulan, tahun):
        """Get Climate records by month and year."""
        try:
            records = ClimateMonthly.query.filter_by(bulan=bulan, tahun=tahun).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_status(self, status):
        """Get Climate records by status."""
        try:
            records = ClimateMonthly.query.filter_by(status=status).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def _build_filter_conditions(self, filters):
        """Build filter conditions for pagination."""
        filter_conditions = []
        
        if 'kd_prov' in filters and filters['kd_prov']:
            filter_conditions.append(ClimateMonthly.kd_prov == filters['kd_prov'])
        
        if 'kd_kab' in filters and filters['kd_kab']:
            filter_conditions.append(ClimateMonthly.kd_kab == filters['kd_kab'])
        
        if 'kd_kec' in filters and filters['kd_kec']:
            filter_conditions.append(ClimateMonthly.kd_kec == filters['kd_kec'])
        
        if 'tahun' in filters and filters['tahun']:
            filter_conditions.append(ClimateMonthly.tahun == int(filters['tahun']))
        
        if 'bulan' in filters and filters['bulan']:
            filter_conditions.append(ClimateMonthly.bulan == int(filters['bulan']))
        
        if 'status' in filters and filters['status']:
            filter_conditions.append(ClimateMonthly.status == filters['status'])
        
        if 'year_range' in filters and filters['year_range']:
            year_start, year_end = filters['year_range'].split('-')
            filter_conditions.append(ClimateMonthly.tahun >= int(year_start))
            filter_conditions.append(ClimateMonthly.tahun <= int(year_end))
        
        # Temperature range filters
        if 'temp_min' in filters and filters['temp_min']:
            filter_conditions.append(ClimateMonthly.tm_tm_mean >= float(filters['temp_min']))
        
        if 'temp_max' in filters and filters['temp_max']:
            filter_conditions.append(ClimateMonthly.tm_tm_mean <= float(filters['temp_max']))
        
        # Humidity range filters
        if 'humidity_min' in filters and filters['humidity_min']:
            filter_conditions.append(ClimateMonthly.rh_rh_mean >= float(filters['humidity_min']))
        
        if 'humidity_max' in filters and filters['humidity_max']:
            filter_conditions.append(ClimateMonthly.rh_rh_mean <= float(filters['humidity_max']))
        
        # Rainfall range filters
        if 'rainfall_min' in filters and filters['rainfall_min']:
            filter_conditions.append(ClimateMonthly.hujan_hujan_mean >= float(filters['rainfall_min']))
        
        if 'rainfall_max' in filters and filters['rainfall_max']:
            filter_conditions.append(ClimateMonthly.hujan_hujan_mean <= float(filters['rainfall_max']))
        
        return filter_conditions
    
    def _get_distinct_values(self):
        """Get distinct values for dropdowns."""
        distinct_provinces = db.session.query(ClimateMonthly.kd_prov).distinct().all()
        distinct_districts = db.session.query(ClimateMonthly.kd_kab).distinct().all()
        distinct_subdistricts = db.session.query(ClimateMonthly.kd_kec).distinct().all()
        distinct_years = db.session.query(ClimateMonthly.tahun).distinct().all()
        distinct_months = db.session.query(ClimateMonthly.bulan).distinct().all()
        distinct_statuses = db.session.query(ClimateMonthly.status).distinct().all()
        
        return {
            "provinces": [p[0] for p in distinct_provinces if p[0]],
            "districts": [d[0] for d in distinct_districts if d[0]],
            "subdistricts": [s[0] for s in distinct_subdistricts if s[0]],
            "years": sorted([y[0] for y in distinct_years if y[0]]),
            "months": sorted([m[0] for m in distinct_months if m[0]]),
            "statuses": [s[0] for s in distinct_statuses if s[0]]
        }