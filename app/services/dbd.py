from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import DBD
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
from app.services.base import BaseCRUDService
class DBDService(BaseCRUDService):
    """
    Service for CRUD operations on DBD model.
    """
    def __init__(self):
        super().__init__(DBD)
    
    def get_by_id(self, id_dbd):
        return super().get_by_id('id_dbd', id_dbd)
    
    def create_dbd(self, data):
        return self.create(data)
    
    def update_dbd(self, id_dbd, data):
        return self.update('id_dbd', id_dbd, data)
    
    def delete_dbd(self, id_dbd):
        return self.delete('id_dbd', id_dbd)
    
    def delete_many_dbd(self, id_dbd_list):
        return self.delete_many('id_dbd', id_dbd_list)
    
    def get_by_province(self, kd_prov):
        """Get DBD records by province code."""
        try:
            records = DBD.query.filter_by(kd_prov=kd_prov).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_district(self, kd_kab):
        """Get DBD records by district code."""
        try:
            records = DBD.query.filter_by(kd_kab=kd_kab).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_year(self, tahun):
        """Get DBD records by year."""
        try:
            records = DBD.query.filter_by(tahun=tahun).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_month_year(self, bulan, tahun):
        """Get DBD records by month and year."""
        try:
            records = DBD.query.filter_by(bulan=bulan, tahun=tahun).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_status(self, status):
        """Get DBD records by status."""
        try:
            records = DBD.query.filter_by(status=status).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def _build_filter_conditions(self, filters):
        """Build filter conditions for pagination."""
        filter_conditions = []
        
        if 'kd_prov' in filters and filters['kd_prov']:
            filter_conditions.append(DBD.kd_prov == filters['kd_prov'])
        
        if 'kd_kab' in filters and filters['kd_kab']:
            filter_conditions.append(DBD.kd_kab == filters['kd_kab'])
        
        if 'tahun' in filters and filters['tahun']:
            filter_conditions.append(DBD.tahun == int(filters['tahun']))
        
        if 'bulan' in filters and filters['bulan']:
            filter_conditions.append(DBD.bulan == int(filters['bulan']))
        
        if 'status' in filters and filters['status']:
            filter_conditions.append(DBD.status == filters['status'])
        
        if 'year_range' in filters and filters['year_range']:
            year_start, year_end = filters['year_range'].split('-')
            filter_conditions.append(DBD.tahun >= int(year_start))
            filter_conditions.append(DBD.tahun <= int(year_end))
        
        return filter_conditions
    
    def _get_distinct_values(self):
        """Get distinct values for dropdowns."""
        distinct_provinces = db.session.query(DBD.kd_prov).distinct().all()
        distinct_districts = db.session.query(DBD.kd_kab).distinct().all()
        distinct_years = db.session.query(DBD.tahun).distinct().all()
        distinct_months = db.session.query(DBD.bulan).distinct().all()
        distinct_statuses = db.session.query(DBD.status).distinct().all()
        
        return {
            "provinces": [p[0] for p in distinct_provinces if p[0]],
            "districts": [d[0] for d in distinct_districts if d[0]],
            "years": sorted([y[0] for y in distinct_years if y[0]]),
            "months": sorted([m[0] for m in distinct_months if m[0]]),
            "statuses": [s[0] for s in distinct_statuses if s[0]]
        }