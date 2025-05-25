from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import MasterKec
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
from app.services.base import BaseCRUDService
class MasterKecService(BaseCRUDService):
    """
    Service for CRUD operations on MasterKec model.
    """
    def __init__(self):
        super().__init__(MasterKec)
    
    def get_by_id(self, kd_kec):
        return super().get_by_id('kd_kec', kd_kec)
    
    def create_district(self, data):
        return self.create(data)
    
    def update_district(self, kd_kec, data):
        return self.update('kd_kec', kd_kec, data)
    
    def delete_district(self, kd_kec):
        return self.delete('kd_kec', kd_kec)
    
    def delete_many_districts(self, kd_kec_list):
        return self.delete_many('kd_kec', kd_kec_list)
    
    def get_by_district(self, kd_kab):
        """Get subdistricts by district code."""
        try:
            subdistricts = MasterKec.query.filter_by(kd_kab=kd_kab).all()
            return [subdistrict.to_dict() for subdistrict in subdistricts], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_province(self, kd_prov):
        """Get subdistricts by province code."""
        try:
            subdistricts = MasterKec.query.filter_by(kd_prov=kd_prov).all()
            return [subdistrict.to_dict() for subdistrict in subdistricts], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_bmkg_code(self, kd_bmkg):
        """Get subdistrict by BMKG code."""
        try:
            subdistrict = MasterKec.query.filter_by(kd_bmkg=kd_bmkg).first()
            if not subdistrict:
                return None, f"Subdistrict with BMKG code {kd_bmkg} not found"
            return subdistrict.to_dict(), None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def _build_filter_conditions(self, filters):
        """Build filter conditions for pagination."""
        filter_conditions = []
        
        if 'kecamatan' in filters and filters['kecamatan']:
            filter_conditions.append(MasterKec.kecamatan.ilike(f"%{filters['kecamatan']}%"))
        
        if 'kd_kec' in filters and filters['kd_kec']:
            filter_conditions.append(MasterKec.kd_kec.ilike(f"%{filters['kd_kec']}%"))
        
        if 'kd_kab' in filters and filters['kd_kab']:
            filter_conditions.append(MasterKec.kd_kab == filters['kd_kab'])
        
        if 'kd_prov' in filters and filters['kd_prov']:
            filter_conditions.append(MasterKec.kd_prov == filters['kd_prov'])
        
        if 'kd_bmkg' in filters and filters['kd_bmkg']:
            filter_conditions.append(MasterKec.kd_bmkg.ilike(f"%{filters['kd_bmkg']}%"))
        
        return filter_conditions
    
    def _get_distinct_values(self):
        """Get distinct values for dropdowns."""
        distinct_provinces = db.session.query(MasterKec.kd_prov).distinct().all()
        distinct_districts = db.session.query(MasterKec.kd_kab).distinct().all()
        
        return {
            "provinces": [p[0] for p in distinct_provinces if p[0]],
            "districts": [d[0] for d in distinct_districts if d[0]]
        }