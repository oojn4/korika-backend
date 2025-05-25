from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import MasterKab 
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
from app.services.base import BaseCRUDService
class MasterKabService(BaseCRUDService):
    """
    Service for CRUD operations on MasterKab model.
    """
    def __init__(self):
        super().__init__(MasterKab)
    
    def get_by_id(self, kd_kab):
        return super().get_by_id('kd_kab', kd_kab)
    
    def create_city(self, data):
        return self.create(data)
    
    def update_city(self, kd_kab, data):
        return self.update('kd_kab', kd_kab, data)
    
    def delete_city(self, kd_kab):
        return self.delete('kd_kab', kd_kab)
    
    def delete_many_cities(self, kd_kab_list):
        return self.delete_many('kd_kab', kd_kab_list)
    
    def get_by_province(self, kd_prov):
        """Get districts by province code."""
        try:
            districts = MasterKab.query.filter_by(kd_prov=kd_prov).all()
            return [district.to_dict() for district in districts], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_bmkg_code(self, kd_bmkg):
        """Get district by BMKG code."""
        try:
            district = MasterKab.query.filter_by(kd_bmkg=kd_bmkg).first()
            if not district:
                return None, f"District with BMKG code {kd_bmkg} not found"
            return district.to_dict(), None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_endemic_status(self, status_endemis):
        """Get districts by endemic status."""
        try:
            districts = MasterKab.query.filter_by(status_endemis=status_endemis).all()
            return [district.to_dict() for district in districts], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def _build_filter_conditions(self, filters):
        """Build filter conditions for pagination."""
        filter_conditions = []
        
        if 'kabkot' in filters and filters['kabkot']:
            filter_conditions.append(MasterKab.kabkot.ilike(f"%{filters['kabkot']}%"))
        
        if 'kd_kab' in filters and filters['kd_kab']:
            filter_conditions.append(MasterKab.kd_kab.ilike(f"%{filters['kd_kab']}%"))
        
        if 'kd_prov' in filters and filters['kd_prov']:
            filter_conditions.append(MasterKab.kd_prov == filters['kd_prov'])
        
        if 'status_endemis' in filters and filters['status_endemis']:
            filter_conditions.append(MasterKab.status_endemis == filters['status_endemis'])
        
        if 'kd_bmkg' in filters and filters['kd_bmkg']:
            filter_conditions.append(MasterKab.kd_bmkg.ilike(f"%{filters['kd_bmkg']}%"))
        
        return filter_conditions
    
    def _get_distinct_values(self):
        """Get distinct values for dropdowns."""
        distinct_provinces = db.session.query(MasterKab.kd_prov).distinct().all()
        distinct_status = db.session.query(MasterKab.status_endemis).distinct().all()
        
        return {
            "provinces": [p[0] for p in distinct_provinces if p[0]],
            "endemic_statuses": [s[0] for s in distinct_status if s[0]]
        }