from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import MasterProv
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
from app.services.base import BaseCRUDService

class MasterProvService(BaseCRUDService):
    """
    Service for CRUD operations on MasterProv model.
    """
    def __init__(self):
        super().__init__(MasterProv)
    
    def get_by_id(self, kd_prov):
        return super().get_by_id('kd_prov', kd_prov)
    
    def create_province(self, data):
        return self.create(data)
    
    def update_province(self, kd_prov, data):
        return self.update('kd_prov', kd_prov, data)
    
    def delete_province(self, kd_prov):
        return self.delete('kd_prov', kd_prov)
    
    def delete_many_provinces(self, kd_prov_list):
        return self.delete_many('kd_prov', kd_prov_list)
    
    def get_by_bmkg_code(self, kd_bmkg):
        """Get province by BMKG code."""
        try:
            province = MasterProv.query.filter_by(kd_bmkg=kd_bmkg).first()
            if not province:
                return None, f"Province with BMKG code {kd_bmkg} not found"
            return province.to_dict(), None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def _build_filter_conditions(self, filters):
        """Build filter conditions for pagination."""
        filter_conditions = []
        
        if 'provinsi' in filters and filters['provinsi']:
            filter_conditions.append(MasterProv.provinsi.ilike(f"%{filters['provinsi']}%"))
        
        if 'kd_prov' in filters and filters['kd_prov']:
            filter_conditions.append(MasterProv.kd_prov.ilike(f"%{filters['kd_prov']}%"))
        
        if 'kd_bmkg' in filters and filters['kd_bmkg']:
            filter_conditions.append(MasterProv.kd_bmkg.ilike(f"%{filters['kd_bmkg']}%"))
        
        return filter_conditions
    
    def _get_distinct_values(self):
        """Get distinct values for dropdowns."""
        return {}