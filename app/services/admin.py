from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import MalariaHealthFacilityMonthly, HealthFacilityId, User

class BaseCRUDService:
    """
    Base class for CRUD operations providing common functionality.
    """
    def __init__(self, model):
        self.model = model

    def get_all(self):
        """Retrieve all records from the model."""
        try:
            records = self.model.query.all()
            print(records)
            return [record.to_dict() if hasattr(record, 'to_dict') else record for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)

    def get_by_id(self, id_field, id_value):
        """Retrieve a record by its ID."""
        try:
            record = self.model.query.filter_by(**{id_field: id_value}).first()
            if not record:
                return None, f"{self.model.__name__} with ID {id_value} not found"
            return record.to_dict() if hasattr(record, 'to_dict') else record, None
        except SQLAlchemyError as e:
            return None, str(e)

    def create(self, data):
        """Create a new record."""
        try:
            new_record = self.model(**data)
            db.session.add(new_record)
            db.session.commit()
            return new_record.to_dict() if hasattr(new_record, 'to_dict') else new_record, None
        except SQLAlchemyError as e:
            db.session.rollback()
            return None, str(e)

    def update(self, id_field, id_value, data):
        """Update an existing record."""
        try:
            record = self.model.query.filter_by(**{id_field: id_value}).first()
            if not record:
                return None, f"{self.model.__name__} with ID {id_value} not found"
            
            for key, value in data.items():
                if hasattr(record, key):
                    setattr(record, key, value)
            
            db.session.commit()
            return record.to_dict() if hasattr(record, 'to_dict') else record, None
        except SQLAlchemyError as e:
            db.session.rollback()
            return None, str(e)

    def delete(self, id_field, id_value):
        """Delete a record."""
        try:
            record = self.model.query.filter_by(**{id_field: id_value}).first()
            if not record:
                return False, f"{self.model.__name__} with ID {id_value} not found"
            
            db.session.delete(record)
            db.session.commit()
            return True, None
        except SQLAlchemyError as e:
            db.session.rollback()
            return False, str(e)

    def filter_by(self, **kwargs):
        """Filter records based on provided criteria."""
        try:
            records = self.model.query.filter_by(**kwargs).all()
            return [record.to_dict() if hasattr(record, 'to_dict') else record for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)


class MalariaHealthFacilityMonthlyService(BaseCRUDService):
    """
    Service for CRUD operations on MalariaHealthFacilityMonthly model.
    """
    def __init__(self):
        super().__init__(MalariaHealthFacilityMonthly)
    
    # Get by primary key
    def get_by_id_mhfm(self, id_mhfm):
        return self.get_by_id('id_mhfm', id_mhfm)
    
    # Create a new record
    def create_record(self, data):
        return self.create(data)
    
    # Update an existing record
    def update_record(self, id_mhfm, data):
        return self.update('id_mhfm', id_mhfm, data)
    
    # Delete a record
    def delete_record(self, id_mhfm):
        return self.delete('id_mhfm', id_mhfm)
    
    # Additional methods specific to MalariaHealthFacilityMonthly
    def get_by_facility_and_period(self, id_faskes, bulan, tahun):
        """Get record by facility and period (month, year)."""
        try:
            record = MalariaHealthFacilityMonthly.query.filter_by(
                id_faskes=id_faskes, 
                bulan=bulan, 
                tahun=tahun
            ).first()
            
            if not record:
                return None, f"Record for facility ID {id_faskes}, period {bulan}/{tahun} not found"
            
            return record.to_dict(), None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_annual_data(self, id_faskes, tahun):
        """Get all records for a facility in a specific year."""
        try:
            records = MalariaHealthFacilityMonthly.query.filter_by(
                id_faskes=id_faskes, 
                tahun=tahun
            ).all()
            
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)


class HealthFacilityIdService(BaseCRUDService):
    """
    Service for CRUD operations on HealthFacilityId model.
    """
    def __init__(self):
        super().__init__(HealthFacilityId)
    
    # Get by primary key
    def get_by_id_faskes(self, id_faskes):
        return self.get_by_id('id_faskes', id_faskes)
    
    # Create a new record
    def create_record(self, data):
        return self.create(data)
    
    # Update an existing record
    def update_record(self, id_faskes, data):
        return self.update('id_faskes', id_faskes, data)
    
    # Delete a record
    def delete_record(self, id_faskes):
        return self.delete('id_faskes', id_faskes)
    
    # Additional methods specific to HealthFacilityId
    def get_by_location(self, provinsi=None, kabupaten=None, kecamatan=None):
        """Get facilities filtered by location."""
        try:
            query = HealthFacilityId.query
            
            if provinsi:
                query = query.filter_by(provinsi=provinsi)
            if kabupaten:
                query = query.filter_by(kabupaten=kabupaten)
            if kecamatan:
                query = query.filter_by(kecamatan=kecamatan)
                
            records = query.all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_facility_type(self, tipe_faskes):
        """Get facilities by type."""
        try:
            records = HealthFacilityId.query.filter_by(tipe_faskes=tipe_faskes).all()
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)


class UserService(BaseCRUDService):
    """
    Service for CRUD operations on User model.
    """
    def __init__(self):
        super().__init__(User)
    
    # Get by primary key
    def get_by_id(self, user_id):
        return super().get_by_id('id', user_id)
    
    # Create a new record
    def create_user(self, data):
        return self.create(data)
    
    # Update an existing record
    def update_user(self, user_id, data):
        return self.update('id', user_id, data)
    
    # Delete a record
    def delete_user(self, user_id):
        return self.delete('id', user_id)
    
    # Additional methods specific to User
    def get_by_email(self, email):
        """Get user by email."""
        try:
            user = User.query.filter_by(email=email).first()
            if not user:
                return None, f"User with email {email} not found"
            return user.to_dict(), None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_by_access_level(self, access_level):
        """Get users by access level."""
        try:
            users = User.query.filter_by(access_level=access_level).all()
            return [user.to_dict() for user in users], None
        except SQLAlchemyError as e:
            return None, str(e)