from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import User
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime

from app.models.db_models import SocioEnvironmentalFactorsMonthly
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
        finally:
            db.session.close()

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
        
        finally:
            db.session.close()

    def filter_by(self, **kwargs):
        """Filter records based on provided criteria."""
        try:
            records = self.model.query.filter_by(**kwargs).all()
            return [record.to_dict() if hasattr(record, 'to_dict') else record for record in records], None
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
    def get_paginated(self, page=1, per_page=10, sort_by='id', sort_order='asc', filters=None):
        try:
            # Start with base query
            query = User.query
            
            # Apply filters if provided
            filter_conditions = []
            if filters:
                # Filter by name
                if 'name' in filters and filters['name']:
                    filter_conditions.append(User.name.ilike(f"%{filters['name']}%"))
                
                # Filter by email
                if 'email' in filters and filters['email']:
                    filter_conditions.append(User.email.ilike(f"%{filters['email']}%"))
                
                # Filter by access level
                if 'access_level' in filters and filters['access_level']:
                    filter_conditions.append(User.access_level == filters['access_level'])
                
                # Apply all filters if any
                if filter_conditions:
                    query = query.filter(and_(*filter_conditions))
            
            # Apply sorting
            if hasattr(User, sort_by):
                order_column = getattr(User, sort_by)
            else:
                # Default to id if invalid sort column
                order_column = User.id
            
            if sort_order and sort_order.lower() == 'desc':
                query = query.order_by(desc(order_column))
            else:
                query = query.order_by(asc(order_column))
            
            # Apply pagination
            paginated_records = query.paginate(page=page, per_page=per_page, error_out=False)
            
            # Prepare result
            records = paginated_records.items
            result = [record.to_dict() for record in records]
            # Prepare distinct values for dropdowns
            distinct_access_levels = db.session.query(User.access_level).distinct().all()
            
            # Prepare metadata
            meta = {
                "page": page,
                "per_page": per_page,
                "total_pages": paginated_records.pages,
                "total_records": paginated_records.total,
                "has_next": paginated_records.has_next,
                "has_prev": paginated_records.has_prev,
                "filters": filters or {},
                "distinct_values": {
                    "access_levels": [a[0] for a in distinct_access_levels if a[0]]
                }
            }
            
            return result, meta, None
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, None, str(e)
class SocioEnvironmentalFactorsMonthlyService(BaseCRUDService):
    """
    Service for CRUD operations on SocioEnvironmentalFactorsMonthly model.
    """
    def __init__(self):
        super().__init__(SocioEnvironmentalFactorsMonthly)
    
    # Get by primary key
    def get_by_id_env_factor(self, id_env_factor):
        return self.get_by_id('id_env_factor', id_env_factor)
    
    # Create a new record
    def create_record(self, data):
        return self.create(data)
    
    # Update an existing record
    def update_record(self, id_env_factor, data):
        return self.update('id_env_factor', id_env_factor, data)
    
    # Delete a record
    def delete_record(self, id_env_factor):
        return self.delete('id_env_factor', id_env_factor)
    
    # Additional methods specific to SocioEnvironmentalFactorsMonthly
    def get_by_facility_and_period(self, id_faskes, bulan, tahun):
        """Get environmental record by facility and period (month, year)."""
        try:
            record = SocioEnvironmentalFactorsMonthly.query.filter_by(
                id_faskes=id_faskes, 
                bulan=bulan, 
                tahun=tahun
            ).first()
            
            if not record:
                return None, f"Environmental record for facility ID {id_faskes}, period {bulan}/{tahun} not found"
            
            return record.to_dict(), None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_annual_data(self, id_faskes, tahun):
        """Get all environmental records for a facility in a specific year."""
        try:
            records = SocioEnvironmentalFactorsMonthly.query.filter_by(
                id_faskes=id_faskes, 
                tahun=tahun
            ).all()
            
            return [record.to_dict() for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)
    
    def get_paginated(self, page=1, per_page=10, month=None, year=None, sort_by='id_env_factor', sort_order='asc', filters=None):
        try:
            # Start with base query that joins with facility table to get location data
            query = db.session.query(SocioEnvironmentalFactorsMonthly).join(
                HealthFacilityId, 
                SocioEnvironmentalFactorsMonthly.id_faskes == HealthFacilityId.id_faskes
            )
            
            # Get default values for month and year if not provided
            if month is None or year is None:
                # Find the latest record
                latest_record = SocioEnvironmentalFactorsMonthly.query.order_by(
                    desc(SocioEnvironmentalFactorsMonthly.tahun),
                    desc(SocioEnvironmentalFactorsMonthly.bulan)
                ).first()
                
                if latest_record:
                    month = month or latest_record.bulan
                    year = year or latest_record.tahun
                else:
                    # If no records at all, default to current month and year
                    current_date = datetime.now()
                    month = month or current_date.month
                    year = year or current_date.year
            
            # Apply all filters
            filter_conditions = [
                SocioEnvironmentalFactorsMonthly.bulan == month,
                SocioEnvironmentalFactorsMonthly.tahun == year
            ]
            
            # Apply additional filters from filter_params
            if filters:
                # Filter by health facility ID
                if 'id_faskes' in filters and filters['id_faskes']:
                    filter_conditions.append(SocioEnvironmentalFactorsMonthly.id_faskes == int(filters['id_faskes']))
                
                # Filter by location (province, district, subdistrict)
                if 'provinsi' in filters and filters['provinsi']:
                    filter_conditions.append(HealthFacilityId.provinsi == filters['provinsi'])
                
                if 'kabupaten' in filters and filters['kabupaten']:
                    filter_conditions.append(HealthFacilityId.kabupaten == filters['kabupaten'])
                
                if 'kecamatan' in filters and filters['kecamatan']:
                    filter_conditions.append(HealthFacilityId.kecamatan == filters['kecamatan'])
                
                # Filter by facility type
                if 'tipe_faskes' in filters and filters['tipe_faskes']:
                    filter_conditions.append(HealthFacilityId.tipe_faskes == filters['tipe_faskes'])
            
            # Apply all filter conditions
            query = query.filter(and_(*filter_conditions))
            
            # Apply sorting
            if hasattr(SocioEnvironmentalFactorsMonthly, sort_by):
                order_column = getattr(SocioEnvironmentalFactorsMonthly, sort_by)
            else:
                # Default to id_env_factor if invalid sort column
                order_column = SocioEnvironmentalFactorsMonthly.id_env_factor
            
            if sort_order and sort_order.lower() == 'desc':
                query = query.order_by(desc(order_column))
            else:
                query = query.order_by(asc(order_column))
            
            # Apply pagination
            paginated_records = query.paginate(page=page, per_page=per_page, error_out=False)
            
            # Prepare result
            records = paginated_records.items
            result = [record.to_dict() for record in records]
            
            # Prepare metadata
            meta = {
                "page": page,
                "per_page": per_page,
                "total_pages": paginated_records.pages,
                "total_records": paginated_records.total,
                "month": month,
                "year": year,
                "has_next": paginated_records.has_next,
                "has_prev": paginated_records.has_prev,
                "filters": filters or {}
            }
            
            return result, meta, None
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, None, str(e)
    
    def create_or_update_from_malaria_data(self, malaria_data):
        """
        Create or update an environmental record from malaria data during migration.
        
        This method is specifically for the data migration process.
        It extracts environmental data from a malaria record and saves it
        to the environmental_factors_monthly table.
        
        Args:
            malaria_data: Dictionary containing malaria data with environmental fields
            
        Returns:
            Tuple of (record, error)
        """
        try:
            # Extract environmental fields
            env_data = {
                'id_faskes': malaria_data['id_faskes'],
                'bulan': malaria_data['bulan'],
                'tahun': malaria_data['tahun'],
                'hujan_hujan_mean': malaria_data.get('hujan_hujan_mean'),
                'hujan_hujan_max': malaria_data.get('hujan_hujan_max'),
                'hujan_hujan_min': malaria_data.get('hujan_hujan_min'),
                'tm_tm_mean': malaria_data.get('tm_tm_mean'),
                'tm_tm_max': malaria_data.get('tm_tm_max'),
                'tm_tm_min': malaria_data.get('tm_tm_min'),
                'rh_mean': malaria_data.get('rh_mean'),
                'rh_min': malaria_data.get('rh_min'),
                'rh_max': malaria_data.get('rh_max'),
                'ss_monthly_mean': malaria_data.get('ss_monthly_mean'),
                'ff_x_monthly_mean': malaria_data.get('ff_x_monthly_mean'),
                'ddd_x_monthly_mean': malaria_data.get('ddd_x_monthly_mean'),
                'ff_avg_monthly_mean': malaria_data.get('ff_avg_monthly_mean'),
                'pop_penduduk_kab': malaria_data.get('pop_penduduk_kab')
            }
            
            # Check if a record already exists for this facility and period
            existing_record = SocioEnvironmentalFactorsMonthly.query.filter_by(
                id_faskes=env_data['id_faskes'],
                bulan=env_data['bulan'],
                tahun=env_data['tahun']
            ).first()
            
            if existing_record:
                # Update existing record
                for key, value in env_data.items():
                    if hasattr(existing_record, key):
                        setattr(existing_record, key, value)
                
                db.session.commit()
                return existing_record.to_dict(), None
            else:
                # Create new record
                new_record = SocioEnvironmentalFactorsMonthly(**env_data)
                db.session.add(new_record)
                db.session.commit()
                return new_record.to_dict(), None
                
        except SQLAlchemyError as e:
            db.session.rollback()
            return None, str(e)
        finally:
            db.session.close()