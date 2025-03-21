from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import MalariaHealthFacilityMonthly, HealthFacilityId, User
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
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
    def get_paginated(self, page=1, per_page=10, month=None, year=None, status=None, sort_by='id_mhfm', sort_order='asc', filters=None):
        try:
            # Start with base query that joins with facility table to get location data
            query = db.session.query(MalariaHealthFacilityMonthly).join(
                HealthFacilityId, 
                MalariaHealthFacilityMonthly.id_faskes == HealthFacilityId.id_faskes
            )
            
            # Determine status value (default to 'actual' if not provided)
            if status is None:
                if filters and 'status' in filters and filters['status']:
                    status = filters['status']
                else:
                    status = 'actual'  # Default status
            
            # Get default values for month and year if not provided,
            # specifically matching the requested status
            if month is None or year is None:
                # Find the latest record with the given status
                latest_record_with_status = MalariaHealthFacilityMonthly.query.filter(
                    MalariaHealthFacilityMonthly.status == status
                ).order_by(
                    desc(MalariaHealthFacilityMonthly.tahun),
                    desc(MalariaHealthFacilityMonthly.bulan)
                ).first()
                
                if latest_record_with_status:
                    # We found a record with the requested status
                    month = month or latest_record_with_status.bulan
                    year = year or latest_record_with_status.tahun
                    print(f"Found latest record with status '{status}': {month}/{year}")
                else:
                    # If no records with requested status, try getting the latest record of any status
                    latest_record = MalariaHealthFacilityMonthly.query.order_by(
                        desc(MalariaHealthFacilityMonthly.tahun),
                        desc(MalariaHealthFacilityMonthly.bulan)
                    ).first()
                    
                    if latest_record:
                        month = month or latest_record.bulan
                        year = year or latest_record.tahun
                        print(f"No records with status '{status}', using latest record: {month}/{year}")
                    else:
                        # If no records at all, default to current month and year
                        current_date = datetime.now()
                        month = month or current_date.month
                        year = year or current_date.year
                        print(f"No records found, using current date: {month}/{year}")
            
            # 1. Apply all filters first
            
            # Start with basic filters (month, year, status)
            filter_conditions = [
                MalariaHealthFacilityMonthly.bulan == month,
                MalariaHealthFacilityMonthly.tahun == year,
                MalariaHealthFacilityMonthly.status == status
            ]
            
            # Apply additional filters from filter_params
            if filters:
                # Filter by health facility ID
                if 'id_faskes' in filters and filters['id_faskes']:
                    filter_conditions.append(MalariaHealthFacilityMonthly.id_faskes == int(filters['id_faskes']))
                
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
            
            # 2. Apply sorting after filtering is complete
            if hasattr(MalariaHealthFacilityMonthly, sort_by):
                order_column = getattr(MalariaHealthFacilityMonthly, sort_by)
            else:
                # Default to id_mhfm if invalid sort column
                order_column = MalariaHealthFacilityMonthly.id_mhfm
            
            if sort_order and sort_order.lower() == 'desc':
                query = query.order_by(desc(order_column))
            else:
                query = query.order_by(asc(order_column))
            
            # 3. Apply pagination after filtering and sorting
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
                "status": status,
                "has_next": paginated_records.has_next,
                "has_prev": paginated_records.has_prev,
                "filters": filters or {}
            }
            
            return result, meta, None
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, None, str(e)



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
    def get_paginated(self, page=1, per_page=10, sort_by='id_faskes', sort_order='asc', filters=None):
        try:
            # Start with base query
            query = HealthFacilityId.query
            
            # Apply filters if provided
            filter_conditions = []
            if filters:
                # Filter by name
                if 'nama_faskes' in filters and filters['nama_faskes']:
                    filter_conditions.append(HealthFacilityId.nama_faskes.ilike(f"%{filters['nama_faskes']}%"))
                
                # Filter by location
                if 'provinsi' in filters and filters['provinsi']:
                    filter_conditions.append(HealthFacilityId.provinsi == filters['provinsi'])
                
                if 'kabupaten' in filters and filters['kabupaten']:
                    filter_conditions.append(HealthFacilityId.kabupaten == filters['kabupaten'])
                
                if 'kecamatan' in filters and filters['kecamatan']:
                    filter_conditions.append(HealthFacilityId.kecamatan == filters['kecamatan'])
                
                # Filter by facility type
                if 'tipe_faskes' in filters and filters['tipe_faskes']:
                    filter_conditions.append(HealthFacilityId.tipe_faskes == filters['tipe_faskes'])
                
                # Apply all filters if any
                if filter_conditions:
                    query = query.filter(and_(*filter_conditions))
            
            # Apply sorting
            if hasattr(HealthFacilityId, sort_by):
                order_column = getattr(HealthFacilityId, sort_by)
            else:
                # Default to id_faskes if invalid sort column
                order_column = HealthFacilityId.id_faskes
            
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
            distinct_provinces = db.session.query(HealthFacilityId.provinsi).distinct().all()
            distinct_districts = db.session.query(HealthFacilityId.kabupaten).distinct().all()
            distinct_subdistricts = db.session.query(HealthFacilityId.kecamatan).distinct().all()
            distinct_types = db.session.query(HealthFacilityId.tipe_faskes).distinct().all()
            
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
                    "provinces": [p[0] for p in distinct_provinces if p[0]],
                    "districts": [d[0] for d in distinct_districts if d[0]],
                    "subdistricts": [s[0] for s in distinct_subdistricts if s[0]],
                    "facility_types": [t[0] for t in distinct_types if t[0]]
                }
            }
            
            return result, meta, None
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, None, str(e)


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