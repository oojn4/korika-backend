from app import db
from sqlalchemy.exc import SQLAlchemyError
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

    def delete_many(self, id_field, id_list):
        """Delete multiple records by their IDs."""
        try:
            deleted_count = 0
            errors = []
            
            for id_value in id_list:
                record = self.model.query.filter_by(**{id_field: id_value}).first()
                if record:
                    db.session.delete(record)
                    deleted_count += 1
                else:
                    errors.append(f"{self.model.__name__} with {id_field} {id_value} not found")
            
            db.session.commit()
            return {
                "deleted_count": deleted_count,
                "errors": errors,
                "total_requested": len(id_list)
            }, None
        except SQLAlchemyError as e:
            db.session.rollback()
            return None, str(e)
        finally:
            db.session.close()

    def filter_by(self, **kwargs):
        """Filter records based on provided criteria."""
        try:
            records = self.model.query.filter_by(**kwargs).all()
            return [record.to_dict() if hasattr(record, 'to_dict') else record for record in records], None
        except SQLAlchemyError as e:
            return None, str(e)

    def get_paginated(self, page=1, per_page=10, sort_by=None, sort_order='asc', filters=None):
        """Generic paginated query with sorting and filtering."""
        try:
            # Start with base query
            query = self.model.query
            
            # Apply filters if provided and if model has filter_conditions method
            if filters and hasattr(self, '_build_filter_conditions'):
                filter_conditions = self._build_filter_conditions(filters)
                if filter_conditions:
                    query = query.filter(and_(*filter_conditions))
            
            # Apply sorting
            if sort_by and hasattr(self.model, sort_by):
                order_column = getattr(self.model, sort_by)
            else:
                # Try to find a default primary key column
                primary_keys = [key.name for key in self.model.__table__.primary_key.columns]
                if primary_keys:
                    order_column = getattr(self.model, primary_keys[0])
                else:
                    order_column = getattr(self.model, 'id', None)
            
            if order_column is not None:
                if sort_order and sort_order.lower() == 'desc':
                    query = query.order_by(desc(order_column))
                else:
                    query = query.order_by(asc(order_column))
            
            # Apply pagination
            paginated_records = query.paginate(page=page, per_page=per_page, error_out=False)
            
            # Prepare result
            records = paginated_records.items
            result = [record.to_dict() if hasattr(record, 'to_dict') else record for record in records]
            
            # Prepare metadata
            meta = {
                "page": page,
                "per_page": per_page,
                "total_pages": paginated_records.pages,
                "total_records": paginated_records.total,
                "has_next": paginated_records.has_next,
                "has_prev": paginated_records.has_prev,
                "filters": filters or {}
            }
            
            # Add distinct values if method exists
            if hasattr(self, '_get_distinct_values'):
                meta["distinct_values"] = self._get_distinct_values()
            
            return result, meta, None
        except Exception as e:
            import traceback
            traceback.print_exc()
            return None, None, str(e)