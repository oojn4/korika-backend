from app import db
from sqlalchemy.exc import SQLAlchemyError
from app.models import User
from sqlalchemy import desc, asc, or_, and_
from datetime import datetime
from app.services.base import BaseCRUDService

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
