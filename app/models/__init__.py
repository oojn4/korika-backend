# Import models to make them available
from app.models.db_models import (
    # User model
    User,
    
    # Master data models
    MasterProv,
    MasterKab,
    MasterKec,
    
    # Health facility models
    HealthFacility,
    
    # Disease report models
    MalariaMonthly,
    SocioEnvironmentalFactorsMonthly,
    Population,
    DBD,
    Lepto
)