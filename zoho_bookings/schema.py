"""
Static schema for the Zoho Bookings connector. Fivetran's Connector SDK only
needs table names and primary keys here — columns are inferred at upsert time
from the data dicts.

Nine tables total:
  - 5 core entities      : workspaces, services, staff, resources, appointments
  - 4 bridge join tables : service_staff_assignments, service_workspace_assignments,
                           staff_service_assignments, staff_workspace_assignments
"""

from helpers import validate_configuration


def get_schema(configuration: dict) -> list:
    validate_configuration(configuration)
    return [
        {"table": "workspaces",  "primary_key": ["id"]},
        {"table": "services",    "primary_key": ["id"]},
        {"table": "staff",       "primary_key": ["id"]},
        {"table": "resources",   "primary_key": ["id"]},
        {"table": "appointments", "primary_key": ["booking_id"]},

        {"table": "service_staff_assignments",
         "primary_key": ["service_id", "staff_id"]},
        {"table": "service_workspace_assignments",
         "primary_key": ["service_id", "workspace_id"]},
        {"table": "staff_service_assignments",
         "primary_key": ["staff_id", "service_id"]},
        {"table": "staff_workspace_assignments",
         "primary_key": ["staff_id", "workspace_id"]},
    ]
