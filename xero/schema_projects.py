"""Schema definitions for Xero Projects API tables."""


def get_projects_schema() -> list:
    return [
        {
            "table": "projects_project",
            "primary_key": ["ProjectId"],
            "columns": {
                "ProjectId":      "STRING",
                "ContactId":      "STRING",
                "EstimateAmount": "FLOAT",
                "TotalInvoiced":  "FLOAT",
                "TotalToBeInvoiced": "FLOAT",
                "MinutesLogged":  "INT",
                "MinutesToBeInvoiced": "INT",
                "TaskAmount":     "FLOAT",
                "ExpenseAmount":  "FLOAT",
                "IsTracked":      "BOOLEAN",
            },
        },
        {
            "table": "projects_task",
            "primary_key": ["ProjectId", "TaskId"],
            "columns": {
                "ProjectId":  "STRING",
                "TaskId":     "STRING",
                "Rate":       "FLOAT",
                "EstimateMinutes": "INT",
                "TotalMinutes": "INT",
                "TotalAmount": "FLOAT",
            },
        },
        {
            "table": "projects_time_entry",
            "primary_key": ["TimeEntryId"],
            "columns": {
                "TimeEntryId": "STRING",
                "ProjectId":   "STRING",
                "TaskId":      "STRING",
                "UserId":      "STRING",
                "Duration":    "INT",
            },
        },
        {
            "table": "projects_project_user",
            "primary_key": ["UserId"],
            "columns": {
                "UserId": "STRING",
            },
        },
    ]
