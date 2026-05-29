"""Schema definitions for Xero Files API tables."""


def get_files_schema() -> list:
    return [
        {
            "table": "files_file",
            "primary_key": ["FileId"],
            "columns": {
                "FileId":   "STRING",
                "FolderId": "STRING",
                "Size":     "LONG",
            },
        },
        {
            "table": "files_folder",
            "primary_key": ["FolderId"],
            "columns": {
                "FolderId":   "STRING",
                "FileCount":  "INT",
                "IsInbox":    "BOOLEAN",
            },
        },
        {
            "table": "files_association",
            "primary_key": ["FileId", "ObjectId"],
            "columns": {
                "FileId":     "STRING",
                "ObjectId":   "STRING",
                "ObjectType": "STRING",
                "ObjectGroup": "STRING",
            },
        },
    ]
