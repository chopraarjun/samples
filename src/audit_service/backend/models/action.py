"""Allowed artifact access actions for event records and API filters.

Main entry points:
    ActionType: StrEnum of download, upload, and delete action values.
"""

from enum import StrEnum


class ActionType(StrEnum):
    """Download, upload, or delete — values match the JSONL ``action`` field.

    Members:
        DOWNLOAD: Artifact was downloaded.
        UPLOAD: Artifact was uploaded.
        DELETE: Artifact was deleted.
    """

    DOWNLOAD = "download"
    UPLOAD = "upload"
    DELETE = "delete"
