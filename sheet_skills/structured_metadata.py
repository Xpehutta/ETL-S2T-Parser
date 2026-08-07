"""Sheet skill for additional objects and PXF-to-A metadata."""

from typing import Any, Dict

from .additional_objects import extract_additional_object_transformations
from .configured_rows import extract_configured_rows


STRUCTURED_METADATA_TARGETS = ("additional_objects", "pxf_to_a")


def extract_structured_metadata(
    file_id: int,
    sheet_group_analysis: Dict[str, Any],
) -> Dict[str, Any]:
    """Extract configured metadata rows without deduplication or embeddings."""
    report = extract_configured_rows(
        file_id,
        sheet_group_analysis,
        STRUCTURED_METADATA_TARGETS,
    )
    report["targets"]["additional_objects"]["etl_transformations"] = (
        extract_additional_object_transformations(file_id)
    )
    return report


__all__ = ["STRUCTURED_METADATA_TARGETS", "extract_structured_metadata"]
