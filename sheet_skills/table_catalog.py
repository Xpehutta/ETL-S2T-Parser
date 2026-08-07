"""Sheet skill for source/target table catalogs."""

from typing import Any, Dict, List

from .configured_rows import extract_configured_rows


TABLE_CATALOG_TARGETS = ("source_tables", "target_tables")


def extract_table_catalogs(
    file_id: int,
    sheet_group_analysis: Dict[str, Any],
) -> Dict[str, Any]:
    """Extract table catalogs, embed descriptions and preserve equal rows."""
    from services.embeddings import embed_descriptions

    def prepare_records(
        target_name: str,
        records: List[Dict[str, Any]],
    ) -> None:
        del target_name
        embeddings = embed_descriptions([row["description"] for row in records])
        for row, embedding in zip(records, embeddings):
            row["description_embedding"] = embedding

    return extract_configured_rows(
        file_id,
        sheet_group_analysis,
        TABLE_CATALOG_TARGETS,
        extra_columns={
            target_name: ("description_embedding",)
            for target_name in TABLE_CATALOG_TARGETS
        },
        prepare_records=prepare_records,
    )


__all__ = ["TABLE_CATALOG_TARGETS", "extract_table_catalogs"]
