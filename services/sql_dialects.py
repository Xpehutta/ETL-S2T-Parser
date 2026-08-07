"""Project SQLGlot dialect registrations."""

from sqlglot.dialects.postgres import Postgres


GREENPLUM_DIALECT = "greenplum"


class Greenplum(Postgres):
    """Greenplum SQL parsed with its PostgreSQL-compatible grammar."""


__all__ = ["GREENPLUM_DIALECT", "Greenplum"]
