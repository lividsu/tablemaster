from __future__ import annotations

from abc import ABC, abstractmethod

from ..models import ActualTable, ColumnDef, IndexDef, TableDef


class BaseDialect(ABC):
    @abstractmethod
    def map_type(self, generic_type: str) -> str:
        pass

    @abstractmethod
    def normalize_type(self, db_type: str) -> str:
        pass

    @abstractmethod
    def introspect(
        self,
        engine,
        database: str,
        table_names: list[str] | None = None,
        schema_name: str | None = None,
    ) -> list[ActualTable]:
        pass

    @abstractmethod
    def gen_create_table(self, table: TableDef) -> str:
        pass

    @abstractmethod
    def gen_add_column(self, table: str, col: ColumnDef, schema_name: str | None = None) -> str:
        pass

    @abstractmethod
    def gen_alter_column_type(
        self,
        table: str,
        col_name: str,
        new_type: str,
        schema_name: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def gen_alter_column_nullable(
        self,
        table: str,
        col_name: str,
        nullable: bool,
        col_type: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def gen_alter_column_default(
        self,
        table: str,
        col_name: str,
        default: str | None,
        schema_name: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def gen_alter_column_comment(
        self,
        table: str,
        col_name: str,
        comment: str | None,
        col_type: str | None = None,
        schema_name: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def gen_alter_table_comment(
        self,
        table: str,
        comment: str | None,
        schema_name: str | None = None,
    ) -> str:
        pass

    @abstractmethod
    def gen_create_index(self, table: str, index: IndexDef, schema_name: str | None = None) -> str:
        pass

    @abstractmethod
    def gen_drop_index(self, table: str, index_name: str, schema_name: str | None = None) -> str:
        pass
