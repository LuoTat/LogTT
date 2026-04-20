from libc.stdint cimport uint32_t, uint64_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector

from modules.duckdb_service cimport (
    EXLogEntry,
    Filters,
    LogEntry,
    compact_database as cxx_compact_database,
    create_log_table_if_not_exists as cxx_create_log_table_if_not_exists,
    delete_log as cxx_delete_log,
    drop_table as cxx_drop_table,
    fetch_csv_table as cxx_fetch_csv_table,
    fetch_filter_table as cxx_fetch_filter_table,
    get_extracted_log_table as cxx_get_extracted_log_table,
    get_log_table as cxx_get_log_table,
    get_table_columns as cxx_get_table_columns,
    get_table_row_count as cxx_get_table_row_count,
    insert_log as cxx_insert_log,
    table_exists as cxx_table_exists,
    update_log_extract_method as cxx_update_log_extract_method,
    update_log_format_type as cxx_update_log_format_type,
    update_log_is_extracted as cxx_update_log_is_extracted,
    update_log_line_count as cxx_update_log_line_count,
)

cdef class DuckDBService:
    @staticmethod
    def create_log_table_if_not_exists():
        with nogil:
            cxx_create_log_table_if_not_exists()

    @staticmethod
    def get_log_table() -> list[tuple]:
        cdef vector[LogEntry] table
        cdef vector[(uint32_t, string, string, string, string, bint, string, uint32_t, string, string)] result

        with nogil:
            table = cxx_get_log_table()

        result.reserve(table.size())
        cdef LogEntry entry
        for entry in table:
            result.push_back(
                (
                    entry.id,
                    entry.log_type,
                    entry.format_type,
                    entry.log_uri,
                    entry.create_time,
                    entry.is_extracted,
                    entry.extract_method,
                    entry.line_count,
                    entry.structured_table_name,
                    entry.templates_table_name,
                )
            )

        return result

    @staticmethod
    def get_extracted_log_table() -> list[tuple]:
        cdef vector[EXLogEntry] table
        cdef vector[(uint32_t, string, string, string)] result

        with nogil:
            table = cxx_get_extracted_log_table()

        result.reserve(table.size())
        cdef EXLogEntry entry
        for entry in table:
            result.push_back(
                (
                    entry.id,
                    entry.log_uri,
                    entry.structured_table_name,
                    entry.templates_table_name,
                )
            )

        return result

    @staticmethod
    def insert_log_with_no_extract_method(string log_type, string log_uri) -> int:
        cdef int ret
        with nogil:
            ret = cxx_insert_log(log_type, log_uri)

        return ret

    @staticmethod
    def insert_log_with_extract_method(
        string log_type,
        string log_uri,
        string extract_method,
    ) -> int:
        cdef int ret
        with nogil:
            ret = cxx_insert_log(log_type, log_uri, extract_method)

        return ret

    @staticmethod
    def update_log(uint32_t log_id, string column_name, object value):
        cdef string log_format_type
        cdef bool is_extracted
        cdef string extract_method
        cdef uint32_t line_count

        if column_name == "format_type":
            log_format_type = value
            with nogil:
                cxx_update_log_format_type(log_id, log_format_type)
        elif column_name == "is_extracted":
            is_extracted = value
            with nogil:
                cxx_update_log_is_extracted(log_id, is_extracted)
        elif column_name == "extract_method":
            extract_method = value
            with nogil:
                cxx_update_log_extract_method(log_id, extract_method)
        elif column_name == "line_count":
            line_count = value
            with nogil:
                cxx_update_log_line_count(log_id, line_count)
        else:
            raise ValueError(f"Unsupported column name: {column_name}")

    @staticmethod
    def delete_log(uint32_t log_id):
        with nogil:
            cxx_delete_log(log_id)

    @staticmethod
    def fetch_csv_table(
        string table_name,
        uint32_t offset,
        uint32_t limit,
        object filters=None,
    ) -> tuple[list[list[str]], int]:
        cdef pair[vector[vector[string]], uint32_t] result
        cdef Filters filters_cxx

        if filters is None:
            filters_cxx = Filters()
        else:
            filters_cxx = filters

        with nogil:
            result = cxx_fetch_csv_table(
                table_name,
                offset,
                limit,
                filters_cxx,
            )

        return result

    @staticmethod
    def fetch_filter_table(
        string table_name,
        string column_name,
        uint32_t offset,
        uint32_t limit,
        object keyword=None,
        object other_filters=None,
    ) -> tuple[list[list[str]], int]:
        cdef pair[vector[vector[string]], uint32_t] result
        cdef string keyword_cxx
        cdef Filters other_filters_cxx

        if keyword is None:
            keyword_cxx = string()
        else:
            keyword_cxx = keyword

        if other_filters is None:
            other_filters_cxx = Filters()
        else:
            other_filters_cxx = other_filters

        with nogil:
            result = cxx_fetch_filter_table(
                table_name,
                column_name,
                offset,
                limit,
                keyword_cxx,
                other_filters_cxx,
            )

        return result

    @staticmethod
    def table_exists(string table_name) -> bool:
        cdef bint exists

        with nogil:
            exists = cxx_table_exists(table_name)

        return exists

    @staticmethod
    def drop_table(string table_name):
        with nogil:
            cxx_drop_table(table_name)

    @staticmethod
    def has_column(string table_name, string column_name) -> bool:
        cdef bint result

        with nogil:
            result = cxx_has_column(table_name, column_name)

        return result

    @staticmethod
    def get_table_row_count(string table_name) -> int:
        cdef uint32_t row_count

        with nogil:
            row_count = cxx_get_table_row_count(table_name)

        return row_count

    @staticmethod
    def get_table_columns(string table_name) -> list[str]:
        cdef vector[string] columns

        with nogil:
            columns = cxx_get_table_columns(table_name)

        return columns

    @staticmethod
    def compact_database() -> tuple[int, int]:
        cdef pair[uint64_t, uint64_t] result

        with nogil:
            result = cxx_compact_database()

        return result
