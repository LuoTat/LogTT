from libc.stdint cimport int64_t, uint32_t, uint64_t
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp.vector cimport vector

cdef extern from "duckdb_service.hxx" namespace "logtt" nogil:
    ctypedef unordered_map[string, vector[string]] Filters

    #  ==================== 日志管理 ====================

    cdef struct LogEntry:
        uint32_t id
        string   format_type
        string   log_path
        string   create_time
        bint     is_extracted
        string   extract_method
        uint32_t line_count
        string   structured_table_name
        string   templates_table_name

    cdef struct EXLogEntry:
        uint32_t id
        string   log_path
        string   structured_table_name
        string   templates_table_name

    void create_log_table_if_not_exists()
    vector[LogEntry] get_log_table()
    vector[EXLogEntry] get_extracted_log_table()
    int insert_log(const string& log_path)
    void update_log_format_type(uint32_t log_id, const string& value)
    void update_log_is_extracted(uint32_t log_id, bool value)
    void update_log_extract_method(uint32_t log_id, const string& value)
    void update_log_line_count(uint32_t log_id, uint32_t value)
    void delete_log(uint32_t log_id)

    # ==================== CSV表格显示 ====================

    pair[vector[vector[string]], int64_t] fetch_csv_table(
        const string&  table_name,
        int64_t       offset,
        int64_t       limit,
        const Filters& filters,
    )

    # ==================== CSV表格过滤器 ====================

    pair[vector[vector[string]], int64_t] fetch_filter_table(
        const string&  table_name,
        const string&  column_name,
        int64_t       offset,
        int64_t       limit,
        const string&  keyword,
        const Filters& other_filters,
    )

    # ==================== 通用方法 ====================

    bint table_exists(const string& table_name)
    void drop_table(const string& table_name)
    bint has_column(const string& table_name, const string& column_name)
    int64_t get_table_row_count(const string& table_name)
    vector[string] get_table_columns(const string& table_name)
