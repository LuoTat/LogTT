#pragma once

#include "precomp.hxx"
#include <string>
#include <unordered_map>
#include <vector>

namespace logtt
{

Connection& get_connection();

using Filters = std::unordered_map<std::string, std::vector<std::string>>;

// ==================== 日志管理 ====================

struct LogEntry
{
    std::uint32_t id;
    std::string   log_type;
    std::string   format_type;
    std::string   log_uri;
    std::string   create_time;
    bool          is_extracted;
    std::string   extract_method;
    std::uint32_t line_count;
    std::string   structured_table_name;
    std::string   templates_table_name;
};

struct EXLogEntry
{
    std::uint32_t id;
    std::string   log_uri;
    std::string   structured_table_name;
    std::string   templates_table_name;
};

void                    create_log_table_if_not_exists();
std::vector<LogEntry>   get_log_table();
std::vector<EXLogEntry> get_extracted_log_table();
int                     insert_log(const std::string& log_type, const std::string& log_uri);
int  insert_log(const std::string& log_type, const std::string& log_uri, const std::string& extract_method);
void update_log_format_type(std::uint32_t log_id, const std::string& value);
void update_log_is_extracted(std::uint32_t log_id, bool value);
void update_log_extract_method(std::uint32_t log_id, const std::string& value);
void update_log_line_count(std::uint32_t log_id, std::uint32_t value);
void delete_log(std::uint32_t log_id);

// ==================== CSV表格显示 ====================

std::pair<std::vector<std::vector<std::string>>, std::uint32_t>
fetch_csv_table(const std::string& table_name, std::uint32_t offset, std::uint32_t limit, const Filters& filters);

// ==================== CSV表格过滤器 ====================

std::pair<std::vector<std::vector<std::string>>, std::uint32_t> fetch_filter_table(
    const std::string& table_name,
    const std::string& column_name,
    std::uint32_t      offset,
    std::uint32_t      limit,
    const std::string& keyword,
    const Filters&     other_filters
);

// ==================== 通用方法 ====================

bool                                    table_exists(const std::string& table_name);
void                                    drop_table(const std::string& table_name);
bool                                    has_column(const std::string& table_name, const std::string& column_name);
std::uint32_t                           get_table_row_count(const std::string& table_name);
std::vector<std::string>                get_table_columns(const std::string& table_name);
std::pair<std::uint64_t, std::uint64_t> compact_database();

}    // namespace logtt