#include "duckdb_service.hxx"
#include "utils.hxx"
#include <filesystem>
#include <format>
#include <print>
#include <ranges>

namespace logtt
{

const static char* const DB_PATH {"logtt.duckdb"};

duckdb::Connection& get_connection()
{
    static duckdb::DuckDB           db {DB_PATH};
    thread_local duckdb::Connection conn {db};

#ifdef LOGTT_ENABLE_PROFILING
    conn.EnableProfiling();
#endif

    return conn;
}

// ==================== 辅助函数 ====================

static std::vector<std::vector<std::string>> _to_df(duckdb::shared_ptr<duckdb::Relation> rel)
{
    rel = rel->Project("COLUMNS(*)::STRING");

    auto result {to_materialized_query_result(rel->Execute())};
    auto row_length {result->RowCount()};
    auto col_length {result->ColumnCount()};

    std::vector<std::vector<std::string>> df;
    df.reserve(row_length);
    for (const auto& data_chunk : result->Collection().Chunks())
    {
        std::vector<const duckdb::string_t*>     all_datas(col_length);
        std::vector<const duckdb::ValidityMask*> all_validities(col_length);
        for (auto col : std::views::iota(0UL, col_length))
        {
            all_datas[col]      = duckdb::FlatVector::GetData<duckdb::string_t>(data_chunk.data[col]);
            all_validities[col] = &duckdb::FlatVector::Validity(data_chunk.data[col]);
        }

        for (auto row : std::views::iota(0UL, data_chunk.size()))
        {
            auto& row_data {df.emplace_back()};
            row_data.reserve(col_length);
            for (auto col : std::views::iota(0UL, col_length))
            {
                if (!all_validities[col]->RowIsValid(row))
                {
                    row_data.emplace_back();
                }
                else
                {
                    const auto& value {all_datas[col][row]};
                    row_data.emplace_back(value.GetData(), value.GetSize());
                }
            }
        }
    }

    return df;
}

static duckdb::unique_ptr<duckdb::ParsedExpression> _build_filter_expr(const Filters& filters)
{
    std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> in_exprs;
    in_exprs.reserve(filters.size());
    for (const auto& [col, values] : filters)
    {
        // 单列多值 → IN 列表
        std::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs;
        arg_exprs.reserve(values.size() + 1);
        arg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(col));
        for (const auto& v : values)
        {
            arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(v)));
        }
        in_exprs.push_back(duckdb::make_uniq<duckdb::OperatorExpression>(duckdb::ExpressionType::COMPARE_IN, std::move(arg_exprs)));
    }

    // 多列 → AND 链接
    return duckdb::make_uniq<duckdb::ConjunctionExpression>(
        duckdb::ExpressionType::CONJUNCTION_AND,
        std::move(in_exprs)
    );
}

static duckdb::unique_ptr<duckdb::ParsedExpression> _build_like_filter_expr(const std::string& column_name, const std::string& keyword)
{
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs;
    arg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(column_name));
    arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(keyword)));
    return duckdb::make_uniq<duckdb::FunctionExpression>("contains", std::move(arg_exprs));
}

// ==================== 日志管理 ====================

void create_log_table_if_not_exists()
{
    auto& conn {get_connection()};
    conn.Query(R"(
        CREATE SEQUENCE IF NOT EXISTS log_id_seq START 1;
        CREATE TABLE IF NOT EXISTS log
        (
            id                    UINT32      PRIMARY KEY DEFAULT nextval('log_id_seq'),
            log_type              STRING      NOT NULL,
            format_type           STRING      NOT NULL DEFAULT '',
            log_uri               STRING      NOT NULL UNIQUE,
            create_time           TIMESTAMP_S NOT NULL DEFAULT current_localtimestamp(),
            is_extracted          BOOL        NOT NULL DEFAULT FALSE,
            extract_method        STRING      NOT NULL DEFAULT '',
            line_count            UINT32      NOT NULL DEFAULT 0,
            structured_table_name STRING AS ('s_' || id::STRING),
            templates_table_name  STRING AS ('t_' || id::STRING)
        );
    )");
}

std::vector<LogEntry> get_log_table()
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};

    auto result {to_materialized_query_result(rel->Execute())};

    std::vector<LogEntry> log_table;
    log_table.reserve(result->RowCount());
    for (const auto& data_chunk : result->Collection().Chunks())
    {
        const auto& id_col {data_chunk.data[0]};
        const auto& log_type_col {data_chunk.data[1]};
        const auto& format_type_col {data_chunk.data[2]};
        const auto& log_uri_col {data_chunk.data[3]};
        const auto& create_time_col {data_chunk.data[4]};
        const auto& is_extracted_col {data_chunk.data[5]};
        const auto& extract_method_col {data_chunk.data[6]};
        const auto& line_count_col {data_chunk.data[7]};
        const auto& structured_table_name_col {data_chunk.data[8]};
        const auto& templates_table_name_col {data_chunk.data[9]};

        const auto id_data {duckdb::FlatVector::GetData<std::uint32_t>(id_col)};
        const auto log_type_data {duckdb::FlatVector::GetData<duckdb::string_t>(log_type_col)};
        const auto format_type_data {duckdb::FlatVector::GetData<duckdb::string_t>(format_type_col)};
        const auto log_uri_data {duckdb::FlatVector::GetData<duckdb::string_t>(log_uri_col)};
        const auto create_time_data {duckdb::FlatVector::GetData<duckdb::timestamp_sec_t>(create_time_col)};
        const auto is_extracted_data {duckdb::FlatVector::GetData<bool>(is_extracted_col)};
        const auto extract_method_data {duckdb::FlatVector::GetData<duckdb::string_t>(extract_method_col)};
        const auto line_count_data {duckdb::FlatVector::GetData<std::uint32_t>(line_count_col)};
        const auto structured_table_name_data {duckdb::FlatVector::GetData<duckdb::string_t>(structured_table_name_col)};
        const auto templates_table_name_data {duckdb::FlatVector::GetData<duckdb::string_t>(templates_table_name_col)};

        for (auto row : std::views::iota(0UL, data_chunk.size()))
        {
            auto id {id_data[row]};
            auto log_type {log_type_data[row]};
            auto format_type {format_type_data[row]};
            auto log_uri {log_uri_data[row]};
            auto create_time {create_time_data[row]};
            auto is_extracted {is_extracted_data[row]};
            auto extract_method {extract_method_data[row]};
            auto line_count {line_count_data[row]};
            auto structured_table_name {structured_table_name_data[row]};
            auto templates_table_name {templates_table_name_data[row]};

            log_table.emplace_back(
                id,
                std::string(log_type.GetData(), log_type.GetSize()),
                std::string(format_type.GetData(), format_type.GetSize()),
                std::string(log_uri.GetData(), log_uri.GetSize()),
                duckdb::Timestamp::ToString(duckdb::Timestamp::FromEpochSeconds(create_time.value)),
                is_extracted,
                std::string(extract_method.GetData(), extract_method.GetSize()),
                line_count,
                std::string(structured_table_name.GetData(), structured_table_name.GetSize()),
                std::string(templates_table_name.GetData(), templates_table_name.GetSize())
            );
        }
    }

    return log_table;
}

std::vector<EXLogEntry> get_extracted_log_table()
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};
    rel = rel->Filter(
        duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL,
            duckdb::make_uniq<duckdb::ColumnRefExpression>("is_extracted"),
            duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::BOOLEAN(true))
        )
    );

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs;
    project_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("id"));
    project_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("log_uri"));
    project_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("structured_table_name"));
    project_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("templates_table_name"));
    rel = rel->Project(std::move(project_exprs), {});

    auto result {to_materialized_query_result(rel->Execute())};

    std::vector<EXLogEntry> log_table;
    log_table.reserve(result->RowCount());
    for (const auto& data_chunk : result->Collection().Chunks())
    {
        const auto& id_col {data_chunk.data[0]};
        const auto& log_uri_col {data_chunk.data[1]};
        const auto& structured_table_name_col {data_chunk.data[2]};
        const auto& templates_table_name_col {data_chunk.data[3]};

        const auto id_data {duckdb::FlatVector::GetData<std::uint32_t>(id_col)};
        const auto log_uri_data {duckdb::FlatVector::GetData<duckdb::string_t>(log_uri_col)};
        const auto structured_table_name_data {duckdb::FlatVector::GetData<duckdb::string_t>(structured_table_name_col)};
        const auto templates_table_name_data {duckdb::FlatVector::GetData<duckdb::string_t>(templates_table_name_col)};

        for (auto row : std::views::iota(0UL, data_chunk.size()))
        {
            auto id {id_data[row]};
            auto log_uri {log_uri_data[row]};
            auto structured_table_name {structured_table_name_data[row]};
            auto templates_table_name {templates_table_name_data[row]};

            log_table.emplace_back(
                id,
                std::string(log_uri.GetData(), log_uri.GetSize()),
                std::string(structured_table_name.GetData(), structured_table_name.GetSize()),
                std::string(templates_table_name.GetData(), templates_table_name.GetSize())
            );
        }
    }

    return log_table;
}

int insert_log(const std::string& log_type, const std::string& log_uri)
{
    auto& conn {get_connection()};
    try
    {
        duckdb::Appender appender {conn, "log"};
        appender.AddColumn("log_type");
        appender.AddColumn("log_uri");
        appender.AppendRow(log_type.c_str(), log_uri.c_str());
        appender.Close();
        return 0;
    }
    catch (const duckdb::Exception& e)
    {
        duckdb::ErrorData error {e};
        if (error.Type() == duckdb::ExceptionType::CONSTRAINT)
        {
            return -1;
        }
        else
        {
            return -2;
        }
    }
}

int insert_log(const std::string& log_type, const std::string& log_uri, const std::string& extract_method)
{
    auto& conn {get_connection()};
    try
    {
        duckdb::Appender appender {conn, "log"};
        appender.AddColumn("log_type");
        appender.AddColumn("log_uri");
        appender.AddColumn("extract_method");
        appender.AppendRow(log_type.c_str(), log_uri.c_str(), extract_method.c_str());
        appender.Close();
        return 0;
    }
    catch (const duckdb::Exception& e)
    {
        duckdb::ErrorData error {e};
        if (error.Type() == duckdb::ExceptionType::CONSTRAINT)
        {
            return -1;
        }
        else
        {
            return -2;
        }
    }
}

void update_log_format_type(std::uint32_t log_id, const std::string& value)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> update_exprs;
    update_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(value)));
    rel->Update(
        {"format_type"},
        std::move(update_exprs),
        duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL,
            duckdb::make_uniq<duckdb::ColumnRefExpression>("id"),
            duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(log_id))
        )
    );
}

void update_log_is_extracted(std::uint32_t log_id, bool value)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> update_exprs;
    update_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::BOOLEAN(value)));
    rel->Update(
        {"is_extracted"},
        std::move(update_exprs),
        duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL,
            duckdb::make_uniq<duckdb::ColumnRefExpression>("id"),
            duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(log_id))
        )
    );
}

void update_log_extract_method(std::uint32_t log_id, const std::string& value)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> update_exprs;
    update_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(value)));
    rel->Update(
        {"extract_method"},
        std::move(update_exprs),
        duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL,
            duckdb::make_uniq<duckdb::ColumnRefExpression>("id"),
            duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(log_id))
        )
    );
}

void update_log_line_count(std::uint32_t log_id, std::uint32_t value)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> update_exprs;
    update_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(value)));
    rel->Update(
        {"line_count"},
        std::move(update_exprs),
        duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL,
            duckdb::make_uniq<duckdb::ColumnRefExpression>("id"),
            duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::UINTEGER(log_id))
        )
    );
}

void delete_log(std::uint32_t log_id)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table("log")};
    rel->Delete(std::format("id = {}", log_id));
}

// ==================== CSV表格显示 ====================

std::pair<std::vector<std::vector<std::string>>, std::uint32_t> fetch_csv_table(
    const std::string& table_name,
    std::uint32_t      offset,
    std::uint32_t      limit,
    const Filters&     filters
)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(table_name)};
    if (!filters.empty())
    {
        rel = rel->Filter(_build_filter_expr(filters));
    }

    rel = get_tmp(conn, rel);

    auto result {conn.Query("SELECT count()::UINT64 FROM _tmp")};
    auto log_length {result->GetValue<std::uint64_t>(0, 0)};

    rel = rel->Limit(limit, offset);
    return {_to_df(rel), log_length};
}

// ==================== CSV表格过滤器 ====================

std::pair<std::vector<std::vector<std::string>>, std::uint32_t> fetch_filter_table(
    const std::string& table_name,
    const std::string& column_name,
    std::uint32_t      offset,
    std::uint32_t      limit,
    const std::string& keyword,
    const Filters&     other_filters
)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(table_name)};

    if (!keyword.empty())
    {
        rel = rel->Filter(_build_like_filter_expr(column_name, keyword));
    }
    if (!other_filters.empty())
    {
        rel = rel->Filter(_build_filter_expr(other_filters));
    }

    auto func_expr {duckdb::make_uniq<duckdb::FunctionExpression>("count", duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> {})};
    func_expr->SetAlias("Count");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(column_name));
    agg_exprs.push_back(std::move(func_expr));
    rel = rel->Aggregate(std::move(agg_exprs), column_name);

    duckdb::vector<duckdb::OrderByNode> order_bys;
    order_bys.emplace_back(
        duckdb::OrderType::DESCENDING,
        duckdb::OrderByNullType::ORDER_DEFAULT,
        duckdb::make_uniq<duckdb::ColumnRefExpression>("Count")
    );
    rel = rel->Order(std::move(order_bys));

    rel = get_tmp(conn, rel);

    auto result {conn.Query("SELECT count()::UINT64 FROM _tmp")};
    auto log_length {result->GetValue<std::uint64_t>(0, 0)};

    rel = rel->Limit(limit, offset);
    return {_to_df(rel), log_length};
}

// ==================== 通用方法 ====================

bool table_exists(const std::string& table_name)
{
    auto& conn {get_connection()};
    return conn.TableInfo(table_name) != nullptr;
}

void drop_table(const std::string& table_name)
{
    auto& conn {get_connection()};
    conn.Query(std::format("DROP TABLE IF EXISTS {}", table_name));
}

std::uint32_t get_table_row_count(const std::string& table_name)
{
    auto& conn {get_connection()};
    auto  result {conn.Query(std::format("SELECT count()::UINT64 FROM {}", table_name))};
    return result->GetValue<std::uint32_t>(0, 0);
}

std::vector<std::string> get_table_columns(const std::string& table_name)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(table_name)};

    std::vector<std::string> columns;
    columns.reserve(rel->Columns().size());
    for (const auto& col : rel->Columns())
    {
        columns.push_back(col.Name());
    }
    return columns;
}

std::pair<std::uint64_t, std::uint64_t> compact_database()
{
    namespace fs = std::filesystem;

    auto& conn {get_connection()};
    auto  db_path {fs::path(DB_PATH)};
    auto  original_size {static_cast<std::uint64_t>(fs::exists(db_path) ? fs::file_size(db_path) : 0)};

    conn.Query("CHECKPOINT");
    conn.Query("VACUUM");

    auto new_size {static_cast<std::uint64_t>(fs::exists(db_path) ? fs::file_size(db_path) : 0)};
    return {original_size, new_size};
}

}    // namespace logtt