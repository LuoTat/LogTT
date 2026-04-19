#include "log_analysis.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"

namespace logtt
{

bool has_column(const std::string& structured_table_name, const std::string& column_name)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(structured_table_name)};
    for (auto&& col : rel->Columns())
    {
        if (col.Name() == column_name)
        {
            return true;
        }
    }
    return false;
}

std::vector<std::pair<std::string, std::uint32_t>> get_level_distribution(const std::string& structured_table_name)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(structured_table_name)};

    auto                                                         func_expr {duckdb::make_uniq<duckdb::FunctionExpression>("count", duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> {})};
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Level"));
    agg_exprs.push_back(std::move(func_expr));
    rel = rel->Aggregate(std::move(agg_exprs), "Level");

    std::vector<std::pair<std::string, std::uint32_t>> distribution;
    auto                                               result {to_materialized_query_result(rel->Execute())};
    for (auto&& i : std::views::iota(0UL, result->RowCount()))
    {
        auto level {result->GetValue(0, i)};
        auto count {result->GetValue<std::uint32_t>(1, i)};
        distribution.emplace_back(level.ToString(), count);
    }

    return distribution;
}

std::vector<std::pair<std::int64_t, std::uint32_t>> get_log_frequency_distribution(
    const std::string& structured_table_name,
    std::uint32_t      months,
    std::uint32_t      days,
    std::uint64_t      micros
)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(structured_table_name)};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_expr;
    arg_expr.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::INTERVAL(months, days, micros)));
    arg_expr.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Timestamp"));
    auto func_expr_1 {duckdb::make_uniq<duckdb::FunctionExpression>("time_bucket", std::move(arg_expr))};
    func_expr_1->SetAlias("Timestamp_bucket");

    auto func_expr_2 {duckdb::make_uniq<duckdb::FunctionExpression>("count", duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> {})};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(std::move(func_expr_1));
    agg_exprs.push_back(std::move(func_expr_2));
    rel = rel->Aggregate(std::move(agg_exprs), "Timestamp_bucket");
    rel = rel->Order("Timestamp_bucket");

    auto result {to_materialized_query_result(rel->Execute())};

    std::vector<std::pair<std::int64_t, std::uint32_t>> distribution;
    distribution.reserve(result->RowCount());
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& timestamp_bucket_col {data_chunk.data[0]};
        const auto& count_col {data_chunk.data[1]};

        const auto timestamp_bucket_data {duckdb::FlatVector::GetData<duckdb::timestamp_sec_t>(timestamp_bucket_col)};
        const auto count_data {duckdb::FlatVector::GetData<std::int64_t>(count_col)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            auto timestamp_bucket {timestamp_bucket_data[row]};
            auto count {count_data[row]};

            distribution.emplace_back(timestamp_bucket.value / 1000000, count);
        }
    }

    return distribution;
}

}    // namespace logtt
