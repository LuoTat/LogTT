#include "log_analysis.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"

namespace logtt
{

std::pair<std::vector<std::string>, std::vector<std::uint32_t>> get_level_distribution(const std::string& structured_table_name)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(structured_table_name)};

    auto                                                         func_expr {duckdb::make_uniq<duckdb::FunctionExpression>("count", duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> {})};
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Level"));
    agg_exprs.push_back(std::move(func_expr));
    rel = rel->Aggregate(std::move(agg_exprs), "Level");
    rel = rel->Order("Level");

    std::pair<std::vector<std::string>, std::vector<std::uint32_t>> distribution;
    auto                                                            result {to_materialized_query_result(rel->Execute())};
    for (auto&& i : std::views::iota(0UL, result->RowCount()))
    {
        auto level {result->GetValue(0, i)};
        auto count {result->GetValue<std::uint32_t>(1, i)};
        distribution.first.push_back(level.ToString());
        distribution.second.push_back(count);
    }

    return distribution;
}

std::pair<std::vector<std::int64_t>, std::vector<std::uint32_t>> get_log_frequency_distribution(
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

    std::pair<std::vector<std::int64_t>, std::vector<std::uint32_t>> distribution;
    distribution.first.reserve(result->RowCount());
    distribution.second.reserve(result->RowCount());
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& timestamp_bucket_col {data_chunk.data[0]};
        const auto& count_col {data_chunk.data[1]};

        const auto timestamp_bucket_data {duckdb::FlatVector::GetData<duckdb::timestamp_t>(timestamp_bucket_col)};
        const auto count_data {duckdb::FlatVector::GetData<std::int64_t>(count_col)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            auto timestamp_bucket {timestamp_bucket_data[row]};
            auto count {count_data[row]};

            distribution.first.push_back(timestamp_bucket.value / 1000000);
            distribution.second.push_back(count);
        }
    }

    return distribution;
}

std::pair<std::vector<std::int64_t>, std::vector<std::uint32_t>> get_template_frequency_distribution(
    const std::string& structured_table_name,
    std::uint32_t      months,
    std::uint32_t      days,
    std::uint64_t      micros
)
{
    auto& conn {get_connection()};
    auto  rel {conn.Table(structured_table_name)};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_expr_1;
    arg_expr_1.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::INTERVAL(months, days, micros)));
    arg_expr_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Timestamp"));
    auto func_expr_1 {duckdb::make_uniq<duckdb::FunctionExpression>("time_bucket", std::move(arg_expr_1))};
    func_expr_1->SetAlias("Timestamp_bucket");

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_expr_2;
    arg_expr_2.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Template"));
    auto func_expr_2 {duckdb::make_uniq<duckdb::FunctionExpression>("count", std::move(arg_expr_2))};
    func_expr_2->distinct = true;

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(std::move(func_expr_1));
    agg_exprs.push_back(std::move(func_expr_2));
    rel = rel->Aggregate(std::move(agg_exprs), "Timestamp_bucket");
    rel = rel->Order("Timestamp_bucket");

    auto result {to_materialized_query_result(rel->Execute())};

    std::pair<std::vector<std::int64_t>, std::vector<std::uint32_t>> distribution;
    distribution.first.reserve(result->RowCount());
    distribution.second.reserve(result->RowCount());
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& timestamp_bucket_col {data_chunk.data[0]};
        const auto& count_col {data_chunk.data[1]};

        const auto timestamp_bucket_data {duckdb::FlatVector::GetData<duckdb::timestamp_t>(timestamp_bucket_col)};
        const auto count_data {duckdb::FlatVector::GetData<std::int64_t>(count_col)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            auto timestamp_bucket {timestamp_bucket_data[row]};
            auto count {count_data[row]};

            distribution.first.push_back(timestamp_bucket.value / 1000000);
            distribution.second.push_back(count);
        }
    }

    return distribution;
}

std::unordered_map<std::string, std::pair<std::vector<std::int64_t>, std::vector<std::uint32_t>>> get_log_level_frequency_distribution(
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
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Level"));
    agg_exprs.push_back(std::move(func_expr_1));
    agg_exprs.push_back(std::move(func_expr_2));
    rel = rel->Aggregate(std::move(agg_exprs), "Timestamp_bucket, Level");
    rel = rel->Order("Timestamp_bucket");

    auto result {to_materialized_query_result(rel->Execute())};

    std::unordered_map<std::string, std::pair<std::vector<std::int64_t>, std::vector<std::uint32_t>>> level_distribution;
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& level_col {data_chunk.data[0]};
        const auto& timestamp_bucket_col {data_chunk.data[1]};
        const auto& count_col {data_chunk.data[2]};

        const auto level_data {duckdb::FlatVector::GetData<duckdb::string_t>(level_col)};
        const auto timestamp_bucket_data {duckdb::FlatVector::GetData<duckdb::timestamp_t>(timestamp_bucket_col)};
        const auto count_data {duckdb::FlatVector::GetData<std::int64_t>(count_col)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            auto level {level_data[row]};
            auto timestamp_bucket {timestamp_bucket_data[row]};
            auto count {count_data[row]};

            level_distribution[level.GetString()].first.push_back(timestamp_bucket.value / 1000000);
            level_distribution[level.GetString()].second.push_back(count);
        }
    }

    return level_distribution;
}



}    // namespace logtt
