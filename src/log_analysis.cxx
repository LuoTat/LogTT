#include "log_analysis.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"

namespace logtt
{

std::pair<std::vector<std::string>, std::vector<std::int64_t>>
get_level_distribution(const std::string& structured_table_name)
{
    auto& conn {get_connection()};

    auto func_expr {make_uniq<FunctionExpression>("count", ParsedExprVec {})};

    ParsedExprVec project_exprs;
    project_exprs.push_back(make_uniq<ColumnRefExpression>("Level"));
    project_exprs.push_back(std::move(func_expr));

    auto rel {conn.Table(structured_table_name)->Aggregate(std::move(project_exprs), "Level")->Order("Level")};

    std::pair<std::vector<std::string>, std::vector<std::int64_t>> distribution;
    auto                                                           result {to_m_result(rel->Execute())};
    result->Print();
    for (auto&& i : std::views::iota(0UL, result->RowCount()))
    {
        auto level {result->GetValue(0, i)};
        auto count {result->GetValue<std::uint32_t>(1, i)};
        distribution.first.push_back(level.ToString());
        distribution.second.push_back(count);
    }

    return distribution;
}

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> get_log_frequency_distribution(
    const std::string& structured_table_name, std::int32_t months, std::int32_t days, std::int64_t micros
)
{
    auto& conn {get_connection()};

    ParsedExprVec arg_expr;
    arg_expr.push_back(make_uniq<ConstantExpression>(Value::INTERVAL(months, days, micros)));
    arg_expr.push_back(make_uniq<ColumnRefExpression>("Timestamp"));

    auto func_expr_1 {make_uniq<FunctionExpression>("time_bucket", std::move(arg_expr))};
    func_expr_1->SetAlias("Timestamp_bucket");

    auto func_expr_2 {make_uniq<FunctionExpression>("count", ParsedExprVec {})};

    ParsedExprVec project_exprs;
    project_exprs.push_back(std::move(func_expr_1));
    project_exprs.push_back(std::move(func_expr_2));

    auto rel {conn.Table(structured_table_name)
                  ->Aggregate(std::move(project_exprs), "Timestamp_bucket")
                  ->Order("Timestamp_bucket")};

    auto                                                            result {to_m_result(rel->Execute())};
    std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> distribution;
    distribution.first.reserve(result->RowCount());
    distribution.second.reserve(result->RowCount());
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& timestamp_bucket_col {data_chunk.data[0]};
        const auto& count_col {data_chunk.data[1]};

        const auto timestamp_bucket_data {FlatVector::GetData<timestamp_t>(timestamp_bucket_col)};
        const auto count_data {FlatVector::GetData<std::int64_t>(count_col)};

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

std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> get_template_frequency_distribution(
    const std::string& structured_table_name, std::int32_t months, std::int32_t days, std::int64_t micros
)
{
    auto& conn {get_connection()};

    ParsedExprVec arg_expr_1;
    arg_expr_1.push_back(make_uniq<ConstantExpression>(Value::INTERVAL(months, days, micros)));
    arg_expr_1.push_back(make_uniq<ColumnRefExpression>("Timestamp"));

    auto func_expr_1 {make_uniq<FunctionExpression>("time_bucket", std::move(arg_expr_1))};
    func_expr_1->SetAlias("Timestamp_bucket");

    ParsedExprVec arg_expr_2;
    arg_expr_2.push_back(make_uniq<ColumnRefExpression>("Template"));

    auto func_expr_2 {make_uniq<FunctionExpression>("count", std::move(arg_expr_2))};
    func_expr_2->distinct = true;

    ParsedExprVec project_exprs;
    project_exprs.push_back(std::move(func_expr_1));
    project_exprs.push_back(std::move(func_expr_2));

    auto rel {conn.Table(structured_table_name)
                  ->Aggregate(std::move(project_exprs), "Timestamp_bucket")
                  ->Order("Timestamp_bucket")};

    auto                                                            result {to_m_result(rel->Execute())};
    std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>> distribution;
    distribution.first.reserve(result->RowCount());
    distribution.second.reserve(result->RowCount());
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& timestamp_bucket_col {data_chunk.data[0]};
        const auto& count_col {data_chunk.data[1]};

        const auto timestamp_bucket_data {FlatVector::GetData<timestamp_t>(timestamp_bucket_col)};
        const auto count_data {FlatVector::GetData<std::int64_t>(count_col)};

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

std::unordered_map<std::string, std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>>>
get_log_level_frequency_distribution(
    const std::string& structured_table_name, std::int32_t months, std::int32_t days, std::int64_t micros
)
{
    auto& conn {get_connection()};

    ParsedExprVec arg_expr;
    arg_expr.push_back(make_uniq<ConstantExpression>(Value::INTERVAL(months, days, micros)));
    arg_expr.push_back(make_uniq<ColumnRefExpression>("Timestamp"));

    auto func_expr_1 {make_uniq<FunctionExpression>("time_bucket", std::move(arg_expr))};
    func_expr_1->SetAlias("Timestamp_bucket");

    auto func_expr_2 {make_uniq<FunctionExpression>("count", ParsedExprVec {})};

    ParsedExprVec project_exprs;
    project_exprs.push_back(make_uniq<ColumnRefExpression>("Level"));
    project_exprs.push_back(std::move(func_expr_1));
    project_exprs.push_back(std::move(func_expr_2));

    auto rel {conn.Table(structured_table_name)
                  ->Aggregate(std::move(project_exprs), "Timestamp_bucket, Level")
                  ->Order("Timestamp_bucket")};

    auto result {to_m_result(rel->Execute())};
    std::unordered_map<std::string, std::pair<std::vector<std::int64_t>, std::vector<std::int64_t>>> level_distribution;
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& level_col {data_chunk.data[0]};
        const auto& timestamp_bucket_col {data_chunk.data[1]};
        const auto& count_col {data_chunk.data[2]};

        const auto level_data {FlatVector::GetData<string_t>(level_col)};
        const auto timestamp_bucket_data {FlatVector::GetData<timestamp_t>(timestamp_bucket_col)};
        const auto count_data {FlatVector::GetData<std::int64_t>(count_col)};

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

std::pair<std::int64_t, std::vector<std::vector<std::int64_t>>>
get_template_transition_matrix(const std::string& structured_table_name, const std::string& template_table_name)
{
    auto& conn {get_connection()};
    auto  s_rel {conn.Table(structured_table_name)};
    auto  t_rel {conn.Table(template_table_name)};

    auto template_count {get_row_count(t_rel)};

    ParsedExprVec arg_exprs;
    arg_exprs.push_back(
        make_uniq<ComparisonExpression>(
            ExpressionType::COMPARE_EQUAL,
            make_uniq<ColumnRefExpression>("Template", structured_table_name),
            make_uniq<ColumnRefExpression>("Template", template_table_name)
        )
    );

    auto col_expr {make_uniq<ColumnRefExpression>("rowid", template_table_name)};
    col_expr->SetAlias("curr_id");

    auto window_expr {make_uniq<WindowExpression>(ExpressionType::WINDOW_LEAD, "", "", "lead")};
    window_expr->children.push_back(make_uniq<ColumnRefExpression>("rowid", template_table_name));
    window_expr->default_expr = make_uniq<ConstantExpression>(Value::BIGINT(-1));
    window_expr->start        = WindowBoundary::UNBOUNDED_PRECEDING;
    window_expr->end          = WindowBoundary::UNBOUNDED_FOLLOWING;
    window_expr->SetAlias("next_id");

    ParsedExprVec project_exprs_1;
    project_exprs_1.push_back(std::move(col_expr));
    project_exprs_1.push_back(std::move(window_expr));

    auto func_expr {make_uniq<FunctionExpression>("count", ParsedExprVec {})};

    ParsedExprVec project_exprs_2;
    project_exprs_2.push_back(make_uniq<ColumnRefExpression>("curr_id"));
    project_exprs_2.push_back(make_uniq<ColumnRefExpression>("next_id"));
    project_exprs_2.push_back(std::move(func_expr));

    auto rel {s_rel->Join(t_rel, std::move(arg_exprs))
                  ->Project(std::move(project_exprs_1), {})
                  ->Filter(
                      make_uniq<ComparisonExpression>(
                          ExpressionType::COMPARE_NOTEQUAL,
                          make_uniq<ColumnRefExpression>("next_id"),
                          make_uniq<ConstantExpression>(Value::BIGINT(-1))
                      )
                  )
                  ->Aggregate(std::move(project_exprs_2), "curr_id, next_id")};

    auto                              result {to_m_result(rel->Execute())};
    std::vector<std::vector<int64_t>> transition_counts;
    transition_counts.reserve(result->RowCount());
    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& curr_id_col {data_chunk.data[0]};
        const auto& next_id_col {data_chunk.data[1]};
        const auto& count_col {data_chunk.data[2]};

        const auto curr_id_data {FlatVector::GetData<std::int64_t>(curr_id_col)};
        const auto next_id_data {FlatVector::GetData<std::int64_t>(next_id_col)};
        const auto count_data {FlatVector::GetData<std::int64_t>(count_col)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            auto curr_id {curr_id_data[row]};
            auto next_id {next_id_data[row]};
            auto count {count_data[row]};
            transition_counts.push_back({curr_id, next_id, count});
        }
    }

    return {template_count, transition_counts};
}

}    // namespace logtt
