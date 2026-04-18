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
    auto&                                                        conn {get_connection()};
    auto                                                         rel {conn.Table(structured_table_name)};
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

}    // namespace logtt
