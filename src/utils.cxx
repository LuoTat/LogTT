#include "utils.hxx"
#include <format>
#include <ranges>

namespace logtt
{

duckdb::unique_ptr<duckdb::MaterializedQueryResult> to_materialized_query_result(duckdb::unique_ptr<duckdb::QueryResult> result)
{
    D_ASSERT(result->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
    return duckdb::unique_ptr_cast<duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));
}

duckdb::shared_ptr<duckdb::Relation> get_tmp(duckdb::Connection& conn, const duckdb::shared_ptr<duckdb::Relation>& rel)
{
    rel->Create("_tmp", true, duckdb::OnCreateConflict::REPLACE_ON_CONFLICT);
    return conn.Table("_tmp");
}

duckdb::shared_ptr<duckdb::Relation> load_data(duckdb::Connection& conn, const std::string& log_file, const std::string& log_regex, const std::vector<std::string>& named_fields)
{
    // 将日志文件作为CSV文件读取，使用特殊的分隔符和引号来避免解析错误
    // 使用 WITH ORDINALITY 生成一个从 1 开始的序列，作为日志行号
    auto rel {conn.RelationFromQuery(std::format(
        R"(
            SELECT ordinality AS LineID, _raw
            FROM read_csv(
                '{}',
                auto_detect=false,
                columns={{'_raw':'VARCHAR'}},
                delim=chr(0),
                quote=''
            )
            WITH ORDINALITY
        )",
        log_file
    ))};

    auto named_fields_values {
        named_fields |
        std::views::transform(
            [](const std::string& s)
            {
                return duckdb::Value(s);
            }
        ) |
        std::ranges::to<duckdb::vector<duckdb::Value>>()
    };

    // 使用正则表达式提取日志字段，并将其展开成多行
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_1;
    arg_exprs_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("_raw"));
    arg_exprs_1.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(log_regex));
    arg_exprs_1.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value::LIST(named_fields_values)));
    auto func_expr_1 {duckdb::make_uniq<duckdb::FunctionExpression>("regexp_extract", std::move(arg_exprs_1))};
    func_expr_1->SetAlias("_cap");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_1;
    project_exprs_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("LineID"));
    project_exprs_1.push_back(std::move(func_expr_1));
    rel = rel->Project(std::move(project_exprs_1), {});

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_2;
    arg_exprs_2.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("_cap"));
    auto                                                         func_expr_2 {duckdb::make_uniq<duckdb::FunctionExpression>("unnest", std::move(arg_exprs_2))};
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_2;
    project_exprs_2.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("LineID"));
    project_exprs_2.push_back(std::move(func_expr_2));
    rel = rel->Project(std::move(project_exprs_2), {});

    return rel;
}

duckdb::shared_ptr<duckdb::Relation> mask_log_rel(duckdb::shared_ptr<duckdb::Relation>& rel, const std::vector<Mask>& maskings)
{
    // 从 ColumnRefExpression("Content") 开始，逐层嵌套 regexp_replace
    duckdb::unique_ptr<duckdb::ParsedExpression> func_expr {duckdb::make_uniq<duckdb::ColumnRefExpression>("Content")};
    for (const auto& [regex, replacement] : maskings)
    {
        duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs;
        arg_exprs.push_back(std::move(func_expr));
        arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(regex)));
        arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(replacement)));
        arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value("g")));
        func_expr = duckdb::make_uniq<duckdb::FunctionExpression>("regexp_replace", std::move(arg_exprs));
    }

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs;
    project_exprs.push_back(duckdb::make_uniq<duckdb::StarExpression>());
    project_exprs.push_back(std::move(func_expr));
    rel = rel->Project(std::move(project_exprs), {"", "MaskedContent"});

    return rel;
}

duckdb::shared_ptr<duckdb::Relation> split_log_rel(duckdb::shared_ptr<duckdb::Relation>& rel, const std::vector<char>& delimiters)
{
    // 从 ColumnRefExpression("MaskedContent") 开始，逐层嵌套 replace
    duckdb::unique_ptr<duckdb::ParsedExpression> func_expr {duckdb::make_uniq<duckdb::ColumnRefExpression>("MaskedContent")};
    for (auto delim : delimiters)
    {
        duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs;
        arg_exprs.push_back(std::move(func_expr));
        arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(std::string {delim})));
        arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(std::format("{} ", delim))));
        func_expr = duckdb::make_uniq<duckdb::FunctionExpression>("replace", std::move(arg_exprs));
    }

    // 在每个分隔符后添加一个空格，以便后续的 str_split 可以正确地将字段分开
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs;
    arg_exprs.push_back(std::move(func_expr));
    arg_exprs.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value(" ")));
    func_expr = duckdb::make_uniq<duckdb::FunctionExpression>("str_split", std::move(arg_exprs));

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs;
    project_exprs.push_back(duckdb::make_uniq<duckdb::StarExpression>());
    project_exprs.push_back(std::move(func_expr));
    rel = rel->Project(std::move(project_exprs), {"", "Tokens"});

    return rel;
}

void to_table(
    duckdb::Connection&                   conn,
    duckdb::shared_ptr<duckdb::Relation>& rel,
    const std::vector<std::string>&       templates,
    const std::string&                    structured_table_name,
    const std::string&                    templates_table_name,
    bool                                  keep_para
)
{
    // 使用 UDF 将 log_templates 中的模板字符串映射到每一行
    auto udf_name {std::format("_get_template_{}", structured_table_name)};
    auto udf {
        [&templates](duckdb::DataChunk& args, duckdb::ExpressionState& state, duckdb::Vector& result)
        {
            result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
            auto result_data {duckdb::FlatVector::GetData<duckdb::string_t>(result)};

            const auto line_id_data {duckdb::FlatVector::GetData<std::int64_t>(args.data[0])};
            for (auto i : std::views::iota(0UL, args.size()))
            {
                result_data[i] = duckdb::StringVector::AddString(result, templates[line_id_data[i] - 1]);
            }
        }
    };
    conn.CreateVectorizedFunction<duckdb::string_t, std::int64_t>(udf_name, udf);

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_1;
    arg_exprs_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("LineID"));
    auto func_expr_1 {duckdb::make_uniq<duckdb::FunctionExpression>(udf_name, std::move(arg_exprs_1))};
    func_expr_1->SetAlias("Template");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_1;
    project_exprs_1.push_back(duckdb::make_uniq<duckdb::StarExpression>());
    project_exprs_1.push_back(std::move(func_expr_1));
    rel = rel->Project(std::move(project_exprs_1), {});
    rel->Create(structured_table_name);

    // 统计每个模板的出现次数，并按出现次数降序排序
    rel = conn.Table(structured_table_name);
    auto func_expr_2 {duckdb::make_uniq<duckdb::FunctionExpression>("count", duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> {})};
    func_expr_2->SetAlias("Count");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Template"));
    agg_exprs.push_back(std::move(func_expr_2));
    rel = rel->Aggregate(std::move(agg_exprs), "Template");

    duckdb::vector<duckdb::OrderByNode> order_bys;
    order_bys.emplace_back(
        duckdb::OrderType::DESCENDING,
        duckdb::OrderByNullType::ORDER_DEFAULT,
        duckdb::make_uniq<duckdb::ColumnRefExpression>("Count")
    );
    rel = rel->Order(std::move(order_bys));

    rel->Create(templates_table_name);
}

}    // namespace logtt