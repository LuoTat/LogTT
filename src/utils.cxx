#include "utils.hxx"
#include <format>
#include <ranges>

namespace logtt
{

static unique_ptr<ParsedExpression>
_build_timestamp_expr(const std::vector<std::string>& timestamp_fields, const std::string& timestamp_format)
{
    unique_ptr<ParsedExpression> func_expr;
    if (timestamp_format == "epoch")
    {
        // Unix 时间戳
        ParsedExprVec arg_exprs_1;
        arg_exprs_1.push_back(
            make_uniq<CastExpression>(LogicalType::BIGINT, make_uniq<ColumnRefExpression>(timestamp_fields[0]))
        );
        arg_exprs_1.push_back(make_uniq<ConstantExpression>(Value::BIGINT(1000)));

        auto timestamp_ms {make_uniq<FunctionExpression>("multiply", std::move(arg_exprs_1))};

        ParsedExprVec arg_exprs_2;
        arg_exprs_2.push_back(std::move(timestamp_ms));

        func_expr = make_uniq<FunctionExpression>("make_timestamp_ms", std::move(arg_exprs_2));
    }
    else
    {
        // 拼接字段
        unique_ptr<ParsedExpression> arg_exprs_1;
        if (timestamp_fields.size() == 1)
        {
            arg_exprs_1 = make_uniq<ColumnRefExpression>(timestamp_fields[0]);
        }
        else
        {
            ParsedExprVec arg_exprs_2;
            arg_exprs_2.push_back(make_uniq<ConstantExpression>(Value(" ")));
            for (auto&& field : timestamp_fields)
            {
                arg_exprs_2.push_back(make_uniq<ColumnRefExpression>(field));
            }
            arg_exprs_1 = make_uniq<FunctionExpression>("concat_ws", std::move(arg_exprs_2));
        }

        // strptime 解析
        ParsedExprVec arg_exprs_2;
        arg_exprs_2.push_back(std::move(arg_exprs_1));
        arg_exprs_2.push_back(make_uniq<ConstantExpression>(Value(timestamp_format)));

        func_expr = make_uniq<FunctionExpression>("strptime", std::move(arg_exprs_2));
    }

    // 转为 TIMESTAMP_S 类型
    return make_uniq<CastExpression>(LogicalType::TIMESTAMP_S, std::move(func_expr));
}

unique_ptr<MaterializedQueryResult> to_m_result(unique_ptr<QueryResult> result)
{
    D_ASSERT(result->type == QueryResultType::MATERIALIZED_RESULT);
    return unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
}

shared_ptr<Relation> get_tmp(Connection& conn, const shared_ptr<Relation>& rel)
{
    rel->Create("_tmp", true, OnCreateConflict::REPLACE_ON_CONFLICT);
    return conn.Table("_tmp");
}

std::int64_t get_row_count(const shared_ptr<Relation>& rel)
{
    ParsedExprVec project_exprs;
    project_exprs.push_back(make_uniq<FunctionExpression>("count", ParsedExprVec {}));

    return to_m_result(rel->Aggregate(std::move(project_exprs))->Execute())->GetValue<std::int64_t>(0, 0);
}

shared_ptr<Relation> load_data(
    Connection&                     conn,
    const std::string&              log_file,
    const std::string&              log_regex,
    const std::vector<std::string>& named_fields,
    const std::vector<std::string>& timestamp_fields,
    const std::string&              timestamp_format
)
{
    // 将日志文件作为CSV文件读取，使用特殊的分隔符和引号来避免解析错误
    // 使用 WITH ORDINALITY 生成一个从 1 开始的序列，作为日志行号
    auto rel {conn.RelationFromQuery(
        std::format(
            R"(
            SELECT ordinality AS LineID, _raw
            FROM read_csv(
                '{}',
                columns={{'_raw':'VARCHAR'}},
                delim=chr(1),
                quote='',
                escape='',
                new_line='\n'
            )
            WITH ORDINALITY
        )",
            log_file
        )
    )};

    auto named_fields_values {
        named_fields |
        std::views::transform(
            [](const std::string& s)
            {
                return Value(s);
            }
        ) |
        std::ranges::to<vector<Value>>()
    };

    // 使用正则表达式提取日志字段，并将其展开成多行
    ParsedExprVec arg_exprs_1;
    arg_exprs_1.push_back(make_uniq<ColumnRefExpression>("_raw"));
    arg_exprs_1.push_back(make_uniq<ConstantExpression>(log_regex));
    arg_exprs_1.push_back(make_uniq<ConstantExpression>(Value::LIST(named_fields_values)));

    auto func_expr_1 {make_uniq<FunctionExpression>("regexp_extract", std::move(arg_exprs_1))};
    func_expr_1->SetAlias("_cap");

    ParsedExprVec project_exprs_1;
    project_exprs_1.push_back(make_uniq<ColumnRefExpression>("LineID"));
    project_exprs_1.push_back(std::move(func_expr_1));


    ParsedExprVec arg_exprs_2;
    arg_exprs_2.push_back(make_uniq<ColumnRefExpression>("_cap"));

    ParsedExprVec project_exprs_2;
    project_exprs_2.push_back(make_uniq<ColumnRefExpression>("LineID"));
    project_exprs_2.push_back(make_uniq<FunctionExpression>("unnest", std::move(arg_exprs_2)));

    // 提取时间戳字段，并将其转换为 TIMESTAMP 类型
    // 同时过滤掉原始的时间戳字段，简化日志表的结构
    auto func_expr_2 {_build_timestamp_expr(timestamp_fields, timestamp_format)};
    func_expr_2->SetAlias("Timestamp");

    auto star_expr {make_uniq<StarExpression>()};
    star_expr->exclude_list.emplace("LineID");
    for (auto&& field : timestamp_fields)
    {
        star_expr->exclude_list.emplace(field);
    }

    ParsedExprVec project_exprs_3;
    project_exprs_3.push_back(make_uniq<ColumnRefExpression>("LineID"));
    project_exprs_3.push_back(std::move(func_expr_2));
    project_exprs_3.push_back(std::move(star_expr));

    return rel->Project(std::move(project_exprs_1), {})
        ->Project(std::move(project_exprs_2), {})
        ->Project(std::move(project_exprs_3), {});
}

shared_ptr<Relation> mask_log_rel(shared_ptr<Relation>& rel, const std::vector<Mask>& maskings)
{
    // 从 ColumnRefExpression("Content") 开始，逐层嵌套 regexp_replace
    unique_ptr<ParsedExpression> func_expr {make_uniq<ColumnRefExpression>("Content")};
    for (auto&& [regex, replacement] : maskings)
    {
        ParsedExprVec arg_exprs;
        arg_exprs.push_back(std::move(func_expr));
        arg_exprs.push_back(make_uniq<ConstantExpression>(Value(regex)));
        arg_exprs.push_back(make_uniq<ConstantExpression>(Value(replacement)));
        arg_exprs.push_back(make_uniq<ConstantExpression>(Value("g")));

        func_expr = make_uniq<FunctionExpression>("regexp_replace", std::move(arg_exprs));
    }

    ParsedExprVec project_exprs;
    project_exprs.push_back(make_uniq<StarExpression>());
    project_exprs.push_back(std::move(func_expr));

    return rel->Project(std::move(project_exprs), {"", "MaskedContent"});
}

shared_ptr<Relation> split_log_rel(shared_ptr<Relation>& rel, const std::vector<char>& delimiters)
{
    // 从 ColumnRefExpression("MaskedContent") 开始，逐层嵌套 replace
    unique_ptr<ParsedExpression> func_expr {make_uniq<ColumnRefExpression>("MaskedContent")};
    for (auto&& delim : delimiters)
    {
        ParsedExprVec arg_exprs;
        arg_exprs.push_back(std::move(func_expr));
        arg_exprs.push_back(make_uniq<ConstantExpression>(Value(std::string {delim})));
        arg_exprs.push_back(make_uniq<ConstantExpression>(Value(std::format("{} ", delim))));

        func_expr = make_uniq<FunctionExpression>("replace", std::move(arg_exprs));
    }

    // 在每个分隔符后添加一个空格，以便后续的 str_split 可以正确地将字段分开
    ParsedExprVec arg_exprs;
    arg_exprs.push_back(std::move(func_expr));
    arg_exprs.push_back(make_uniq<ConstantExpression>(Value(" ")));

    func_expr = make_uniq<FunctionExpression>("str_split", std::move(arg_exprs));

    ParsedExprVec project_exprs;
    project_exprs.push_back(make_uniq<StarExpression>());
    project_exprs.push_back(std::move(func_expr));

    return rel->Project(std::move(project_exprs), {"", "Tokens"});
}

void to_table(
    Connection&                     conn,
    shared_ptr<Relation>&           rel,
    const std::vector<std::string>& templates,
    const std::string&              structured_table_name,
    const std::string&              templates_table_name,
    bool                            keep_para
)
{
    // 使用 UDF 将 log_templates 中的模板字符串映射到每一行
    auto udf_name {std::format("_get_template_{}", structured_table_name)};
    auto udf {[&templates](DataChunk& args, ExpressionState& state, Vector& result)
              {
                  result.SetVectorType(VectorType::FLAT_VECTOR);
                  auto result_data {FlatVector::GetData<string_t>(result)};

                  const auto line_id_data {FlatVector::GetData<std::int64_t>(args.data[0])};
                  for (auto&& i : std::views::iota(0UL, args.size()))
                  {
                      result_data[i] = StringVector::AddString(result, templates[line_id_data[i] - 1]);
                  }
              }};
    conn.CreateVectorizedFunction<string_t, std::int64_t>(udf_name, udf);

    ParsedExprVec arg_exprs_1;
    arg_exprs_1.push_back(make_uniq<ColumnRefExpression>("LineID"));

    auto func_expr_1 {make_uniq<FunctionExpression>(udf_name, std::move(arg_exprs_1))};
    func_expr_1->SetAlias("Template");

    ParsedExprVec project_exprs_1;
    project_exprs_1.push_back(make_uniq<StarExpression>());
    project_exprs_1.push_back(std::move(func_expr_1));

    rel->Project(std::move(project_exprs_1), {})->Create(structured_table_name);

    // 统计每个模板的出现次数，并按出现次数降序排序
    auto func_expr_2 {make_uniq<FunctionExpression>("count", ParsedExprVec {})};
    func_expr_2->SetAlias("Count");

    ParsedExprVec project_exprs;
    project_exprs.push_back(make_uniq<ColumnRefExpression>("Template"));
    project_exprs.push_back(std::move(func_expr_2));

    vector<OrderByNode> order_exprs;
    order_exprs.emplace_back(
        OrderType::DESCENDING, OrderByNullType::ORDER_DEFAULT, make_uniq<ColumnRefExpression>("Count")
    );

    conn.Table(structured_table_name)
        ->Aggregate(std::move(project_exprs), "Template")
        ->Order(std::move(order_exprs))
        ->Create(templates_table_name);
}

}    // namespace logtt
