#include "utils.hxx"
#include <format>
#include <ranges>

namespace logparser
{

duckdb::unique_ptr<duckdb::MaterializedQueryResult> to_materialized_query_result(duckdb::unique_ptr<duckdb::QueryResult> result)
{
    D_ASSERT(result->type == duckdb::QueryResultType::MATERIALIZED_RESULT);
    return duckdb::unique_ptr_cast<duckdb::QueryResult, duckdb::MaterializedQueryResult>(std::move(result));
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

    auto named_fields_str {
        named_fields |
        std::views::transform(
            [](const std::string& s)
            {
                return "'" + s + "'";
            }
        ) |
        std::views::join_with(',') |
        std::ranges::to<std::string>()
    };

    // 使用正则表达式提取日志字段，并将其展开成多行
    rel = rel->Project({"LineID", std::format("regexp_extract(_raw, '{}', [{}]) AS _cap", log_regex, named_fields_str)});
    rel = rel->Project(duckdb::vector<duckdb::string> {"LineID", "unnest(_cap)"});

    return rel;
}

duckdb::shared_ptr<duckdb::Relation> mask_log_rel(duckdb::shared_ptr<duckdb::Relation> rel, const std::vector<Mask>& maskings)
{
    // 对每个正则表达式，将其替换为 replacement
    std::string expr {"Content"};
    for (const auto& [regex, replacement] : maskings)
    {
        expr = std::format("regexp_replace({}, '{}', '{}', 'g')", expr, regex, replacement);
    }
    rel = rel->Project(std::format("*, {} AS MaskedContent", expr));

    return rel;
}

duckdb::shared_ptr<duckdb::Relation> split_log_rel(duckdb::shared_ptr<duckdb::Relation> rel, const std::vector<char>& delimiters)
{
    // 对每个分隔符，将其替换为 分隔符+空格
    std::string expr {"MaskedContent"};
    for (auto delim : delimiters)
    {
        expr = std::format("replace({}, '{}', '{} ')", expr, delim, delim);
    }
    // 按空格分割
    expr = std::format("str_split({}, ' ')", expr);
    rel  = rel->Project(std::format("*, {} AS Tokens", expr));

    return rel;
}

void to_table(
    duckdb::Connection&                  conn,
    duckdb::shared_ptr<duckdb::Relation> rel,
    const std::vector<std::string>&      templates,
    const std::string&                   structured_table_name,
    const std::string&                   templates_table_name,
    bool                                 keep_para
)
{
    // 使用 UDF 将 log_templates 中的模板字符串映射到每一行
    auto udf {
        [&templates](duckdb::DataChunk& args, duckdb::ExpressionState& state, duckdb::Vector& result)
        {
            result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
            auto result_data {duckdb::FlatVector::GetData<duckdb::string_t>(result)};

            const auto line_id_data {duckdb::FlatVector::GetData<int64_t>(args.data[0])};
            for (auto i : std::views::iota(0UL, args.size()))
            {
                result_data[i] = duckdb::StringVector::AddString(result, templates[line_id_data[i] - 1]);
            }
        }
    };
    conn.CreateVectorizedFunction<duckdb::string_t, std::int64_t>("_get_template", udf);
    rel = rel->Project("*, _get_template(LineID) AS Template");
    rel->Create(structured_table_name);
    rel = conn.Table(structured_table_name);

    // 统计每个模板的出现次数，并按出现次数降序排序
    rel = rel->Project("Template");
    rel = rel->Aggregate("Template, COUNT(*) AS Count", "Template");
    rel = rel->Order("Count DESC");
    rel->Create(templates_table_name);
}

}    // namespace logparser