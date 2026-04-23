#include "ael_log_parser.hxx"
#include "duckdb_service.hxx"
#include "utils.hxx"
#include <ranges>

namespace logtt
{

AELLogParser::AELLogParser(
    std::string              log_regex,
    std::vector<std::string> named_fields,
    std::vector<std::string> timestamp_fields,
    std::string              timestamp_format,
    std::vector<Mask>        maskings,
    std::vector<char>        delimiters,
    std::uint32_t            cluster_thr,
    float                    merge_thr
):
    BaseLogParser {std::move(log_regex), std::move(named_fields), std::move(timestamp_fields), std::move(timestamp_format), std::move(maskings), std::move(delimiters)},
    m_cluster_thr {cluster_thr},
    m_merge_thr {merge_thr}
{}

std::uint32_t AELLogParser::parse(const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name, bool keep_para)
{
    // 初始化日志簇池
    this->m_cluster_pool.clear();
    std::vector<std::string> templates;
    // 获取数据库连接
    auto& conn {get_connection()};

    auto rel {load_data(conn, log_file, this->m_log_regex, this->m_named_fields, this->m_timestamp_fields, this->m_timestamp_format)};
    rel = mask_log_rel(rel, this->m_maskings);
    rel = split_log_rel(rel, this->m_delimiters);

    // 缓存分词结果，避免重复计算
    rel = get_tmp(conn, rel);

    auto log_length {get_row_count(rel)};

    auto log_bin {this->_get_log_bins(rel)};
    auto merged_clusters {this->_reconcile(log_bin)};

    templates.resize(log_length);
    for (auto&& cluster : merged_clusters)
    {
        auto log_template {cluster->get_template()};

        for (auto&& row : cluster->rows)
        {
            templates[row - 1] = log_template;
        }
    }

    // 移除多余列
    auto star_expr {duckdb::make_uniq<duckdb::StarExpression>()};
    star_expr->exclude_list.emplace("MaskedContent");
    star_expr->exclude_list.emplace("Tokens");
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs;
    project_exprs.push_back(std::move(star_expr));
    rel = rel->Project(std::move(project_exprs), {});
    to_table(conn, rel, templates, structured_table_name, templates_table_name, keep_para);

    return log_length;
}

AELLogParser::LogBin AELLogParser::_get_log_bins(duckdb::shared_ptr<duckdb::Relation> rel)
{
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_1;
    arg_exprs_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Tokens"));
    auto func_expr_1 {duckdb::make_uniq<duckdb::FunctionExpression>("length", std::move(arg_exprs_1))};
    auto cast_expr_1 {duckdb::make_uniq<duckdb::CastExpression>(duckdb::LogicalType::UTINYINT, std::move(func_expr_1))};
    cast_expr_1->SetAlias("token_count");

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_2;
    arg_exprs_2.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("MaskedContent"));
    arg_exprs_2.push_back(duckdb::make_uniq<duckdb::ConstantExpression>("<#.*#>"));
    auto func_expr_2 {duckdb::make_uniq<duckdb::FunctionExpression>("regexp_extract_all", std::move(arg_exprs_2))};

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_3;
    arg_exprs_3.push_back(std::move(func_expr_2));
    auto func_expr_3 {duckdb::make_uniq<duckdb::FunctionExpression>("length", std::move(arg_exprs_3))};
    auto cast_expr_2 {duckdb::make_uniq<duckdb::CastExpression>(duckdb::LogicalType::UTINYINT, std::move(func_expr_3))};
    cast_expr_2->SetAlias("para_count");

    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> project_exprs_1;
    project_exprs_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("LineID"));
    project_exprs_1.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Tokens"));
    project_exprs_1.push_back(std::move(cast_expr_1));
    project_exprs_1.push_back(std::move(cast_expr_2));

    rel = rel->Project(std::move(project_exprs_1), {});
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> arg_exprs_4;
    arg_exprs_4.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("LineID"));
    auto                                                         func_expr_4 {duckdb::make_uniq<duckdb::FunctionExpression>("list", std::move(arg_exprs_4))};
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> agg_exprs;
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("token_count"));
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("para_count"));
    agg_exprs.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>("Tokens"));
    agg_exprs.push_back(std::move(func_expr_4));
    rel = rel->Aggregate(std::move(agg_exprs), "token_count, para_count, Tokens");

    auto   result {to_m_result(rel->Execute())};
    LogBin log_bin;

    for (auto&& data_chunk : result->Collection().Chunks())
    {
        const auto& token_count_col {data_chunk.data[0]};
        const auto& para_count_col {data_chunk.data[1]};
        const auto& tokens_col {data_chunk.data[2]};
        const auto& tokens_child {duckdb::ListVector::GetEntry(tokens_col)};
        const auto& line_ids_col {data_chunk.data[3]};
        const auto& line_ids_child {duckdb::ListVector::GetEntry(line_ids_col)};

        const auto token_count_data {duckdb::FlatVector::GetData<std::uint8_t>(token_count_col)};
        const auto para_count_data {duckdb::FlatVector::GetData<std::uint8_t>(para_count_col)};
        const auto tokens_data {duckdb::FlatVector::GetData<duckdb::list_entry_t>(tokens_col)};
        const auto tokens_child_data {duckdb::FlatVector::GetData<duckdb::string_t>(tokens_child)};
        const auto line_ids_data {duckdb::FlatVector::GetData<duckdb::list_entry_t>(line_ids_col)};
        const auto line_ids_child_data {duckdb::FlatVector::GetData<std::int64_t>(line_ids_child)};

        for (auto&& row : std::views::iota(0UL, data_chunk.size()))
        {
            TContent                   content;
            std::vector<std::uint32_t> rows;

            auto        token_count {token_count_data[row]};
            auto        para_count {para_count_data[row]};
            const auto& tokens_entry {tokens_data[row]};
            const auto& line_ids_entry {line_ids_data[row]};

            for (auto&& i : std::views::iota(0UL, tokens_entry.length))
            {
                const auto& token {tokens_child_data[tokens_entry.offset + i]};
                content.emplace_back(token.GetData(), token.GetSize());
            }
            for (auto&& i : std::views::iota(0UL, line_ids_entry.length))
            {
                auto line_id {line_ids_child_data[line_ids_entry.offset + i]};
                rows.push_back(line_id);
            }

            this->m_cluster_pool.emplace_back(content, rows);
            auto cluster {&this->m_cluster_pool.back()};

            auto [it, inserted] {log_bin.try_emplace(LogBinKey {token_count, para_count}, 1, cluster)};
            if (!inserted)
            {
                it->second.push_back(cluster);
            }
        }
    }

    return log_bin;
}

std::vector<AELLogParser::LogCluster*> AELLogParser::_reconcile(LogBin& log_bin)
{
    std::vector<LogCluster*> merged_clusters;

    // 对于每个桶中的日志簇，计算它们之间的差异，并将相似的簇合并
    for (auto&& [_, clusters] : log_bin)
    {
        if (clusters.size() <= this->m_cluster_thr)
        {
            merged_clusters.insert(merged_clusters.end(), clusters.begin(), clusters.end());
            continue;
        }

        std::vector<std::vector<LogCluster*>> cluster_groups;
        for (auto&& i : std::views::iota(0UL, clusters.size()))
        {
            auto cluster1 {clusters[i]};
            if (cluster1->merged)
            {
                continue;
            }

            cluster1->merged = true;
            cluster_groups.emplace_back(1, cluster1);

            for (auto&& j : std::views::iota(i + 1, clusters.size()))
            {
                auto cluster2 {clusters[j]};
                if (cluster2->merged)
                {
                    continue;
                }

                if (this->_has_diff(cluster1, cluster2))
                {
                    cluster2->merged = true;
                    cluster_groups.back().push_back(cluster2);
                }
            }
        }

        for (auto&& group : cluster_groups)
        {
            auto merged_cluster {std::accumulate(group.begin() + 1, group.end(), group.front(), AELLogParser::_merge_log_cluster)};
            merged_clusters.push_back(merged_cluster);
        }
    }

    return merged_clusters;
}

bool AELLogParser::_has_diff(const LogCluster* cluster1, const LogCluster* cluster2)
{
    std::uint16_t diff {0};

    for (auto&& [token1, token2] : std::views::zip(cluster1->content, cluster2->content))
    {
        if (token1 != token2)
        {
            ++diff;
        }
    }

    auto ratio {static_cast<float>(diff) / cluster1->content.size()};
    return diff > 0 && ratio <= this->m_merge_thr;
}

AELLogParser::LogCluster* AELLogParser::_merge_log_cluster(LogCluster* cluster1, const LogCluster* cluster2)
{
    for (auto&& [token1, token2] : std::views::zip(cluster1->content, cluster2->content))
    {
        if (token1 != token2)
        {
            token1 = WILDCARD;
        }
    }

    cluster1->rows.insert(cluster1->rows.end(), cluster2->rows.begin(), cluster2->rows.end());
    return cluster1;
}

std::string AELLogParser::LogCluster::get_template() const
{
    return this->content |
           std::views::join_with(' ') |
           std::ranges::to<std::string>();
}

}    // namespace logtt
