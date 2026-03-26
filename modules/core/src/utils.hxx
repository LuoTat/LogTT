#pragma once

#include "duckdb.hxx"
#include "precomp.hxx"
#include <string>
#include <vector>

namespace logtt
{

duckdb::unique_ptr<duckdb::MaterializedQueryResult> to_materialized_query_result(duckdb::unique_ptr<duckdb::QueryResult> result);
duckdb::shared_ptr<duckdb::Relation>                get_tmp(duckdb::Connection& conn, const duckdb::shared_ptr<duckdb::Relation>& rel);

duckdb::shared_ptr<duckdb::Relation> load_data(duckdb::Connection& conn, const std::string& log_file, const std::string& log_regex, const std::vector<std::string>& named_fields);
duckdb::shared_ptr<duckdb::Relation> mask_log_rel(duckdb::shared_ptr<duckdb::Relation>& rel, const std::vector<Mask>& maskings);
duckdb::shared_ptr<duckdb::Relation> split_log_rel(duckdb::shared_ptr<duckdb::Relation>& rel, const std::vector<char>& delimiters);

void to_table(
    duckdb::Connection&                   conn,
    duckdb::shared_ptr<duckdb::Relation>& rel,
    const std::vector<std::string>&       templates,
    const std::string&                    structured_table_name,
    const std::string&                    templates_table_name,
    bool                                  keep_para
);

}    // namespace logtt