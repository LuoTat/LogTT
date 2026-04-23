#pragma once

#include "precomp.hxx"
#include <string>
#include <vector>

namespace logtt
{

unique_ptr<MaterializedQueryResult> to_m_result(unique_ptr<QueryResult> result);
shared_ptr<Relation>                get_tmp(Connection& conn, const shared_ptr<Relation>& rel);
std::int64_t                        get_row_count(const shared_ptr<Relation>& rel);

shared_ptr<Relation> load_data(
    Connection&                     conn,
    const std::string&              log_file,
    const std::string&              log_regex,
    const std::vector<std::string>& named_fields,
    const std::vector<std::string>& timestamp_fields,
    const std::string&              timestamp_format
);
shared_ptr<Relation> mask_log_rel(shared_ptr<Relation>& rel, const std::vector<Mask>& maskings);
shared_ptr<Relation> split_log_rel(shared_ptr<Relation>& rel, const std::vector<char>& delimiters);

void to_table(
    Connection&                     conn,
    shared_ptr<Relation>&           rel,
    const std::vector<std::string>& templates,
    const std::string&              structured_table_name,
    const std::string&              templates_table_name,
    bool                            keep_para
);

}    // namespace logtt