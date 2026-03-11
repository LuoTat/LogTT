#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace logparser
{

duckdb::shared_ptr<duckdb::Relation> load_data(const std::string& log_file, const std::string& log_format, const std::vector<std::string>& group_names);

}    // namespace logparser