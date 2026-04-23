#pragma once

#include "duckdb.hpp"
#include <string>
#include <vector>

namespace logtt
{

using Mask     = std::pair<std::string, std::string>;
using Token    = std::string;
using TContent = std::vector<Token>;

inline constexpr std::string WILDCARD {"<#*#>"};

using namespace duckdb;
using ParsedExprVec = vector<unique_ptr<ParsedExpression>>;

}    // namespace logtt