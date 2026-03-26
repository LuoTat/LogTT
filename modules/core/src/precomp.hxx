#pragma once

#include <string>
#include <vector>

namespace logtt
{

using Mask     = std::pair<std::string, std::string>;
using Token    = std::string;
using TContent = std::vector<Token>;

inline constexpr std::string WILDCARD {"<#*#>"};

}    // namespace logtt