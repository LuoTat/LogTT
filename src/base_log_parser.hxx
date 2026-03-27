#pragma once

#include "precomp.hxx"
#include <cstdint>

namespace logtt
{

class BaseLogParser
{
public:
    BaseLogParser() = default;
    BaseLogParser(std::string log_regex, std::vector<std::string> named_fields, std::vector<Mask> maskings, std::vector<char> delimiters);

    // 返回解析的日志条数
    virtual std::uint32_t parse(const std::string& log_file, const std::string& structured_table_name, const std::string& templates_table_name, bool keep_para) = 0;

    std::string              m_log_regex;
    std::vector<std::string> m_named_fields;
    std::vector<Mask>        m_maskings;
    std::vector<char>        m_delimiters;
};

}    // namespace logtt