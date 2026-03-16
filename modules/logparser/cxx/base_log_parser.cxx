#include "base_log_parser.hxx"

namespace logparser
{

BaseLogParser::BaseLogParser(std::string log_regex, std::vector<std::string> named_fields, std::vector<Mask> maskings, std::vector<char> delimiters):
    m_log_regex {std::move(log_regex)}, m_named_fields {std::move(named_fields)}, m_maskings {std::move(maskings)}, m_delimiters {std::move(delimiters)}
{}

}    // namespace logparser
