#include "base_log_parser.hxx"

namespace logtt
{

BaseLogParser::BaseLogParser(std::string log_regex, std::vector<std::string> named_fields, std::vector<std::string> timestamp_fields, std::string timestamp_format, std::vector<Mask> maskings, std::vector<char> delimiters):
    m_log_regex {std::move(log_regex)}, m_named_fields {std::move(named_fields)}, m_timestamp_fields {std::move(timestamp_fields)}, m_timestamp_format {std::move(timestamp_format)}, m_maskings {std::move(maskings)}, m_delimiters {std::move(delimiters)}
{}

}    // namespace logtt
