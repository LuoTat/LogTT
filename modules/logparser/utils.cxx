#include "utils.hxx"

namespace logparser
{

duckdb::shared_ptr<duckdb::Relation> load_data(const std::string& log_file, const std::string& log_format, const std::vector<std::string>& group_names)
{
    duckdb::DuckDB                db {nullptr};
    duckdb::Connection            con {db};
    duckdb::named_parameter_map_t params {
        {"all_varchar", true},
    };

    return con.ReadCSV(log_file, std::move(params));
}

}    // namespace logparser