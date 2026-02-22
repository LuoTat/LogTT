# distutils: language=c++

cdef object fp
cdef object pl
cdef object re
cdef object DuckDBService

import formatparse as fp
import polars as pl
import regex as re

from modules.duckdb_service import DuckDBService

cdef object mask_log_df(object log_df, vector[pair[string, string]]& maskings):
    """Mask the DataFrame"""
    cdef pair[string, string] masking
    cdef string regex
    cdef string replacement
    cdef object content_col = pl.col("Content")
    for masking in maskings:
        regex = masking.first
        replacement = masking.second
        content_col = content_col.str.replace_all(regex, replacement)
    return log_df.with_columns(content_col.alias("MaskedContent"))

cdef string mask_log(string log, vector[pair[string, string]]& maskings):
    """Mask the log"""
    cdef pair[string, string] masking
    cdef string regex
    cdef string replacement
    for masking in maskings:
        regex = masking.first
        replacement = masking.second
        log = re.sub(regex, replacement, log)
    return log

cdef object split_log_df(object log_df, vector[string]& delimiters):
    """Split the DataFrame"""
    cdef string delim
    cdef object content_col = pl.col("MaskedContent")
    for delim in delimiters:
        content_col = content_col.str.replace_all(delim, f"{delim} ", literal=True)
    # 分割后过滤掉空字符串
    content_col = content_col.str.split(" ").list.filter(pl.element() != "")
    return log_df.with_columns(content_col.alias("Tokens"))

cdef Content split_log(string log, vector[string]& delimiters):
    """Split the log"""
    cdef string delim
    cdef str log_str = log
    for delim in delimiters:
        log_str = log_str.replace(delim, f"{delim} ")
    return log_str.split()

# TODO: 完成模板参数提取函数
# cdef vector[string] _get_parameter_list(row: dict[str, str]):
#     """Extract parameter list from a log row based on its event template"""
#     template_regex = re.sub("<.{1,5}>", "<#*#>", row["EventTemplate"])
#     if "<#*#>" not in template_regex:
#         return []
#     template_regex = re.sub("([^A-Za-z0-9])", r"\\\1", template_regex)
#     template_regex = re.sub(r"\\ +", r"\\s+", template_regex)
#     template_regex = "^" + template_regex.replace(r"\<\*\>", "(.*?)") + "$"
#     parameter_list = re.findall(template_regex, row["Content"])
#     parameter_list = parameter_list[0] if parameter_list else ()
#     parameter_list = (
#         list(parameter_list) if isinstance(parameter_list, tuple) else [parameter_list]
#     )
#     return parameter_list

cdef to_table(
    object log_df,
    vector[string]& log_templates,
    string structured_table_name,
    string templates_table_name,
    bint keep_para,
):
    """Output structured log data and templates to DuckDB tables"""
    log_df = log_df.drop("MaskedContent")
    log_df = log_df.drop("Tokens")
    log_df = log_df.with_columns(pl.Series("Template", log_templates))
    # if keep_para:
    #     log_df = log_df.with_columns(
    #         pl.struct(["EventTemplate", "Content"])
    #         .map_elements(_get_parameter_list, return_dtype=pl.List(pl.String))
    #         .alias("ParameterList")
    #     )
    cdef object template_df = (
        log_df["Template"]
        .value_counts(sort=True, parallel=True, name="Count")
        .select(["Template", "Count"])
    )

    DuckDBService.create_table_from_polars(log_df, structured_table_name)
    DuckDBService.create_table_from_polars(template_df, templates_table_name)

cdef object load_data(string log_file, string log_format):
    """Load log data into a Polars LazyFrame"""

    cdef str regex = "^" + fp.compile(log_format)._expression + "$"

    cdef object log_df = (
        pl.scan_lines(log_file, name="raw", row_index_name="LineId", row_index_offset=1)
        .with_columns(pl.col("raw").str.extract_groups(regex).alias("_caps"))
        .unnest("_caps")
    )

    return log_df
