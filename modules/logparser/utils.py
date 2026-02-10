from collections.abc import Callable
from pathlib import Path

import polars as pl
import regex as re


def _get_parameter_list(row: dict[str, str]) -> list[str]:
    """Extract parameter list from a log row based on its event template"""
    template_regex = re.sub("<.{1,5}>", "<*>", row["EventTemplate"])
    if "<*>" not in template_regex:
        return []
    template_regex = re.sub("([^A-Za-z0-9])", r"\\\1", template_regex)
    template_regex = re.sub(r"\\ +", r"\\s+", template_regex)
    template_regex = "^" + template_regex.replace(r"\<\*\>", "(.*?)") + "$"
    parameter_list = re.findall(template_regex, row["Content"])
    parameter_list = parameter_list[0] if parameter_list else ()
    parameter_list = list(parameter_list) if isinstance(parameter_list, tuple) else [parameter_list]
    return parameter_list


def _log_to_dataframe(
    log_file: Path, headers: list[str], format_regex: re.Pattern, should_stop: Callable[[], bool]
) -> pl.DataFrame:
    """Function to transform log file to dataframe"""
    log_messages = {h: [] for h in headers}
    linecount = 0
    with open(log_file, "r") as fin:
        for line in fin:
            if should_stop():
                raise InterruptedError
            try:
                match = format_regex.search(line.strip())
                for header in headers:
                    log_messages[header].append(match.group(header))
                linecount += 1
            except Exception as e:
                print(f"[Warning] Skip line: {line}")
                print(e)
    logdf = pl.DataFrame(log_messages)
    logdf = logdf.with_row_index("LineId", offset=1)
    print(f"Total lines: {logdf.height}")
    return logdf


def _generate_logformat_regex(log_format: str) -> tuple[list[str], re.Pattern]:
    """Function to generate regular expression to split log messages"""
    headers = []
    splitters = re.split("(<[^<>]+>)", log_format)
    regex = ""
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(" +", r"\\s+", splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip("<").strip(">")
            regex += "(?P<%s>.*?)" % header
            headers.append(header)
    regex = re.compile("^" + regex + "$")
    return headers, regex


def load_data(log_file: Path, log_format: str, regex: list[str], should_stop: Callable[[], bool]) -> pl.DataFrame:
    """Load and preprocess log data into a Polars DataFrame"""
    headers, format_regex = _generate_logformat_regex(log_format)
    df_log = _log_to_dataframe(log_file, headers, format_regex, should_stop)
    # 使用 Polars 原生字符串操作批量预处理，利用 Rust 多核并行加速
    content_col = pl.col("Content")
    for rex in regex:
        content_col = content_col.str.replace_all(rex, "<*>")
    return df_log.with_columns(content_col)


def output_result(
    df_log: pl.DataFrame,
    log_templates: list[str],
    output_dir: Path,
    log_structured_file: Path,
    log_templates_file: Path,
    keep_para: bool,
) -> None:
    """Output structured log file and log templates file"""
    output_dir.mkdir(parents=True, exist_ok=True)

    df_log = df_log.with_columns(
        pl.Series("EventTemplate", log_templates),
    )
    if keep_para:
        df_log = df_log.with_columns(
            pl.struct(["EventTemplate", "Content"])
            .map_elements(_get_parameter_list, return_dtype=pl.List(pl.String))
            .alias("ParameterList")
        )
    df_log.write_csv(log_structured_file)

    df_log["EventTemplate"].value_counts().rename({"count": "Occurrences"}).select(
        ["EventTemplate", "Occurrences"]
    ).sort("Occurrences", descending=True).write_csv(log_templates_file)
