from .log_parser_config import LogParserConfig

# 内置日志格式配置
BUILTIN_LOG_PARSER_CONFIGS: list[LogParserConfig] = [
    LogParserConfig(
        name="Android",
        log_format="{Date} {Time} {Pid:^} {Tid:>} {Level} {Component}: {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%m-%d %H:%M:%S.%g",
        user_maskings=[
            (
                r"(/[\w-]+)+",
                "<#PATH#>",
            ),
        ],
        delimiters="=:",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.6,
            },
            "BrainLogParser": {
                "var_thr": 5,
            },
            "DrainLogParser": {
                "depth": 6,
                "sim_thr": 0.2,
            },
            "JaccardDrainLogParser": {
                "depth": 6,
                "sim_thr": 0.2,
            },
            "SpellLogParser": {
                "sim_thr": 0.95,
            },
        },
    ),
    LogParserConfig(
        name="Apache",
        log_format="[{DateTime}] [{Level}] {Content}",
        timestamp_fields=["DateTime"],
        timestamp_format="%a %b %d %H:%M:%S %Y",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 4,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.6,
            },
        },
    ),
    LogParserConfig(
        name="BGL",
        log_format="{Label} {Timestamp} {Date} {Node} {DateTime} {NodeRepeat} {Type} {Component} {Level} {Content}",
        timestamp_fields=["Timestamp", "Date", "DateTime"],
        timestamp_format="epoch",
        user_maskings=[
            (
                r"core\.\d+",
                "<#CORE#>",
            ),
            (
                r"\d+:[A-Fa-f\d]{8,}",
                "<#ADDR#>",
            ),
        ],
        delimiters="=()",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.5,
            },
            "BrainLogParser": {
                "var_thr": 6,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.75,
            },
        },
    ),
    LogParserConfig(
        name="HDFS",
        log_format="{Date} {Time} {Pid} {Level} {Component}: {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%y%m%d %H%M%S",
        user_maskings=[
            (
                r"blk_-?\d+",
                "<#BLK#>",
            ),
        ],
        delimiters=":",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.5,
            },
            "BrainLogParser": {
                "var_thr": 2,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="HPC",
        log_format="{LogId} {Node} {Component} {State} {Timestamp} {Flag} {Content}",
        timestamp_fields=["Timestamp"],
        timestamp_format="epoch",
        delimiters="=:-",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 5,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 5,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.65,
            },
        },
    ),
    LogParserConfig(
        name="Hadoop",
        log_format="{Date} {Time} {Level} [{Process}] {Component}: {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%Y-%m-%d %H:%M:%S,%g",
        delimiters="=:_()",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 6,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="HealthApp",
        log_format="{DateTime}|{Component}|{Pid}|{Content}",
        timestamp_fields=["DateTime"],
        timestamp_format="%Y%m%d-%-H:%-M:%-S:%g",
        user_maskings=[
            (
                r"\d+##\d+##\d+##\d+##\d+##\d+",
                "<#SEQ#>",
            ),
        ],
        delimiters="=:|",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.6,
            },
            "BrainLogParser": {
                "var_thr": 4,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.2,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.2,
            },
            "SpellLogParser": {
                "sim_thr": 0.5,
            },
        },
    ),
    LogParserConfig(
        name="Linux",
        log_format="{Month} {Day} {Time} {Level} {Component}: {Content}",
        timestamp_fields=["Month", "Day", "Time"],
        timestamp_format="%b %d %H:%M:%S",
        delimiters="=:",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.6,
            },
            "BrainLogParser": {
                "var_thr": 4,
            },
            "DrainLogParser": {
                "depth": 6,
                "sim_thr": 0.39,
            },
            "JaccardDrainLogParser": {
                "depth": 6,
                "sim_thr": 0.39,
            },
            "SpellLogParser": {
                "sim_thr": 0.55,
            },
        },
    ),
    LogParserConfig(
        name="Mac",
        log_format="{Month}  {Day} {Time} {User} {Component}: {Content}",
        timestamp_fields=["Month", "Day", "Time"],
        timestamp_format="%b %d %H:%M:%S",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.6,
            },
            "BrainLogParser": {
                "var_thr": 5,
            },
            "DrainLogParser": {
                "depth": 6,
                "sim_thr": 0.7,
            },
            "JaccardDrainLogParser": {
                "depth": 6,
                "sim_thr": 0.7,
            },
            "SpellLogParser": {
                "sim_thr": 0.6,
            },
        },
    ),
    LogParserConfig(
        name="OpenSSH",
        log_format="{Month} {Day} {Time} {Component} sshd[{Pid}]: {Content}",
        timestamp_fields=["Month", "Day", "Time"],
        timestamp_format="%b %d %H:%M:%S",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 10,
                "merge_thr": 0.7,
            },
            "BrainLogParser": {
                "var_thr": 6,
            },
            "DrainLogParser": {
                "depth": 5,
                "sim_thr": 0.6,
            },
            "JaccardDrainLogParser": {
                "depth": 5,
                "sim_thr": 0.6,
            },
            "SpellLogParser": {
                "sim_thr": 0.8,
            },
        },
    ),
    LogParserConfig(
        name="OpenStack",
        log_format="{Logrecord} {Date} {Time} {Pid} {Level} {Component} [{ADDR}] {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%Y-%m-%d %H:%M:%S.%g",
        user_maskings=[
            (
                r"\[instance:(.*?)\]",
                "<#INST#>",
            ),
            (
                r"(/[\w-]+)+",
                "<#PATH#>",
            ),
        ],
        ex_args={
            "AELLogParser": {
                "cluster_thr": 6,
                "merge_thr": 0.5,
            },
            "BrainLogParser": {
                "var_thr": 5,
            },
            "DrainLogParser": {
                "depth": 5,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 5,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.9,
            },
        },
    ),
    LogParserConfig(
        name="Proxifier",
        log_format="[{DateTime}] {Program} - {Content}",
        timestamp_fields=["DateTime"],
        timestamp_format="%m.%d %H:%M:%S",
        user_maskings=[
            (
                r"<\d+\ssec",
                "<#DURATION#>",
            ),
        ],
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 3,
            },
            "DrainLogParser": {
                "depth": 3,
                "sim_thr": 0.6,
            },
            "JaccardDrainLogParser": {
                "depth": 3,
                "sim_thr": 0.6,
            },
            "SpellLogParser": {
                "sim_thr": 0.85,
            },
        },
    ),
    LogParserConfig(
        name="Spark",
        log_format="{Date} {Time} {Level} {Component}: {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%y/%m/%d %H:%M:%S",
        delimiters=":",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 4,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.55,
            },
        },
    ),
    LogParserConfig(
        name="Thunderbird",
        log_format="{Label} {Timestamp} {Date} {User} {Month} {Day} {Time} {Location} {Component}: {Content}",
        timestamp_fields=["Timestamp", "Date", "Month", "Day", "Time"],
        timestamp_format="epoch",
        delimiters="=:",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 3,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.5,
            },
        },
    ),
    LogParserConfig(
        name="Windows",
        log_format="{Date} {Time}, {Level:<} {Component}    {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%c",
        delimiters="=:[]",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 3,
            },
            "DrainLogParser": {
                "depth": 5,
                "sim_thr": 0.7,
            },
            "JaccardDrainLogParser": {
                "depth": 5,
                "sim_thr": 0.7,
            },
            "SpellLogParser": {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="Zookeeper",
        log_format="{Date} {Time} - {Level:<} [{Node}:{Component}@{Id}] - {Content}",
        timestamp_fields=["Date", "Time"],
        timestamp_format="%Y-%m-%d %H:%M:%S,%g",
        delimiters="=:",
        ex_args={
            "AELLogParser": {
                "cluster_thr": 2,
                "merge_thr": 0.4,
            },
            "BrainLogParser": {
                "var_thr": 3,
            },
            "DrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "JaccardDrainLogParser": {
                "depth": 4,
                "sim_thr": 0.5,
            },
            "SpellLogParser": {
                "sim_thr": 0.7,
            },
        },
    ),
]
