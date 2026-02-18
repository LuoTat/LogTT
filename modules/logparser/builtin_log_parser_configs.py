from .ael_log_parser import AELLogParser
from .brain_log_parser import BrainLogParser
from .drain_log_parser import DrainLogParser
from .jaccard_drain_log_parser import JaccardDrainLogParser
from .log_parser_config import LogParserConfig
from .spell_log_parser import SpellLogParser

# 内置日志格式配置
BUILTIN_LOG_PARSER_CONFIGS: list[LogParserConfig] = [
    LogParserConfig(
        name="HDFS",
        log_format="<Date> <Time> <Pid> <Level> <Component>: <Content>",
        masking=[
            (
                r"blk_-?\d+",
                "<§BLK§>",
            ),
        ],
        delimiters=[":"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.5,
            },
            BrainLogParser: {
                "var_thr": 2,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="Hadoop",
        log_format=r"<Date> <Time> <Level> \[<Process>\] <Component>: <Content>",
        delimiters=["=", ":", "_", "(", ")"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 6,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="Spark",
        log_format="<Date> <Time> <Level> <Component>: <Content>",
        delimiters=[":"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 4,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.55,
            },
        },
    ),
    LogParserConfig(
        name="Zookeeper",
        log_format=r"<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>",
        delimiters=["=", ":"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 3,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="BGL",
        log_format="<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        masking=[
            (
                r"core\.\d+",
                "<§CORE§>",
            ),
            (
                r"\d+:[A-Fa-f\d]{8,}",
                "<§ADDR§>",
            ),
        ],
        delimiters=["=", "..", "(", ")"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.5,
            },
            BrainLogParser: {
                "var_thr": 6,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.75,
            },
        },
    ),
    LogParserConfig(
        name="HPC",
        log_format="<LogId> <Node> <Component> <State> <Time> <Flag> <Content>",
        delimiters=["=", ":", "-"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 5,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 5,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.65,
            },
        },
    ),
    LogParserConfig(
        name="Thunderbird",
        log_format=r"<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>",
        delimiters=["=", ":"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 3,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.5,
            },
        },
    ),
    LogParserConfig(
        name="Windows",
        log_format="<Date> <Time>, <Level>                  <Component>    <Content>",
        delimiters=["=", ":", "[", "]"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 3,
            },
            DrainLogParser: {
                "depth": 5,
                "sim_thr": 0.7,
            },
            JaccardDrainLogParser: {
                "depth": 5,
                "sim_thr": 0.7,
            },
            SpellLogParser: {
                "sim_thr": 0.7,
            },
        },
    ),
    LogParserConfig(
        name="Linux",
        log_format=r"<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>",
        delimiters=["=", ":"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.6,
            },
            BrainLogParser: {
                "var_thr": 4,
            },
            DrainLogParser: {
                "depth": 6,
                "sim_thr": 0.39,
            },
            JaccardDrainLogParser: {
                "depth": 6,
                "sim_thr": 0.39,
            },
            SpellLogParser: {
                "sim_thr": 0.55,
            },
        },
    ),
    LogParserConfig(
        name="Android",
        log_format="<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>",
        masking=[
            (
                r"(/[\w-]+)+",
                "<§PATH§>",
            ),
        ],
        delimiters=["=", ":"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.6,
            },
            BrainLogParser: {
                "var_thr": 5,
            },
            DrainLogParser: {
                "depth": 6,
                "sim_thr": 0.2,
            },
            JaccardDrainLogParser: {
                "depth": 6,
                "sim_thr": 0.2,
            },
            SpellLogParser: {
                "sim_thr": 0.95,
            },
        },
    ),
    LogParserConfig(
        name="HealthApp",
        log_format=r"<Time>\|<Component>\|<Pid>\|<Content>",
        masking=[
            (
                r"\d+##\d+##\d+##\d+##\d+##\d+",
                "<§SEQ§>",
            ),
        ],
        delimiters=["=", ":", "|"],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.6,
            },
            BrainLogParser: {
                "var_thr": 4,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.2,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.2,
            },
            SpellLogParser: {
                "sim_thr": 0.5,
            },
        },
    ),
    LogParserConfig(
        name="Apache",
        log_format=r"\[<Time>\] \[<Level>\] <Content>",
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 4,
            },
            DrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 4,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.6,
            },
        },
    ),
    LogParserConfig(
        name="Proxifier",
        log_format=r"\[<Time>\] <Program> - <Content>",
        masking=[
            (
                r"<\d+\ssec",
                "<§DURATION§>",
            ),
        ],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.4,
            },
            BrainLogParser: {
                "var_thr": 3,
            },
            DrainLogParser: {
                "depth": 3,
                "sim_thr": 0.6,
            },
            JaccardDrainLogParser: {
                "depth": 3,
                "sim_thr": 0.6,
            },
            SpellLogParser: {
                "sim_thr": 0.85,
            },
        },
    ),
    LogParserConfig(
        name="OpenSSH",
        log_format=r"<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>",
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 10,
                "merge_thr": 0.7,
            },
            BrainLogParser: {
                "var_thr": 6,
            },
            DrainLogParser: {
                "depth": 5,
                "sim_thr": 0.6,
            },
            JaccardDrainLogParser: {
                "depth": 5,
                "sim_thr": 0.6,
            },
            SpellLogParser: {
                "sim_thr": 0.8,
            },
        },
    ),
    LogParserConfig(
        name="OpenStack",
        log_format=r"<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>",
        masking=[
            (
                r"\[instance:(.*?)\]",
                "<§INST§>",
            ),
            (
                r"(/[\w-]+)+",
                "<§PATH§>",
            ),
        ],
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 6,
                "merge_thr": 0.5,
            },
            BrainLogParser: {
                "var_thr": 5,
            },
            DrainLogParser: {
                "depth": 5,
                "sim_thr": 0.5,
            },
            JaccardDrainLogParser: {
                "depth": 5,
                "sim_thr": 0.5,
            },
            SpellLogParser: {
                "sim_thr": 0.9,
            },
        },
    ),
    LogParserConfig(
        name="Mac",
        log_format=r"<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>",
        ex_args={
            AELLogParser: {
                "log_cluster_thr": 2,
                "merge_thr": 0.6,
            },
            BrainLogParser: {
                "var_thr": 5,
            },
            DrainLogParser: {
                "depth": 6,
                "sim_thr": 0.7,
            },
            JaccardDrainLogParser: {
                "depth": 6,
                "sim_thr": 0.7,
            },
            SpellLogParser: {
                "sim_thr": 0.6,
            },
        },
    ),
]
