from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "logtt.duckdb"
CONFIG_PATH = PROJECT_ROOT / "config.json"


# 内置掩码规则(正则表达式, 替换字符串)
BUILTIN_MASKING: list[tuple[str, str]] = [
    (
        r"(?P<S>^|[^A-Za-z\d])([A-Za-z\d]{2,}:){3,}[A-Za-z\d]{2,}(?P<E>[^A-Za-z\d]|$)",
        r"$S<§ID§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(:\d{0,})?(?P<E>[^A-Za-z\d]|$)",
        r"$S<§IP§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])([A-Fa-f\d]{4,}\s){3,}[A-Fa-f\d]{4,}(?P<E>[^A-Za-z\d]|$)",
        r"$S<§SEQ§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])0x[A-Fa-f\d]+(?P<E>[^A-Za-z\d]|$)",
        r"$S<§HEX§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])[A-Fa-f\d]{4,}(?P<E>[^A-Za-z\d]|$)",
        r"$S<§HEX§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])[KMGT]?i?B(?P<E>[^A-Za-z\d]|$)",
        r"$S<§SIZE§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])(\d\d:)+\d\d(?P<E>[^A-Za-z\d]|$)",
        r"$S<§TIME§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])\d{1,3}(,\d\d\d)*(?P<E>[^A-Za-z\d]|$)",
        r"$S<§NUM§>$E",
    ),
    (
        r"(?P<S>^|[^A-Za-z\d])[-+]?\d+(?P<E>[^A-Za-z\d]|$)",
        r"$S<§NUM§>$E",
    ),
    # (
    #     r"(?P<S>^|[^A-Za-z\d])(([\w-]+\.){2,}[\w-]+)(?P<E>[^A-Za-z\d]|$)",
    #     "<FQDN>",
    # ),
]


# 内置日志格式列表：(名称, 日志格式, 掩码规则, 分隔符列表)
BUILTIN_LOG_FORMATS: list[tuple[str, str, list[tuple[str, str]], list[str]]] = [
    (
        "HDFS",
        "<Date> <Time> <Pid> <Level> <Component>: <Content>",
        [
            (
                r"blk_-?\d+",
                "<§BLK§>",
            ),
        ],
        [":"],
    ),
    (
        "Hadoop",
        r"<Date> <Time> <Level> \[<Process>\] <Component>: <Content>",
        [],
        ["=", ":", "_", "(", ")"],
    ),
    (
        "Spark",
        "<Date> <Time> <Level> <Component>: <Content>",
        [],
        [":"],
    ),
    (
        "Zookeeper",
        r"<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>",
        [],
        ["=", ":"],
    ),
    (
        "BGL",
        "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        [
            (
                r"core\.\d+",
                "<§CORE§>",
            ),
            (
                r"\d+:[A-Fa-f\d]{8,}",
                "<§ADDR§>",
            ),
        ],
        ["=", "..", "(", ")"],
    ),
    (
        "HPC",
        "<LogId> <Node> <Component> <State> <Time> <Flag> <Content>",
        [],
        ["=", ":", "-"],
    ),
    (
        "Thunderbird",
        r"<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>",
        [],
        ["=", ":"],
    ),
    (
        "Windows",
        "<Date> <Time>, <Level>                  <Component>    <Content>",
        [],
        ["=", ":", "[", "]"],
    ),
    (
        "Linux",
        r"<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>",
        [],
        [],
    ),
    (
        "Android",
        "<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>",
        [
            (
                r"(/[\w-]+)+",
                "<§PATH§>",
            ),
        ],
        [":", "="],
    ),
    (
        "HealthApp",
        r"<Time>\|<Component>\|<Pid>\|<Content>",
        [
            (
                r"\d+##\d+##\d+##\d+##\d+##\d+",
                "<§SEQ§>",
            ),
        ],
        [":", "=", "|"],
    ),
    (
        "Apache",
        r"\[<Time>\] \[<Level>\] <Content>",
        [],
        ["(", ")"],
    ),
    (
        "Proxifier",
        r"\[<Time>\] <Program> - <Content>",
        [
            (
                r"<\d+\ssec",
                "<§DURATION§>",
            ),
        ],
        [],
    ),
    (
        "OpenSSH",
        r"<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>",
        [],
        ["=", ":", "[", "]"],
    ),
    (
        "OpenStack",
        r"<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>",
        [
            (
                r"\[instance:(.*?)\]",
                "<§INST§>",
            ),
            (
                r"(/[\w-]+)+",
                "<§PATH§>",
            ),
        ],
        [],
    ),
    (
        "Mac",
        r"<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>",
        [],
        [],
    ),
]
