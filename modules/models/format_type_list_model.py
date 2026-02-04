from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt

from modules.app_config import appcfg

buildin_log_formats = [
    ("HDFS", "<Date> <Time> <Pid> <Level> <Component>: <Content>", [r"blk_-?\d+", r"(\d+\.){3}\d+(:\d+)?"]),
    (
        "Hadoop",
        r"<Date> <Time> <Level> \[<Process>\] <Component>: <Content>",
        [r"(\d+\.){3}\d+"],
    ),
    (
        "Spark",
        "<Date> <Time> <Level> <Component>: <Content>",
        [r"(\d+\.){3}\d+", r"\b[KGTM]?B\b", r"([\w-]+\.){2,}[\w-]+"],
    ),
    (
        "Zookeeper",
        r"<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>",
        [r"(/|)(\d+\.){3}\d+(:\d+)?"],
    ),
    (
        "BGL",
        "<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>",
        [r"core\.\d+", r"\d+:[a-fA-F0-9]{8,}", r"[a-fA-F0-9]{8,}"],
    ),
    (
        "HPC",
        "<LogId> <Node> <Component> <State> <Time> <Flag> <Content>",
        [r"=\d+"],
    ),
    (
        "Thunderbird",
        r"<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>",
        [r"(\d+\.){3}\d+"],
    ),
    (
        "Windows",
        "<Date> <Time>, <Level>                  <Component>    <Content>",
        [r"0x.*?\s"],
    ),
    (
        "Linux",
        r"<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>",
        [r"(\d+\.){3}\d+", r"\d{2}:\d{2}:\d{2}"],
    ),
    (
        "Android",
        "<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>",
        [r"(/[\w-]+)+", r"([\w-]+\.){2,}[\w-]+", r"\b(\-?\+?\d+)\b|\b0[Xx][a-fA-F\d]+\b|\b[a-fA-F\d]{4,}\b"],
    ),
    (
        "HealthApp",
        r"<Time>\|<Component>\|<Pid>\|<Content>",
        [r"\d+##\d+##\d+##\d+##\d+##\d+", r"=\d+"],
    ),
    (
        "Apache",
        r"\[<Time>\] \[<Level>\] <Content>",
        [r"(\d+\.){3}\d+"],
    ),
    (
        "Proxifier",
        r"\[<Time>\] <Program> - <Content>",
        [r"<\d+\ssec", r"([\w-]+\.)+[\w-]+(:\d+)?", r"\d{2}:\d{2}(:\d{2})*", r"[KGTM]B"],
    ),
    (
        "OpenSSH",
        r"<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>",
        [r"(\d+\.){3}\d+", r"([\w-]+\.){2,}[\w-]+"],
    ),
    (
        "OpenStack",
        r"<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[<ADDR>\] <Content>",
        [r"\[instance:\s*(.*?)\]", r"((\d+\.){3}\d+,?)+", r"/.+?\s", r"\d+"],
    ),
    (
        "Mac",
        r"<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>",
        [r"([\w-]+\.){2,}[\w-]+"],
    ),
]


class FormatTypeListModel(QAbstractListModel):
    """日志格式列表模型"""

    # 用户自定义角色
    LOG_FORMAT_ROLE = Qt.ItemDataRole.UserRole + 1
    LOG_FORMAT_REGEX_ROLE = Qt.ItemDataRole.UserRole + 2

    def __init__(self, parent=None):
        super().__init__(parent)

        self._df: list[tuple[str, str, list[str]]] = buildin_log_formats
        if user_formats := appcfg.get(appcfg.userFormatType):
            self._df.append(user_formats)

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()

        # 显示角色
        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return self._df[row][0]

        # 自定义角色
        elif role == self.LOG_FORMAT_ROLE:
            return self._df[row][1]

        elif role == self.LOG_FORMAT_REGEX_ROLE:
            return self._df[row][2]

        return None
