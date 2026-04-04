from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QPersistentModelIndex, Qt

from modules.logparser import LogParserProtocol, ParserFactory


class LogParserListModel(QAbstractListModel):
    """日志解析器列表模型"""

    # 用户自定义角色
    LOG_PARSER_TYPE_ROLE = Qt.ItemDataRole.UserRole + 1  # 解析器类型角色
    LOG_PARSER_DISCRIPTION_ROLE = Qt.ItemDataRole.UserRole + 2  # 解析器描述角色

    def __init__(self, parent=None):
        super().__init__(parent)

        self._data: list[type[LogParserProtocol]] = ParserFactory.get_all_parsers_type()

    # ==================== 重写方法 ====================

    def rowCount(
        self,
        parent: QModelIndex | QPersistentModelIndex = QModelIndex(),
    ) -> int:
        return len(self._data)

    def data(
        self,
        index: QModelIndex | QPersistentModelIndex,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if not index.isValid():
            return None

        row = index.row()

        # 显示角色
        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return self._data[row].name()

        # 自定义角色
        elif role == self.LOG_PARSER_TYPE_ROLE:
            return self._data[row]

        elif role == self.LOG_PARSER_DISCRIPTION_ROLE:
            return self._data[row].description()

        return None
