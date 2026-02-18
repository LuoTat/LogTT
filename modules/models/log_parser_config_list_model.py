from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt

from modules.app_config import appcfg
from modules.logparser import BUILTIN_LOG_PARSER_CONFIGS


class LogParserConfigListModel(QAbstractListModel):
    """日志格式列表模型"""

    # 用户自定义角色
    LOG_PARSER_CONFIG_ROLE = Qt.ItemDataRole.UserRole + 1

    def __init__(self, parent=None):
        super().__init__(parent)

        self._df = BUILTIN_LOG_PARSER_CONFIGS
        if user_formats := appcfg.get(appcfg.logParserConfigs):
            self._df.extend(user_formats)

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df)

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        row = index.row()

        # 显示角色
        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return self._df[row].name

        # 自定义角色
        elif role == self.LOG_PARSER_CONFIG_ROLE:
            return self._df[row]

        return None
