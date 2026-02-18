from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, Qt

from modules.app_config import appcfg
from modules.constants import BUILTIN_LOG_FORMATS


class FormatTypeListModel(QAbstractListModel):
    """日志格式列表模型"""

    # 用户自定义角色
    LOG_FORMAT_ROLE = Qt.ItemDataRole.UserRole + 1
    LOG_FORMAT_MASK_ROLE = Qt.ItemDataRole.UserRole + 2
    LOG_FORMAT_DELIM_ROLE = Qt.ItemDataRole.UserRole + 3

    def __init__(self, parent=None):
        super().__init__(parent)

        self._df = BUILTIN_LOG_FORMATS
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

        elif role == self.LOG_FORMAT_MASK_ROLE:
            return self._df[row][2]

        elif role == self.LOG_FORMAT_DELIM_ROLE:
            return self._df[row][3]

        return None
