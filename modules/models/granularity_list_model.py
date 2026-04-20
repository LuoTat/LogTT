from typing import Any

from PySide6.QtCore import QAbstractListModel, QModelIndex, QPersistentModelIndex, Qt

# (显示名, months, days, micros)
_GRANULARITIES = [
    ("1 秒", 0, 0, 1_000_000),
    ("10 秒", 0, 0, 10_000_000),
    ("1 分钟", 0, 0, 60_000_000),
    ("5 分钟", 0, 0, 300_000_000),
    ("10 分钟", 0, 0, 600_000_000),
    ("30 分钟", 0, 0, 1_800_000_000),
    ("1 小时", 0, 0, 3_600_000_000),
    ("6 小时", 0, 0, 21_600_000_000),
    ("1 天", 0, 1, 0),
    ("1 周", 0, 7, 0),
    ("1 月", 1, 0, 0),
]


class GranularityListModel(QAbstractListModel):
    """时间粒度列表模型"""

    INTERVAL_ROLE = Qt.ItemDataRole.UserRole + 1  # (months, days, micros)

    def __init__(self, parent=None):
        super().__init__(parent)

    # ==================== 重写方法 ====================

    def rowCount(
        self,
        parent: QModelIndex | QPersistentModelIndex = QModelIndex(),
    ) -> int:
        return len(_GRANULARITIES)

    def data(
        self,
        index: QModelIndex | QPersistentModelIndex,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if not index.isValid():
            return None

        row = index.row()

        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return _GRANULARITIES[row][0]

        elif role == self.INTERVAL_ROLE:
            return (
                _GRANULARITIES[row][1],
                _GRANULARITIES[row][2],
                _GRANULARITIES[row][3],
            )

        return None
