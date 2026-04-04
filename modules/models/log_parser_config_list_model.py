from typing import Any

from PySide6.QtCore import (
    QAbstractListModel,
    QModelIndex,
    QPersistentModelIndex,
    Qt,
    Signal,
)

from modules.app_config import appcfg
from modules.logparser import BUILTIN_LOG_PARSER_CONFIGS
from modules.logparser.log_parser_config import LogParserConfig


class LogParserConfigListModel(QAbstractListModel):
    """日志格式列表模型"""

    # 用户自定义角色
    LOG_PARSER_CONFIG_ROLE = Qt.ItemDataRole.UserRole + 1

    # UI 控制信号
    addSuccess = Signal()  # 添加成功
    deleteSuccess = Signal()  # 删除成功
    editSuccess = Signal()  # 编辑成功

    def __init__(self, parent=None, use_builtin: bool = True):
        super().__init__(parent)

        self._data: list[LogParserConfig] = appcfg.get(appcfg.logParserConfigs).copy()

        if use_builtin:
            self._data.extend(BUILTIN_LOG_PARSER_CONFIGS)

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
            return self._data[row].name

        # 自定义角色
        elif role == self.LOG_PARSER_CONFIG_ROLE:
            return self._data[row]

        return None

    # ==================== 私有方法 ====================

    def _save_configs(self):
        """保存当前配置到 appcfg"""
        appcfg.set(appcfg.logParserConfigs, self._data)

    # ==================== 公共方法 ====================

    def request_add(self, config: LogParserConfig):
        """请求添加配置"""
        row = len(self._data)
        self.beginInsertRows(QModelIndex(), row, row)
        self._data.append(config)
        self.endInsertRows()
        self._save_configs()
        self.addSuccess.emit()

    def request_edit(self, index: QModelIndex, new_config: LogParserConfig):
        """请求编辑配置"""
        row = index.row()
        self._data[row] = new_config
        self.dataChanged.emit(index, index)
        self._save_configs()
        self.editSuccess.emit()

    def request_delete(self, index: QModelIndex):
        """请求删除配置"""
        row = index.row()
        self.beginRemoveRows(QModelIndex(), row, row)
        self._data.pop(row)
        self.endRemoveRows()
        self._save_configs()
        self.deleteSuccess.emit()
