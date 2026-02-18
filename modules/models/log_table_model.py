from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any

import duckdb
from PySide6.QtCore import (
    QT_TRANSLATE_NOOP,
    QAbstractTableModel,
    QModelIndex,
    QObject,
    Qt,
    QThread,
    Signal,
    Slot,
)
from PySide6.QtGui import QColor

from modules.duckdb_service import DuckDBService
from modules.logparser.base_log_parser import BaseLogParser


class LogColumn(IntEnum):
    """日志表模型列枚举"""

    NAME = 0  # 名称
    LINE_COUNT = 1  # 日志行数
    LOG_TYPE = 2  # 日志类型
    FORMAT_TYPE = 3  # 日志格式
    CREATE_TIME = 4  # 创建时间
    PROGRESS = 5  # 进度 -> 虚拟列
    STATUS = 6  # 状态 -> 虚拟列
    EXTRACT_METHOD = 7  # 提取方法


class SqlColumn(IntEnum):
    """日志表数据库列枚举"""

    ID = 0  # id
    LOG_TYPE = 1  # log_type
    FORMAT_TYPE = 2  # format_type
    LOG_URI = 3  # log_uri
    CREATE_TIME = 4  # create_time
    IS_EXTRACTED = 5  # is_extracted
    EXTRACT_METHOD = 6  # extract_method
    LINE_COUNT = 7  # line_count
    STRUCTURED_TABLE_NAME = 8  # structured_table_name
    TEMPLATES_TABLE_NAME = 9  # templates_table_name


class LogStatus(IntEnum):
    """日志状态枚举"""

    EXTRACTED = 0  # 已提取
    NOT_EXTRACTED = 1  # 未提取
    EXTRACTING = 2  # 提取中


class LogExtractTask(QObject):
    """日志提取工作线程"""

    finished = Signal(int, int)  # (log_id, line_count)
    interrupted = Signal(int)  # (log_id)
    error = Signal(int, str)  # (log_id, error_message)
    progress = Signal(int, int)  # (log_id, progress)

    def __init__(
        self,
        log_id: int,
        log_file: Path,
        logparser_type: type[BaseLogParser],
        format_type: str,
        log_format: str,
        mask: list[tuple[str, str]],
        delimiters: list[str],
        structured_table_name: str,
        templates_table_name: str,
    ):
        super().__init__()
        self._log_id = log_id
        self._log_file = log_file
        self._logparser_type = logparser_type
        self._format_type = format_type
        self._log_format = log_format
        self._mask = mask
        self._delimiters = delimiters
        self._structured_table_name = structured_table_name
        self._templates_table_name = templates_table_name

    @Slot()
    def run(self):
        try:
            result = self._logparser_type(
                self._log_format,
                self._mask,
                self._delimiters,
            ).parse(
                self._log_file,
                self._structured_table_name,
                self._templates_table_name,
                lambda: QThread.currentThread().isInterruptionRequested(),
                False,
                lambda progress: self.progress.emit(self._log_id, progress),
            )

            # 提取完成
            self.finished.emit(self._log_id, result.line_count)
        except InterruptedError:
            self.interrupted.emit(self._log_id)
        except Exception as e:
            self.error.emit(self._log_id, str(e))


@dataclass
class LogExtractTaskInfo:
    """日志提取任务信息"""

    thread: QThread
    task: LogExtractTask
    progress: int = 0


class LogTableModel(QAbstractTableModel):
    """日志管理页面的日志表模型"""

    # 显示的表头
    _TABLE_HEADERS = [
        QT_TRANSLATE_NOOP("LogTableModel", "名称"),
        QT_TRANSLATE_NOOP("LogTableModel", "日志行数"),
        QT_TRANSLATE_NOOP("LogTableModel", "日志类型"),
        QT_TRANSLATE_NOOP("LogTableModel", "日志格式"),
        QT_TRANSLATE_NOOP("LogTableModel", "创建时间"),
        QT_TRANSLATE_NOOP("LogTableModel", "进度"),
        QT_TRANSLATE_NOOP("LogTableModel", "状态"),
        QT_TRANSLATE_NOOP("LogTableModel", "提取方法"),
    ]
    # 数据库表头
    _SQL_HEADERS = [
        "id",
        "log_type",
        "format_type",
        "log_uri",
        "create_time",
        "is_extracted",
        "extract_method",
        "line_count",
        "structured_table_name",
        "templates_table_name",
    ]
    # 模型列到数据库列的映射
    _MODEL_TO_SQL = [
        SqlColumn.LOG_URI,
        SqlColumn.LINE_COUNT,
        SqlColumn.LOG_TYPE,
        SqlColumn.FORMAT_TYPE,
        SqlColumn.CREATE_TIME,
        None,  # 进度虚拟列
        None,  # 状态虚拟列
        SqlColumn.EXTRACT_METHOD,
    ]
    # 状态显示文本
    _STATUS_TO_TEXT = [
        QT_TRANSLATE_NOOP("LogTableModel", "已提取"),
        QT_TRANSLATE_NOOP("LogTableModel", "未提取"),
        QT_TRANSLATE_NOOP("LogTableModel", "提取中"),
    ]

    # 用户自定义角色
    LOG_ID_ROLE = Qt.ItemDataRole.UserRole + 1
    LOG_STATUS_ROLE = Qt.ItemDataRole.UserRole + 2

    # UI 控制信号
    extractFinished = Signal(int, int)  # 提取完成 (log_id, line_count)
    extractInterrupted = Signal(int)  # 提取中断 (log_id)
    extractError = Signal(int, str)  # 提取错误 (log_id, error_message)
    addSuccess = Signal()  # 添加成功
    addDuplicate = Signal()  # 添加重复
    addError = Signal(str)  # 添加失败 (error_message)
    deleteSuccess = Signal()  # 删除成功
    deleteError = Signal(str)  # 删除失败 (error_message)

    def __init__(self, parent=None):
        super().__init__(parent)

        # 创建log表
        DuckDBService.create_log_table_if_not_exists()
        # 一次性获取整个表的数据到内存中
        self._df: list[tuple] = DuckDBService.get_log_table()
        # 存储正在提取的任务信息: log_id -> LogExtractTaskInfo
        self._extract_tasks: dict[int, LogExtractTaskInfo] = {}

    # ==================== 重写方法 ====================

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._df)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return len(self._TABLE_HEADERS)

    def headerData(
        self,
        section: int,
        orientation: Qt.Orientation,
        role: int = Qt.ItemDataRole.DisplayRole,
    ) -> Any:
        if (
            orientation == Qt.Orientation.Horizontal
            and role == Qt.ItemDataRole.DisplayRole
        ):
            return self.tr(self._TABLE_HEADERS[section])
        return None

    def data(self, index: QModelIndex, role: int = Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        # 处理显示和编辑角色
        if role in (Qt.ItemDataRole.DisplayRole, Qt.ItemDataRole.EditRole):
            return self._get_display_data(index)

        # 处理前景色角色
        elif role == Qt.ItemDataRole.ForegroundRole:
            status = self._get_status(index)
            if status == LogStatus.EXTRACTING:
                return QColor(Qt.GlobalColor.green)
            return None

        # 处理自定义角色 - LOG_ID_ROLE
        elif role == self.LOG_ID_ROLE:
            return self._df[index.row()][SqlColumn.ID]

        # 处理自定义角色 - LOG_STATUS_ROLE
        elif role == self.LOG_STATUS_ROLE:
            return self._get_status(index)

        return None

    def sort(
        self, column: int, order: Qt.SortOrder = Qt.SortOrder.AscendingOrder
    ) -> None:
        # 无效列索引
        if column < 0:
            return

        col = LogColumn(column)
        descending = order == Qt.SortOrder.DescendingOrder

        # 虚拟列：进度
        if col == LogColumn.PROGRESS:

            def sort_progress(log: tuple) -> int:
                log_id = log[SqlColumn.ID]
                if log[SqlColumn.IS_EXTRACTED]:
                    return 100
                elif log_id in self._extract_tasks:
                    return self._extract_tasks[log_id].progress
                else:
                    return 0

            self.layoutAboutToBeChanged.emit()
            self._df = sorted(self._df, key=sort_progress, reverse=descending)
            self.layoutChanged.emit()
            return

        # 虚拟列：状态
        if col == LogColumn.STATUS:

            def sort_status(log: tuple) -> int:
                if log[SqlColumn.IS_EXTRACTED]:
                    return LogStatus.EXTRACTED
                elif log[SqlColumn.ID] in self._extract_tasks:
                    return LogStatus.EXTRACTING
                else:
                    return LogStatus.NOT_EXTRACTED

            self.layoutAboutToBeChanged.emit()
            self._df = sorted(self._df, key=sort_status, reverse=descending)
            self.layoutChanged.emit()
            return

        # 常规列：直接按数据库列索引排序
        sql_col = self._MODEL_TO_SQL[col]
        self.layoutAboutToBeChanged.emit()
        self._df = sorted(
            self._df,
            key=lambda log: (log[sql_col] is None, log[sql_col]),
            reverse=descending,
        )
        self.layoutChanged.emit()

    # ==================== 私有方法 ====================

    def _get_row(self, log_id: int) -> int:
        """根据 log_id 获取行号"""
        for idx, row in enumerate(self._df):
            if row[SqlColumn.ID] == log_id:
                return idx
        return -1

    def _get_display_data(self, index: QModelIndex) -> Any:
        """获取显示数据"""
        row = index.row()
        col = index.column()

        # 进度列
        if col == LogColumn.PROGRESS:
            return self._get_progress(index)

        # 状态列
        elif col == LogColumn.STATUS:
            status = self._get_status(index)
            return self.tr(self._STATUS_TO_TEXT[status])

        # 名称列
        elif col == LogColumn.NAME:
            uri = self._df[row][SqlColumn.LOG_URI]
            return Path(uri).name

        # 常规列
        return str(self._df[row][self._MODEL_TO_SQL[col]])

    def _get_status(self, index: QModelIndex) -> LogStatus:
        """获取日志状态"""
        is_extracted = self._df[index.row()][SqlColumn.IS_EXTRACTED]
        if is_extracted:
            return LogStatus.EXTRACTED
        elif index.data(self.LOG_ID_ROLE) in self._extract_tasks:
            return LogStatus.EXTRACTING
        else:
            return LogStatus.NOT_EXTRACTED

    def _get_progress(self, index: QModelIndex) -> int:
        """获取提取进度"""
        is_extracted = self._df[index.row()][SqlColumn.IS_EXTRACTED]
        if is_extracted:
            return 100
        elif (log_id := index.data(self.LOG_ID_ROLE)) in self._extract_tasks:
            return self._extract_tasks[log_id].progress
        return 0

    def _set_df_data(self, row: int, column: SqlColumn, value: object):
        """更新内存DataFrame"""
        row_list = list(self._df[row])
        row_list[column] = value
        self._df[row] = tuple(row_list)

    def _set_sql_data(self, log_id: int, column: SqlColumn, value: object):
        """同步数据库"""
        DuckDBService.update_log(log_id, self._SQL_HEADERS[column], value)

    def _interrupt_task(self, index: QModelIndex):
        """中断提取任务"""
        log_id = index.data(self.LOG_ID_ROLE)
        if log_id not in self._extract_tasks:
            return

        task_info = self._extract_tasks.pop(log_id)
        task_info.thread.requestInterruption()
        task_info.thread.quit()
        task_info.thread.wait()

    def _clean_task(self, log_id: int):
        """清理任务信息"""
        if log_id in self._extract_tasks:
            task_info = self._extract_tasks.pop(log_id)
            task_info.thread.quit()
            task_info.thread.wait()

    # ==================== 槽函数 ====================

    @Slot(int, int)
    def _on_extract_finished(
        self,
        log_id: int,
        line_count: int,
    ):
        """处理提取完成"""
        # 清理任务信息
        self._clean_task(log_id)

        # 更新数据库和ui状态
        self._set_sql_data(log_id, SqlColumn.IS_EXTRACTED, True)
        self._set_sql_data(log_id, SqlColumn.LINE_COUNT, line_count)
        if (row := self._get_row(log_id)) >= 0:
            self._set_df_data(row, SqlColumn.IS_EXTRACTED, True)
            self._set_df_data(row, SqlColumn.LINE_COUNT, line_count)
            self.dataChanged.emit(
                self.index(row, 0),
                self.index(row, self.columnCount() - 1),
            )

        # 发出完成信号
        self.extractFinished.emit(log_id, line_count)

    @Slot(int)
    def _on_extract_interrupted(self, log_id: int):
        """处理提取中断"""
        # 清理任务信息
        self._clean_task(log_id)

        # 更新ui状态
        if (row := self._get_row(log_id)) >= 0:
            self.dataChanged.emit(
                self.index(row, 0),
                self.index(row, self.columnCount() - 1),
            )
        # 发出中断信号
        self.extractInterrupted.emit(log_id)

    @Slot(int, str)
    def _on_extract_errored(self, log_id: int, error_msg: str):
        """处理提取错误"""
        # 清理任务信息
        self._clean_task(log_id)

        # 更新ui状态
        if (row := self._get_row(log_id)) >= 0:
            self.dataChanged.emit(
                self.index(row, 0),
                self.index(row, self.columnCount() - 1),
            )
        # 发出错误信号
        self.extractError.emit(log_id, error_msg)

    @Slot(int, int)
    def _on_extract_progress(self, log_id: int, progress: int):
        """处理提取进度"""
        self._extract_tasks[log_id].progress = progress
        # 更新ui状态
        if (row := self._get_row(log_id)) >= 0:
            self.dataChanged.emit(
                self.index(row, LogColumn.PROGRESS),
                self.index(row, LogColumn.PROGRESS),
            )

    # ==================== 公共方法 ====================

    def request_add(self, log_type: str, log_uri: str, extract_method: str = ""):
        """请求添加日志记录"""
        # 更新数据库和ui状态
        try:
            DuckDBService.insert_log(log_type, log_uri, extract_method)
            self.refresh()
            self.addSuccess.emit()
        except duckdb.ConstraintException as e:
            if "unique constraint" in str(e.args).lower():
                self.addDuplicate.emit()
            else:
                self.addError.emit(str(e))
        except Exception as e:
            self.addError.emit(str(e))

    def request_delete(self, index: QModelIndex):
        """请求删除日志记录"""
        # 如果有正在提取的任务，先中断
        self._interrupt_task(index)
        # 更新数据库和ui状态
        try:
            row = index.row()
            log_id = index.data(self.LOG_ID_ROLE)
            structured_table_name = self._df[row][SqlColumn.STRUCTURED_TABLE_NAME]
            templates_table_name = self._df[row][SqlColumn.TEMPLATES_TABLE_NAME]

            # 删除关联的结构化表和模板表
            DuckDBService.drop_table(structured_table_name)
            DuckDBService.drop_table(templates_table_name)
            # 删除日志记录
            DuckDBService.delete_log(log_id)

            self.beginRemoveRows(QModelIndex(), row, row)
            self._df.pop(row)
            self.endRemoveRows()
            self.deleteSuccess.emit()
        except Exception as e:
            self.deleteError.emit(str(e))

    def request_extract(
        self,
        index: QModelIndex,
        logparser_type: type[BaseLogParser],
        format_type: str,
        log_format: str,
        mask: list[tuple[str, str]],
        delimiters: list[str],
    ):
        """请求提取日志"""
        row = index.row()
        log_id = index.data(self.LOG_ID_ROLE)
        # 更新数据库和ui状态
        self._set_sql_data(log_id, SqlColumn.FORMAT_TYPE, format_type)
        self._set_sql_data(log_id, SqlColumn.EXTRACT_METHOD, logparser_type.name())
        self._set_df_data(row, SqlColumn.FORMAT_TYPE, format_type)
        self._set_df_data(row, SqlColumn.EXTRACT_METHOD, logparser_type.name())
        self.dataChanged.emit(
            self.index(row, LogColumn.FORMAT_TYPE),
            self.index(row, LogColumn.EXTRACT_METHOD),
        )

        # 创建提取任务
        task = LogExtractTask(
            log_id,
            Path(self._df[row][SqlColumn.LOG_URI]),
            logparser_type,
            format_type,
            log_format,
            mask,
            delimiters,
            self._df[row][SqlColumn.STRUCTURED_TABLE_NAME],
            self._df[row][SqlColumn.TEMPLATES_TABLE_NAME],
        )

        # 创建工作线程
        thread = QThread()
        task.moveToThread(thread)

        # 保存任务信息
        task_info = LogExtractTaskInfo(thread, task)
        self._extract_tasks[log_id] = task_info

        # 连接信号
        thread.started.connect(task.run)
        task.finished.connect(self._on_extract_finished)
        task.interrupted.connect(self._on_extract_interrupted)
        task.error.connect(self._on_extract_errored)
        task.progress.connect(self._on_extract_progress)

        # 启动线程
        thread.start()

    def request_interrupt_task(self, index: QModelIndex):
        """请求中断提取任务"""
        self._interrupt_task(index)

    def has_extracting_tasks(self) -> bool:
        """是否有正在提取的任务"""
        return len(self._extract_tasks) > 0

    def interrupt_all_tasks(self):
        """中断所有正在提取的任务"""
        for row in range(self.rowCount()):
            self._interrupt_task(self.index(row, 0))

    def search_by_name(self, keyword: str):
        """按 URI 关键字搜索"""
        kw = keyword.strip().lower()

        # 关键字为空时恢复全量数据
        if not kw:
            self.refresh()
            return

        self.beginResetModel()
        self._df = [
            row for row in self._df if kw in Path(row[SqlColumn.LOG_URI]).stem.lower()
        ]
        self.endResetModel()

    def clear_search(self):
        """清除搜索"""
        # 恢复全量数据
        self.refresh()

    def refresh(self):
        """刷新模型数据"""
        self.beginResetModel()
        self._df = DuckDBService.get_log_table()
        self.endResetModel()
