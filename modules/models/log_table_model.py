from concurrent.futures import Future, ProcessPoolExecutor
from datetime import datetime
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
    Signal,
    Slot,
)
from PySide6.QtGui import QColor

from modules.duckdb_service import DuckDBService
from modules.logparser import BaseLogParser, LogParserConfig, ParseResult


class LogColumn(IntEnum):
    """日志表模型列枚举"""

    NAME = 0  # 名称
    LINE_COUNT = 1  # 日志行数
    LOG_TYPE = 2  # 日志类型
    FORMAT_TYPE = 3  # 日志格式
    CREATE_TIME = 4  # 创建时间
    STATUS = 5  # 状态 -> 虚拟列
    EXTRACT_METHOD = 6  # 提取方法


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


class LogExtractTaskPool(QObject):
    """日志提取工作进程"""

    finished = Signal(int, int)  # (log_id, line_count)
    error = Signal(int, str)  # (log_id, error_message)

    @staticmethod
    def _run_log_extract_task(
        log_file: Path,
        log_parser_type: type[BaseLogParser],
        log_parser_config: LogParserConfig,
        structured_table_name: str,
        templates_table_name: str,
    ) -> ParseResult:
        """在子进程中执行日志提取任务。"""
        print(f"Parsing file: {log_file}")
        start_time = datetime.now()
        ex_args = log_parser_config.ex_args.get(log_parser_type, {})
        parser = log_parser_type(
            log_parser_config.log_format,
            log_parser_config.masking,
            log_parser_config.delimiters,
            **ex_args,
        )
        result = parser.parse(
            log_file.as_posix(),
            structured_table_name,
            templates_table_name,
            False,
        )
        print(f"Parsing done. [Time taken: {datetime.now() - start_time}]")
        return result

    def __init__(
        self,
        max_workers: int = 1,
        parent=None,
    ):
        super().__init__(parent)

        self._pool = ProcessPoolExecutor(max_workers=max_workers)

    def submit(
        self,
        log_id: int,
        log_file: Path,
        log_parser_type: type[BaseLogParser],
        log_parser_config: LogParserConfig,
        structured_table_name: str,
        templates_table_name: str,
    ):
        future = self._pool.submit(
            LogExtractTaskPool._run_log_extract_task,
            log_file,
            log_parser_type,
            log_parser_config,
            structured_table_name,
            templates_table_name,
        )
        future.add_done_callback(
            lambda f, log_id=log_id: self._on_future_done(f, log_id)
        )

    def _on_future_done(self, future: Future, log_id: int):
        try:
            result = future.result()
            self.finished.emit(log_id, result.line_count)
        except Exception as e:
            self.error.emit(log_id, str(e))

    def kill(self):
        self._pool.kill_workers()
        self._pool.shutdown(wait=False, cancel_futures=True)


class LogTableModel(QAbstractTableModel):
    """日志管理页面的日志表模型"""

    # 显示的表头
    _TABLE_HEADERS = [
        QT_TRANSLATE_NOOP("LogTableModel", "名称"),
        QT_TRANSLATE_NOOP("LogTableModel", "日志行数"),
        QT_TRANSLATE_NOOP("LogTableModel", "日志类型"),
        QT_TRANSLATE_NOOP("LogTableModel", "日志格式"),
        QT_TRANSLATE_NOOP("LogTableModel", "创建时间"),
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
        # 创建日志提取任务进程池
        self._log_extract_pool = LogExtractTaskPool(4, self)
        self._log_extract_pool.finished.connect(self._on_extract_finished)
        self._log_extract_pool.error.connect(self._on_extract_errored)
        # 一次性获取整个表的数据到内存中
        self._df: list[tuple] = DuckDBService.get_log_table()
        # 存储正在提取的任务信息: log_id
        self._extract_tasks: set[int] = set()

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
            return self.tr(str(self._TABLE_HEADERS[section]))
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
        self,
        column: int,
        order: Qt.SortOrder = Qt.SortOrder.AscendingOrder,
    ) -> None:
        # 无效列索引
        if column < 0:
            return

        col = LogColumn(column)
        descending = order == Qt.SortOrder.DescendingOrder

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

    def _get_display_data(self, index: QModelIndex) -> str:
        """获取显示数据"""
        row = index.row()
        col = index.column()

        # 状态列
        if col == LogColumn.STATUS:
            status = self._get_status(index)
            return self.tr(str(self._STATUS_TO_TEXT[status]))

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

    def _set_df_data(self, row: int, column: SqlColumn, value: object):
        """更新内存DataFrame"""
        row_list = list(self._df[row])
        row_list[column] = value
        self._df[row] = tuple(row_list)

    def _set_sql_data(self, log_id: int, column: SqlColumn, value: object):
        """同步数据库"""
        DuckDBService.update_log(log_id, self._SQL_HEADERS[column], value)

    # ==================== 槽函数 ====================

    @Slot(int, int)
    def _on_extract_finished(
        self,
        log_id: int,
        line_count: int,
    ):
        """处理提取完成"""
        # 清理任务信息
        self._extract_tasks.remove(log_id)

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

    @Slot(int, str)
    def _on_extract_errored(self, log_id: int, error_msg: str):
        """处理提取错误"""
        # 清理任务信息
        self._extract_tasks.remove(log_id)

        # 更新ui状态
        if (row := self._get_row(log_id)) >= 0:
            self.dataChanged.emit(
                self.index(row, 0),
                self.index(row, self.columnCount() - 1),
            )
        # 发出错误信号
        self.extractError.emit(log_id, error_msg)

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
        log_parser_type: type[BaseLogParser],
        log_parser_config: LogParserConfig,
    ):
        """请求提取日志"""
        row = index.row()
        log_id = index.data(self.LOG_ID_ROLE)
        # 更新数据库和ui状态
        self._set_sql_data(log_id, SqlColumn.FORMAT_TYPE, log_parser_config.name)
        self._set_sql_data(log_id, SqlColumn.EXTRACT_METHOD, log_parser_type.name())
        self._set_df_data(row, SqlColumn.FORMAT_TYPE, log_parser_config.name)
        self._set_df_data(row, SqlColumn.EXTRACT_METHOD, log_parser_type.name())
        self.dataChanged.emit(
            self.index(row, LogColumn.FORMAT_TYPE),
            self.index(row, LogColumn.EXTRACT_METHOD),
        )

        # 启动进程
        self._log_extract_pool.submit(
            log_id,
            Path(self._df[row][SqlColumn.LOG_URI]),
            log_parser_type,
            log_parser_config,
            self._df[row][SqlColumn.STRUCTURED_TABLE_NAME],
            self._df[row][SqlColumn.TEMPLATES_TABLE_NAME],
        )

        # 记录任务信息
        self._extract_tasks.add(log_id)

    def has_extracting_tasks(self) -> bool:
        """是否有正在提取的任务"""
        return len(self._extract_tasks) > 0

    def kill_tasks(self):
        """中断所有正在提取的任务"""
        self._log_extract_pool.kill()

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
