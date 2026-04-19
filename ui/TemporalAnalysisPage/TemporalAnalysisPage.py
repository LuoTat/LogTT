from modules.duckdb_service import DuckDBService
from modules.log_analysis import LogAnalysis
from PySide6.QtCore import (
    Qt,
    Slot,
)
from PySide6.QtGui import QShowEvent
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QVBoxLayout,
    QWidget,
)
from qfluentwidgets import BodyLabel, ComboBox, InfoBar, InfoBarPosition
from qfluentwidgets.components import ModelComboBox

from modules.models import ExtractedLogListModel
from ui.Widgets import GRANULARITIES, LogFrequencyCard


class TemporalAnalysisPage(QWidget):
    """时序分析界面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("TemporalAnalysisPage")

        # 主布局
        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        # 初始化日志列表模型
        self._extracted_log_list_model = ExtractedLogListModel(self)
        self._select_log_id = -1
        self._init_toolbar()
        self._init_chart()

    # ==================== 重写方法 ====================

    def showEvent(self, event: QShowEvent):
        """页面显示时自动刷新日志列表"""
        super().showEvent(event)
        self._extracted_log_list_model.refresh()
        # 查找对应的索引
        if (index := self._extracted_log_list_model.get_row(self._select_log_id)) >= 0:
            self._log_combo_box.setCurrentIndex(index)
        else:
            # 刚进入此页面或日志已被删除，重置为初始状态
            self._select_log_id = -1
            self._log_combo_box.setCurrentIndex(-1)
            self._frequency_card.clear()

    # ==================== 私有方法 ====================

    def _init_toolbar(self):
        """初始化工具栏"""
        tool_bar_layout = QHBoxLayout()
        tool_bar_layout.setSpacing(16)

        label = BodyLabel(self.tr("选择日志："), self)
        tool_bar_layout.addWidget(label)

        self._log_combo_box = ModelComboBox(self)
        self._log_combo_box.setModel(self._extracted_log_list_model)
        self._log_combo_box.setMinimumWidth(400)
        self._log_combo_box.setPlaceholderText(self.tr("请选择已提取的日志文件"))
        self._log_combo_box.currentIndexChanged.connect(self._on_log_selected)
        tool_bar_layout.addWidget(self._log_combo_box)

        tool_bar_layout.addStretch()

        granularity_label = BodyLabel(self.tr("时间粒度："), self)
        tool_bar_layout.addWidget(granularity_label)

        self._granularity_combo_box = ComboBox(self)
        self._granularity_combo_box.setMinimumWidth(120)
        self._granularity_combo_box.addItems([g[0] for g in GRANULARITIES])
        self._granularity_combo_box.setCurrentIndex(2)  # 默认1分钟
        self._granularity_combo_box.currentIndexChanged.connect(
            self._on_granularity_changed
        )
        tool_bar_layout.addWidget(self._granularity_combo_box)

        self._main_layout.addLayout(tool_bar_layout)

    def _init_chart(self):
        """初始化图表"""
        self._card_layout = QGridLayout()
        self._card_layout.setSpacing(16)

        self._frequency_card = LogFrequencyCard(parent=self)
        self._card_layout.addWidget(self._frequency_card, 0, 0)

        self._main_layout.addLayout(self._card_layout)

    def _refresh_chart(self):
        """根据当前选择刷新图表"""
        if self._select_log_id < 0:
            return

        model_index = self._extracted_log_list_model.index(
            self._log_combo_box.currentIndex()
        )
        structured_table_name = model_index.data(
            ExtractedLogListModel.STRUCTURED_TABLE_NAME_ROLE
        )

        if not structured_table_name or not DuckDBService.table_exists(
            structured_table_name
        ):
            return

        if not LogAnalysis.has_column(structured_table_name, "Timestamp"):
            InfoBar.warning(
                title=self.tr("缺少时间戳"),
                content=self.tr("该日志未包含 Timestamp 列，无法进行时序分析"),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=5000,
                parent=self,
            )
            self._frequency_card.clear()
            return

    # ==================== 槽函数 ====================

    @Slot(int)
    def _on_log_selected(self, index: int):
        # 从模型获取数据
        model_index = self._extracted_log_list_model.index(index)
        structured_table_name = model_index.data(
            ExtractedLogListModel.STRUCTURED_TABLE_NAME_ROLE
        )
        log_id = model_index.data(ExtractedLogListModel.LOG_ID_ROLE)

        # 检查表是否存在
        if not DuckDBService.table_exists(structured_table_name):
            InfoBar.error(
                title=self.tr("数据未找到"),
                content=self.tr("未找到结构化表: {0}").format(structured_table_name),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=5000,
                parent=self,
            )
            return

        self._select_log_id = log_id
        granularity_index = self._granularity_combo_box.currentIndex()
        self._frequency_card.setTable(structured_table_name, granularity_index)

    @Slot(int)
    def _on_granularity_changed(self, index: int):
        log_combo_box_index = self._log_combo_box.currentIndex()
        if log_combo_box_index < 0:
            return

        model_index = self._extracted_log_list_model.index(log_combo_box_index)
        structured_table_name = model_index.data(
            ExtractedLogListModel.STRUCTURED_TABLE_NAME_ROLE
        )
        self._frequency_card.setTable(structured_table_name, index)
