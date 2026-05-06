from modules.duckdb_service import DuckDBService
from PySide6.QtCore import (
    Qt,
    Slot,
)
from PySide6.QtGui import QShowEvent
from PySide6.QtWidgets import (
    QHBoxLayout,
    QVBoxLayout,
    QWidget,
)
from qfluentwidgets import BodyLabel, InfoBar, InfoBarPosition, SmoothScrollArea
from qfluentwidgets.components import ModelComboBox

from modules.models import ExtractedLogListModel
from ui.Widgets import TemplateEmbeddingScatterCard


class ClusterVisualizationPage(QWidget):
    """聚类可视化界面"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("ClusterVisualizationPage")

        # 主布局
        self._main_layout = QVBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        # 初始化日志列表模型
        self._extracted_log_list_model = ExtractedLogListModel(self)
        self._select_log_id = -1
        self._init_toolbar()
        self._init_card()

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
            self._template_cluster_card.clear()

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
        self._main_layout.addLayout(tool_bar_layout)

    def _init_card(self):
        """初始化统计卡片"""
        self._scroll_area = SmoothScrollArea(self)
        self._scroll_area.setWidgetResizable(True)

        self._scroll_widget = QWidget()
        self._card_layout = QVBoxLayout(self._scroll_widget)

        self._template_cluster_card = TemplateEmbeddingScatterCard(parent=self)
        self._card_layout.addWidget(self._template_cluster_card)

        self._scroll_area.setWidget(self._scroll_widget)
        self._scroll_area.enableTransparentBackground()

        self._main_layout.addWidget(self._scroll_area)

    # ==================== 槽函数 ====================

    @Slot(int)
    def _on_log_selected(self, index: int):
        # 从模型获取数据
        model_index = self._extracted_log_list_model.index(index)
        templates_table_name = model_index.data(
            ExtractedLogListModel.TEMPLATES_TABLE_NAME_ROLE
        )
        log_id = model_index.data(ExtractedLogListModel.LOG_ID_ROLE)

        # 检查表是否存在
        if not DuckDBService.table_exists(templates_table_name):
            InfoBar.error(
                title=self.tr("数据未找到"),
                content=self.tr("未找到结构化表: {0}").format(templates_table_name),
                orient=Qt.Orientation.Horizontal,
                isClosable=True,
                position=InfoBarPosition.TOP,
                duration=5000,
                parent=self,
            )
            return

        self._select_log_id = log_id
        self._template_cluster_card.setTable(templates_table_name)
