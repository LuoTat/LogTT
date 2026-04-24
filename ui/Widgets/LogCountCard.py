from modules.duckdb_service import DuckDBService
from PySide6.QtCore import Qt
from PySide6.QtWidgets import QHBoxLayout, QVBoxLayout
from qfluentwidgets import (
    BodyLabel,
    CardWidget,
    FluentIcon,
    IconWidget,
    TitleLabel,
)


class LogCountCard(CardWidget):
    """统计指标卡片，左侧显示日志总数，右侧显示模板数量"""

    def __init__(self, parent=None):
        super().__init__(parent)

        self._main_layout = QHBoxLayout(self)
        self._main_layout.setContentsMargins(24, 24, 24, 24)
        self._main_layout.setSpacing(16)

        # 左侧：日志总数
        self._init_log_value()

        # 分隔线
        separator = BodyLabel("|", self)
        separator.setStyleSheet("color: #d0d0d0; font-size: 40px; font-weight: 100;")
        separator.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._main_layout.addWidget(separator)

        # 右侧：模板数量
        self._init_template_value()

    # ==================== 私有方法 ====================
    def _init_log_value(self):
        """初始化日志总数的数值显示"""
        layout = QVBoxLayout()
        layout.setSpacing(4)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        header_layout = QHBoxLayout()
        header_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        icon_widget = IconWidget(FluentIcon.DOCUMENT, self)
        icon_widget.setFixedSize(16, 16)
        header_layout.addWidget(icon_widget)
        header_layout.addSpacing(4)

        label = BodyLabel(self.tr("日志总数"), self)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header_layout.addWidget(label)

        layout.addLayout(header_layout)

        self._log_value_label = TitleLabel("--", self)
        self._log_value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._log_value_label)

        self._main_layout.addLayout(layout)

    def _init_template_value(self):
        """初始化模板数量的数值显示"""
        layout = QVBoxLayout()
        layout.setSpacing(4)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        header_layout = QHBoxLayout()
        header_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        icon_widget = IconWidget(FluentIcon.PIE_SINGLE, self)
        icon_widget.setFixedSize(16, 16)
        header_layout.addWidget(icon_widget)
        header_layout.addSpacing(4)

        label = BodyLabel(self.tr("模板数量"), self)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header_layout.addWidget(label)

        layout.addLayout(header_layout)

        self._template_value_label = TitleLabel("--", self)
        self._template_value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._template_value_label)

        self._main_layout.addLayout(layout)

    # ==================== 公共方法 ====================

    def setTable(
        self,
        structured_table_name: str,
        templates_table_name: str,
    ):
        """设置表名并刷新数值"""
        log_count = DuckDBService.get_table_row_count(structured_table_name)
        template_count = DuckDBService.get_table_row_count(templates_table_name)
        self._log_value_label.setText(f"{log_count:,}")
        self._template_value_label.setText(f"{template_count:,}")

    def clear(self):
        """清空数值显示"""
        self._log_value_label.setText("--")
        self._template_value_label.setText("--")
