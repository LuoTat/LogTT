from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, unquote

import pandas as pd

from modules.logparser.parser_factory import ParserFactory, ParseResult
from .log_extract_record import LogExtractRecord
from .log_extract_repository import LogExtractRepository
from .log_source_repository import LogSourceRepository


class LogExtractService:
    """日志提取服务类"""

    def __init__(self):
        self.extract_repo = LogExtractRepository()
        self.source_repo = LogSourceRepository()
        # 输出目录：程序目录下的tmp文件夹
        self.output_dir = Path(__file__).resolve().parent.parent / "tmp"

    def extract_log(self, log_source_id: int, parser_name: str) -> ParseResult:
        """
        提取日志模板

        Args:
            log_source_id: 日志源ID
            parser_name: 解析器名称

        Returns:
            ParseResult: 解析结果
        """

        # 更新日志源的提取状态
        self._update_source_extracted(log_source_id, parser_name, result.line_count)

        # 将CSV数据导入数据库
        self._import_structured_csv(log_source_id, result.log_structured_file)

        return result

    def _update_source_extracted(self, log_source_id: int, extract_method: str, line_count: int):
        """更新日志源的提取状态"""
        with self.source_repo._get_connection() if hasattr(self.source_repo, '_get_connection') else __import__(
                'modules.db', fromlist=['get_connection']).get_connection() as conn:
            conn.execute(
                """
                UPDATE log_sources
                SET is_extracted   = 1,
                    extract_method = ?,
                    line_count     = ?
                WHERE id = ?
                """,
                [extract_method, line_count, log_source_id]
            )
            conn.commit()

    def _import_structured_csv(self, log_source_id: int, structured_file: Path):
        """
        将结构化CSV导入数据库

        Args:
            log_source_id: 日志源ID
            structured_file: 结构化CSV文件路径
        """
        if not structured_file.exists():
            raise FileNotFoundError(f"结构化文件不存在: {structured_file}")

        # 读取CSV文件
        df = pd.read_csv(structured_file)

        # 构造提取记录
        records = []
        create_time = datetime.now()

        for _, row in df.iterrows():
            record = LogExtractRecord(
                id=-1,
                log_source_id=log_source_id,
                line_num=int(row.get('LineId', 0)),
                content=str(row.get('Content', '')),
                event_id=str(row.get('EventId', '')),
                event_template=str(row.get('EventTemplate', '')),
                parameter_list=str(row.get('ParameterList', '')) if pd.notna(row.get('ParameterList')) else None,
                create_time=create_time
            )
            records.append(record)

        # 批量插入数据库
        if records:
            self.extract_repo.add_batch(records)

    def get_extracts_by_source(self, log_source_id: int):
        """获取指定日志源的所有提取记录"""
        return self.extract_repo.get_by_source_id(log_source_id)

    def delete_extracts_by_source(self, log_source_id: int):
        """删除指定日志源的所有提取记录"""
        self.extract_repo.delete_by_source_id(log_source_id)

    @staticmethod
    def get_available_parsers() -> list[str]:
        """获取所有可用的解析器名称"""
        return ParserFactory.get_all_parsers_name()