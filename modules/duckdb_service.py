from functools import reduce
from operator import and_

import duckdb
import polars

from .constants import DB_PATH


class DuckDBService:
    """DuckDB数据库服务类"""

    # ==================== 日志管理用 ====================

    @staticmethod
    def create_log_table_if_not_exists():
        """创建日志表(如果不存在)"""
        with duckdb.connect(DB_PATH) as conn:
            conn.sql(
                # @formatter:off
                """
                CREATE SEQUENCE IF NOT EXISTS log_id_seq START 1;
                CREATE TABLE IF NOT EXISTS log
                (
                    id                    INTEGER PRIMARY KEY DEFAULT nextval('log_id_seq'),
                    log_type              VARCHAR     NOT NULL,
                    format_type           VARCHAR,
                    log_uri               VARCHAR     NOT NULL UNIQUE,
                    create_time           TIMESTAMP_S NOT NULL DEFAULT current_localtimestamp(),
                    is_extracted          BOOLEAN     NOT NULL DEFAULT FALSE,
                    extract_method        VARCHAR,
                    line_count            INTEGER,
                    structured_table_name VARCHAR GENERATED ALWAYS AS (CAST(id AS VARCHAR) || '_S'),
                    templates_table_name  VARCHAR GENERATED ALWAYS AS (CAST(id AS VARCHAR) || '_T')
                );
                """
                # @formatter:on
            )

    @staticmethod
    def get_log_table() -> list[tuple]:
        """获取日志列表"""
        with duckdb.connect(DB_PATH) as conn:
            return conn.table("log").fetchall()

    @staticmethod
    def get_extracted_log_table() -> list[tuple]:
        """获取已提取的日志列表"""
        with duckdb.connect(DB_PATH) as conn:
            rel = conn.table("log")
            rel = rel.filter(duckdb.ColumnExpression("is_extracted") == True)
            rel = rel.select(
                "id",
                "log_uri",
                "structured_table_name",
                "templates_table_name",
            )
            return rel.fetchall()

    @staticmethod
    def insert_log(log_type: str, log_uri: str, extract_method: str):
        """插入日志记录"""
        with duckdb.connect(DB_PATH) as conn:
            conn.sql(
                # @formatter:off
                """
                INSERT INTO log (log_type, log_uri, extract_method)
                VALUES (?, ?, ?)
                """,
                # @formatter:on
                params=[log_type, log_uri, extract_method],
            )

    @staticmethod
    def update_log(log_id: int, column_name: str, value: object):
        with duckdb.connect(DB_PATH) as conn:
            rel = conn.table("log")
            rel.update(
                {column_name: duckdb.ConstantExpression(value)},
                condition=duckdb.ColumnExpression("id") == log_id,
            )

    @staticmethod
    def delete_log(log_id: int):
        """删除日志记录"""
        with duckdb.connect(DB_PATH) as conn:
            conn.sql(
                """
                DELETE
                FROM log
                WHERE id = ?;
                """,
                params=[int(log_id)],
            )

    # ==================== CSV表格显示用 ====================

    @staticmethod
    def _build_filter_expr(
        filters: dict[str, list[object]],
    ) -> duckdb.Expression:
        """构建精确匹配过滤条件"""
        filter_exprs = []
        for col, values in filters.items():
            # 单列多值 → IN 列表
            col_expr = duckdb.ColumnExpression(col).isin(
                *[duckdb.ConstantExpression(v) for v in values]
            )
            filter_exprs.append(col_expr)
        # 多列 → AND 链接
        expr = reduce(and_, filter_exprs)
        return expr

    @staticmethod
    def _build_sort_expr(sort: tuple[str, bool]) -> duckdb.Expression:
        """根据sort构建DuckDB Expression API排序条件"""
        col, ascending = sort
        col_expr = duckdb.ColumnExpression(col)
        if ascending:
            return col_expr.asc()
        else:
            return col_expr.desc()

    @staticmethod
    def create_table_from_polars(df: polars.DataFrame, table_name: str):
        """导入Polars DataFrame到DuckDB表"""
        with duckdb.connect(DB_PATH) as conn:
            rel = conn.from_arrow(df.to_arrow())
            rel.to_table(table_name)

    @staticmethod
    def fetch_csv_table(
        table_name: str,
        offset: int,
        limit: int,
        filters: dict[str, list[object]] | None = None,
    ) -> tuple[polars.DataFrame, int]:
        """
        分页查询csv数据(支持过滤和排序)
        Args:
            table_name: 表名
            offset: 偏移量
            limit: 返回行数
            filters: 过滤条件字典, key为列名, value为允许的值列表

        Returns:
            返回查询到的DataFrame和过滤后的总行数
        """

        with duckdb.connect(DB_PATH) as conn:
            rel = conn.table(table_name)
            total_count = rel.shape[0]
            if filters:
                rel = rel.filter(DuckDBService._build_filter_expr(filters))
                total_count = rel.shape[0]
            rel = rel.limit(limit, offset)
            return rel.pl(), total_count

    # ==================== CSV表格过滤器用 ====================

    @staticmethod
    def _build_like_filter_expr(
        column_name: str,
        keyword: str,
    ) -> duckdb.Expression:
        """构建模糊匹配过滤条件"""
        return duckdb.FunctionExpression(
            "regexp_matches",
            duckdb.ColumnExpression(column_name),
            duckdb.ConstantExpression(f"(?i){keyword}"),  # (?i) = 不区分大小写标志
        )

    @staticmethod
    def fetch_filter_table(
        table_name: str,
        column_name: str,
        offset: int,
        limit: int,
        keyword: str | None = None,
        other_filters: dict[str, list[object]] | None = None,
    ) -> tuple[polars.DataFrame, int]:
        """
        获取列的值和计数(用于构建过滤器)
        Args:
            table_name: 表名
            column_name: 列名
            offset: 偏移量
            limit: 返回行数
            keyword: 搜索关键字
            other_filters: 其他列的过滤条件字典, key为列名, value为允许的值列表
        """

        with duckdb.connect(DB_PATH) as conn:
            rel = conn.table(table_name)
            if other_filters:
                rel = rel.filter(DuckDBService._build_filter_expr(other_filters))
            rel = rel.aggregate(
                [
                    duckdb.ColumnExpression(column_name),
                    duckdb.FunctionExpression("count").alias("count"),
                ],
                column_name,
            )
            total_count = rel.shape[0]
            if keyword:
                rel = rel.filter(
                    DuckDBService._build_like_filter_expr(column_name, keyword)
                )
                total_count = rel.shape[0]

            rel = rel.sort(DuckDBService._build_sort_expr(("count", False)))
            rel = rel.limit(limit, offset)
            return rel.pl(), total_count

    # ==================== 通用方法 ====================

    @staticmethod
    def table_exists(table_name: str) -> bool:
        """检查表是否存在"""
        with duckdb.connect(DB_PATH) as conn:
            result = conn.sql(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = ?
                """,
                params=[table_name],
            ).fetchone()
            return result is not None

    @staticmethod
    def drop_table(table_name: str):
        """删除表"""
        with duckdb.connect(DB_PATH) as conn:
            conn.sql(
                f"""
                DROP TABLE IF EXISTS "{table_name}";
                """
            )

    @staticmethod
    def get_table_row_count(table_name: str) -> int:
        """获取表的总行数"""
        with duckdb.connect(DB_PATH) as conn:
            rel = conn.table(table_name)
            return rel.shape[0]

    @staticmethod
    def get_table_columns(table_name: str) -> list[str]:
        """获取表的所有列名"""
        with duckdb.connect(DB_PATH) as conn:
            rel = conn.table(table_name)
            return rel.columns