import duckdb
import pandas
from pathlib import Path
from functools import reduce
from operator import and_, or_


class DuckDBService:
    """DuckDB数据库服务类"""

    _DB_PATH: Path = Path(__file__).resolve().parent.parent.parent / "logtt.duckdb"

    @staticmethod
    def _build_filter_expr(filters: dict[str, list[str]] | None) -> duckdb.Expression | None:
        """根据filters构建DuckDB Expression API过滤条件"""
        if not filters:
            return None
        filter_exprs = []
        for col, values in filters.items():
            # 单列多值 → OR 链接
            col_expr = reduce(or_, [duckdb.ColumnExpression(col) == value for value in values])
            filter_exprs.append(col_expr)
        # 多列 → AND 链接
        expr = reduce(and_, filter_exprs)
        return expr

    @staticmethod
    def _build_sort_expr(sort: tuple[str, bool] | None) -> duckdb.Expression | None:
        """根据sort构建DuckDB Expression API排序条件"""
        if not sort:
            return None
        col, ascending = sort
        col_expr = duckdb.ColumnExpression(col)
        if ascending:
            return col_expr.asc()
        else:
            return col_expr.desc()

    def create_log_table_if_not_exists(self):
        """创建日志表(如果不存在)"""
        with duckdb.connect(self._DB_PATH) as conn:
            conn.sql(
                # @formatter:off
                """
                CREATE SEQUENCE IF NOT EXISTS log_id_seq START 1;
                CREATE TABLE IF NOT EXISTS log
                (
                    id             INTEGER PRIMARY KEY DEFAULT nextval('log_id_seq'),
                    log_type       VARCHAR     NOT NULL,
                    format_type    VARCHAR,
                    log_uri        VARCHAR     NOT NULL UNIQUE,
                    create_time    TIMESTAMP_S NOT NULL DEFAULT current_localtimestamp(),
                    is_extracted   BOOLEAN     NOT NULL DEFAULT FALSE,
                    extract_method VARCHAR,
                    line_count     INTEGER,
                    log_structured VARCHAR,
                    log_templates  VARCHAR
                );
                """
                # @formatter:on
            )

    def get_log_table(self) -> pandas.DataFrame:
        """获取"""
        with duckdb.connect(self._DB_PATH) as conn:
            return conn.table("log").to_df()

    def insert_log(self, log_type: str, log_uri: str, extract_method: str):
        """插入日志记录"""
        with duckdb.connect(self._DB_PATH) as conn:
            conn.sql(
                # @formatter:off
                """
                INSERT INTO log (log_type, log_uri, extract_method)
                VALUES (?, ?, ?)
                """,
                # @formatter:on
                params=[log_type, log_uri, extract_method]
            )

    def update_log(self, log_id: int, column: str, value: object):
        with duckdb.connect(self._DB_PATH) as conn:
            rel = conn.table("log")
            rel.update(
                {column: duckdb.ConstantExpression(value)},
                condition=duckdb.ColumnExpression("id") == log_id
            )

    def delete_log(self, log_id: int):
        """删除日志记录"""
        with duckdb.connect(self._DB_PATH) as conn:
            conn.sql(
                """
                DELETE
                FROM log
                WHERE id = ?;
                """,
                params=[int(log_id)]
            )

    def create_table_from_csv(self, csv_file: Path) -> str:
        """导入CSV文件到DuckDB表"""
        table_name = csv_file.stem

        with duckdb.connect(self._DB_PATH) as conn:
            rel = conn.read_csv(csv_file)
            rel.to_table(table_name)

        return table_name

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在"""
        with duckdb.connect(self._DB_PATH) as conn:
            result = conn.sql(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_name = ?
                """,
                params=[table_name]
            ).fetchone()
            return result is not None

    def drop_table(self, table_name: str):
        """删除表"""
        if self.table_exists(table_name):
            with duckdb.connect(self._DB_PATH) as conn:
                conn.sql(
                    """
                    DROP TABLE ?
                    """,
                    params=[table_name]
                )

    def get_table_row_count(self, table_name: str) -> int:
        """获取表的总行数"""
        with duckdb.connect(self._DB_PATH) as conn:
            rel = conn.table(table_name)
            return rel.shape[0]

    def get_table_columns(self, table_name: str) -> list[str]:
        """获取表的所有列名"""
        with duckdb.connect(self._DB_PATH) as conn:
            rel = conn.table(table_name)
            return rel.columns

    def fetch_page(self, table_name: str, offset: int, limit: int, filters: dict[str, list[object]] | None = None, sort: tuple[str, bool] | None = None) -> pandas.DataFrame:
        """
        分页查询数据(支持过滤和排序)
        Args:
            table_name: 表名
            offset: 偏移量
            limit: 返回行数
            filters: 过滤条件字典, key为列名, value为允许的值列表
            sort: 排序条件元组, 第一个元素为列名, 第二个元素为是否升序(True为升序, False为降序)
        """

        with duckdb.connect(self._DB_PATH) as conn:
            rel = conn.table(table_name)
            rel = rel.limit(limit, offset)
            if filter_exprs := self._build_filter_expr(filters):
                rel = rel.filter(filter_exprs)
            if sort_expr := self._build_sort_expr(sort):
                rel = rel.sort(sort_expr)
            return rel.to_df()

    def get_column_value_counts(self, table_name: str, column_name: str, search_keyword: str | None = None) -> duckdb.DuckDBPyRelation:
        # TODO:重构使用 Expression API
        """
        获取列的值和计数(用于构建过滤器)
        Args:
            table_name: 表名
            column_name: 列名
            search_keyword: 搜索关键字
        """

        with duckdb.connect(self._DB_PATH) as conn:
            rel = conn.table(table_name)
            if search_keyword:
                # 防止单引号在SQL里面必须使用两个单引号转义
                escaped_keyword = search_keyword.replace("'", "''")
                rel = rel.filter(
                    f"""
                    CAST("{column_name}" AS VARCHAR) ILIKE '%{escaped_keyword}%'
                    """
                )
            # 分组计数
            rel = rel.aggregate("count(*) AS count", f'"{column_name}"')
            # 按照计数降序排列
            rel = rel.order('count DESC')
            return rel