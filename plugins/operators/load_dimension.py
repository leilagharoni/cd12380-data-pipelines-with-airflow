from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id,
        table,
        sql,
        truncate=True,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )

        self.log.info("Loading dimension table: %s", self.table)

        if self.truncate:
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")

        insert_sql = f"INSERT INTO {self.table} {self.sql}"
        redshift_hook.run(insert_sql)

        self.log.info("Finished loading: %s", self.table)
