from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id,
        table,
        sql,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )

        insert_sql = f"INSERT INTO {self.table} {self.sql}"

        self.log.info("Loading fact table: %s", self.table)
        redshift_hook.run(insert_sql)
        self.log.info("Finished loading: %s", self.table)
