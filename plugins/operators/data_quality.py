from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id,
        checks,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id
        )

        if not self.checks:
            raise AirflowException("No data quality checks provided")

        for check in self.checks:
            result = redshift_hook.get_first(check['sql'])
            expected = check['expected_result']

            if result is None or len(result) != 1:
                raise AirflowException(
                    f"Check must return one value: {check['sql']}"
                )

            actual = result[0]

            self.log.info(
                "Data quality check: %s | Expected: %s | Actual: %s",
                check['sql'],
                expected,
                actual
            )

            if actual != expected:
                raise AirflowException(
                    f"Data quality check failed: {check['sql']}. "
                    f"Expected {expected}, received {actual}"
                )

        self.log.info("All data quality checks passed")