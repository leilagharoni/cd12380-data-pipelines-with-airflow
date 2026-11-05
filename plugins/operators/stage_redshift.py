
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id,
        aws_credentials_id,
        table,
        s3_bucket,
        s3_key,
        region,
        json_format='auto',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.json_format = json_format

    def execute(self, context):
        aws_hook = AwsBaseHook(
            aws_conn_id=self.aws_credentials_id,
            client_type='s3'
        )
        credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(
            postgres_conn_id=self.redshift_conn_id,
            log_sql=False
        )

        s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

        authorization = (
            f"aws_access_key_id={credentials.access_key};"
            f"aws_secret_access_key={credentials.secret_key}"
        )

        if credentials.token:
            authorization += f";token={credentials.token}"

        copy_sql = f"""
            COPY {self.table}
            FROM %(s3_path)s
            CREDENTIALS %(authorization)s
            FORMAT AS JSON %(json_format)s
            REGION %(region)s
            TIMEFORMAT AS 'auto';
        """

        self.log.info(
            "Replacing staging data in %s from %s",
            self.table,
            s3_path
        )

        redshift_hook.run(
            sql=[
                f"DELETE FROM {self.table}",
                copy_sql,
            ],
            parameters={
                's3_path': s3_path,
                'authorization': authorization,
                'json_format': self.json_format,
                'region': self.region,
            },
            autocommit=False,
        )

        self.log.info("Finished loading %s", self.table)




