from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials = "",
                 redshift_conn_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key= "",
                 file_type = "",
                 ignore_headers = 1,
                 delimeter = ",",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key= s3_key
        self.file_type = file_type
        self.ignore_headers = ignore_headers
        self.delimeter = delimeter

    def execute(self, context):
        self.log.info('copying data and moving to redshift')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        redshift.run(f"DELETE FROM {self.table}")
        sql_copy = """
            COPY {} 
            FROM '{}'
            ACCESS_KEY_ID {}
            SECRET_ACCESS_KEY {}
            IGNORE HEADER {}
            DELIMETER {}
        """.format(self.table, s3_path, credentials.access_key, credentials.secret_access_key, self.ignore_headers, self.delimiter)
        redshift.run(sql_copy)
                





