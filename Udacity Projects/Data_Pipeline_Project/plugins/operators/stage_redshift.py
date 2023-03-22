from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials = "",
                 redshift_conn_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key= "",
                 json_path = "",
                 ignore_headers = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials = aws_credentials
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key= s3_key
        self.ignore_headers = ignore_headers
        self.json_path = json_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        
        self.log.info('Deleting data from Redshift tables')
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info('Copying data from S3 to Redshift')
        sql_copy = """
            COPY {} 
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            JSON '{}'
        """.format(self.table, s3_path, credentials.access_key, credentials.secret_key, self.json_path)
        redshift.run(sql_copy)
                





