from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 sql_stmt = "",
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.truncate = truncate
        self.sql_stmt = sql_stmt

    def execute(self, context):
        self.log.info('Load data into dimension tables')
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        else:
            redshift.run(f"INSERT INTO {self.table} {self.sql_stmnt}")
