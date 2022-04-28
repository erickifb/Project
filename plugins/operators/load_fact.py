from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                  # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 table,
                 sql,
                 action,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        self.redshift_conn_id = redshift_conn_id
        self.table = table,
        self.sql = sql
        self.action = action
        
    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.action == 'truncate':
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table_name}")
        
        # Insert data from staging table into dimension
        custom_sql = f"INSERT INTO {self.table} ({self.sql})"
        redshift.run(custom_sql)        
        self.log.info(f"Success: Inserting values on {self.table}, {self.task_id} loaded.")
