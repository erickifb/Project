from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id = '',
                 tables = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables,
        
    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for tbl in range(len(self.tables)):
            table = list(self.tables.keys())[tbl][0]
            field = list(self.tables.values())[tbl][1]
            
            # Quality check 1 - check that dimension tables have rows
            custom_sql = "SELECT Count(*) FROM {}".format(table)
            rows = redshift.get_first(custom_sql) 
            self.log.info('Table: {} has {} rows'.format(table, rows))
            
            # Quality check 2 - check that key fields dont have null entries
            custom_sql = "SELECT count(*) FROM {} where {} IS NULL".format(table, field)
            rows = redshift.get_first(custom_sql) 
            self.log.info('Field: {} in table: {} has {} NULL rows'.format(field, table, rows))