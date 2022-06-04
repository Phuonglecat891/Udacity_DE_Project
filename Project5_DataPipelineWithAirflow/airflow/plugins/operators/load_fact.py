from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Process data from staging tables into fact table on Redshift.
    input
        redshift_conn_id: Redshift connection ID
        table: Target table 
        select_query: SQL select query for getting data to load into target table
        truncate_mode: Enable truncate table before insert, default False
    """  
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_query="",
                 truncate_mode=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_query = select_query
        self.truncate_mode = truncate_mode

    def execute(self, context):
        self.log.info('Conecting to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_mode:
            self.log.info(f"Insert table {self.table} with truncate mode")
            redshift.run(f"DELETE FROM {self.table}")
        else:
            self.log.info(f"Insert table {self.table} with append mode")
            
        query = f"""
            INSERT INTO {self.table}
            {self.select_query}
        """
        
        self.log.info("Processing data from staging tables into dimension tables on Redshift")
        redshift.run(query)
            
        
