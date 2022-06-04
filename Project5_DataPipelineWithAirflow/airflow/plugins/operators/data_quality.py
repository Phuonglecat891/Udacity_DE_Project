from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Check by passing test SQL query and expected result
    input
        redshift_conn_id: Redshift connection ID
        lst_validate_query: List of jsons with below structure
                    validate_query: SQL query to validate Redshift's data
                    expected_result: Expected result to match against result of validate_sql
                    
            Sample:
            {
                'validate_query': "SELECT COUNT(userid) FROM users WHERE userid IS NULL",
                'expected_result': 0
            }
            """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 lst_validate_query=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.lst_validate_query = lst_validate_query

    def execute(self, context):
        self.log.info('Conecting to Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Run all valiadate queries")
        for validate_item in self.lst_validate_query:
            query = validate_item["validate_query"]
            expected_result = validate_item["expected_result"]
            
            # Get result from query
            records = redshift.get_records(query)
            result = records[0][0]
            
            if result != expected_result:
                raise ValueError(f"Data quality check failed. Expected: {expected_result} | result: {result}")
            else:
                self.log.info(f"Data quality on SQL {query} check passed with {result} records")
            
            