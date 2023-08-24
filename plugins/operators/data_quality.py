from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 table_name: str,
                 id_column_name: str,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id,
        self.table_name = table_name,
        self.id_column_name = id_column_name

    def execute(self, context):
        self.log.info(f'Performing data quality check on "{self.table_name}"')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        # Check if any rows were retrieved
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table_name}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table_name} returned no results")
        
        # Check if id column is null
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table_name} WHERE {self.id_column_name} IS NULL")
        num_of_records = records[0][0]
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f'Data quality check failed. Column "{self.id_column_name}" in table "{self.table_name}" contains {num_of_records:,} null values')        
        
        self.log.info(f'Data quality check on "{self.table_name}" passed')