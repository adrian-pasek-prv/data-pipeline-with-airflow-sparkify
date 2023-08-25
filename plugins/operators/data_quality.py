from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str = "",
                 staging_table_names: list = [],
                 table_column_map: dict = {},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.staging_table_names = staging_table_names
        self.table_column_map = table_column_map


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.staging_table_names:
            self.log.info(f'Performing data quality check on "{table}"')
            # Check if any rows were retrieved
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            self.log.info(f'Data quality check on "{table}" passed')
        
        for table_name, id_column_name in self.table_column_map.items():
            self.log.info(f'Performing data quality check on "{table_name}"')
            # Check if id column is null
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table_name} WHERE {id_column_name} IS NULL")
            num_of_records = records[0][0]
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f'Data quality check failed. Column "{id_column_name}" in table "{table_name}" contains {num_of_records:,} null values')        
            self.log.info(f'Data quality check on "{table_name}" passed')