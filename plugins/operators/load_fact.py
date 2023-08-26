from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id: str = "",
                 table_name: str = "",
                 sql: str = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql = sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Loading fact table "{self.table_name}"')
        sql_stmt = f'''
            INSERT INTO {self.table_name}
            {self.sql}
        '''
        redshift.run(sql_stmt)
        self.log.info(f'Finished loading fact table {self.table_name}')
