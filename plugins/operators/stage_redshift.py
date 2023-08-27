from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # What field will be templateable, in what field can I use Airflow context variables
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        JSON '{}'
        REGION '{}'
    """

    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 table_name: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 json_path: str = "auto",
                 aws_region: str = "",
                 ignore_headers: int = 1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.aws_region = aws_region
        self.ignore_headers = ignore_headers
        

    def execute(self, context):
        aws_hook = AwsGenericHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table_name))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        if self.json_path != "auto":
            json_path = "s3://{}/{}".format(self.s3_bucket, self.json_path)
        else:
            json_path = self.json_path
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            json_path,
            self.aws_region
        )
        redshift.run(formatted_sql)





