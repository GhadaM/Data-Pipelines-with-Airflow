from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table="",
                 query="",
                 redshift_conn_id="",
                 append=False,
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table = table
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.append=append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
       
        if self.append == False:
            
            self.log.info(f"Clearing data from {self.table} table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Loading data to {self.table} Table")
        formatted_sql = """
        Insert Into {table} {query}
        """.format(
            table=self.table,
            query=self.query
        )
       
        redshift.run(formatted_sql)
