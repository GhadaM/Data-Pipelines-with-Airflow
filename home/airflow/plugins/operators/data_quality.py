from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    null_id_checks = [
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result': 0}
    ]

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        fail_count = 0
        fail_record = []
        
        for table_check in DataQualityOperator.null_id_checks:
            record_query = table_check.get('check_sql')
            record_expected = table_check.get('expected_result')
            
            self.log.info(f"Running {record_query}")

            records = redshift.get_records(record_query)
            if records[0][0] != record_expected:
                fail_count += 1
                fail_record.append(record_query)

        if fail_count > 0:
            self.log.info("Data quality check failed.")
            self.log.info(fail_record)
        else:
            self.log.info("Data quality check done.")
