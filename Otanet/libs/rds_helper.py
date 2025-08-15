import psycopg2
import boto3

class RDSHelper:
    def __init__(self):
        self.db_host = 'pg-otanet-devo.cqrmaae6m4xg.us-east-1.rds.amazonaws.com'
        self.db_user = 'postgres'
        self.db_port = 5432
        self.region = "us-east-1"
        self.db_name = "pg-otanet-devo"
        self.rds_client = boto3.client('rds', region_name=self.region)
        self.token = self.rds_client.generate_db_auth_token(
            DBHostname=self.db_host, 
            Port=self.db_port, 
            DBUsername=self.db_user, 
            Region=self.region)
        self.conn = psycopg2.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.token,
            database=self.db_name,
            port=self.db_port,
            ssl={'ca': 'rds-combined-ca-bundle.pem'}
        )

    def create_table(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            title VARCHAR(100) NOT NULL,
            description VARCHAR(2000),
            tags TEXT []
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(create_table_query)
