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
            sslmode="verify-full",
            sslrootcert="us-east-1-bundle.pem"
        )

    def create_table(self, table_name):
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            title VARCHAR(100) NOT NULL,
            description VARCHAR(2000),
            tags TEXT [],
            hash VARCHAR(255) NOT NULL
        );
        """
        with self.conn.cursor() as cursor:
            cursor.execute(create_table_query)
    
    def insert_manga_metadata(self, table_name, manga):
        insert_metadata_query = f"""
        INSERT INTO {table_name} (title, description, tags, hash) 
        VALUES (%s,%s,%s,%s);"""

        insert_data = (manga.get_title(), 
                       manga.get_description(),
                       manga.get_tags(),
                       manga.get_id())
        
        with self.conn.cursor() as cursor:
            try:
                cursor.execute(insert_metadata_query, insert_data)
            except psycopg2.Error as e:
                print(f"Error executing INSERT statement: {e}")
                self.conn.rollback()
            self.conn.commit()
            print("Data inserted successfully")
        
    
