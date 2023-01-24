import os
import argparse
from time import time
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url

    file_name='output.parquet'

    os.system(f"wget {url} -O {file_name}")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    temp_df=pd.read_parquet(file_name, engine='pyarrow')
    temp_df.head(n=0).to_sql(name=table_name,con=engine, if_exists='replace')
    import pyarrow.parquet as pq

    pfile = pq.ParquetFile(file_name)
    count=0
    for table in pfile.iter_batches(batch_size=100000):
        df = table.to_pandas()
        count+=1
        print('Inserting chunk no #%.3f' % count)
        df.to_sql(name=table_name,con=engine,if_exists='append')
    print("Ingestion to Postgres Completed Successfully")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)