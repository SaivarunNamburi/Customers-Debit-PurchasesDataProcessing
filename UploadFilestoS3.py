import boto3
import os
from datetime import datetime, timedelta


def upload_to_s3(filename, bucket_name, partition_key):
    s3_client = boto3.client('s3')
    key = f"raw_data/{partition_key}/{os.path.basename(filename)}"
    s3_client.upload_file(filename, bucket_name, key)


def main():
    bucket_name = 'debit-card-sampleraw-data'
    for filename in os.listdir('.'):
        if filename.endswith('.csv') and filename.startswith('transactions_'):
            date_str = filename.split('_')[1].split('.')[0]
            transaction_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            partition_key = f"date={transaction_date}"
            upload_to_s3(filename, bucket_name, partition_key)
            print(f"Uploaded {filename} to S3 bucket {bucket_name} under partition {partition_key}")


if __name__ == "__main__":
    main()
