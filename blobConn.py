import io
import pandas as pd
import datetime
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

def main():
        # Get the data from the Google Sheet
        SHEET_ID = '1-QFHeOYXZt6_wL3UElMnUgiGs9ccmKeE3rBGpfA9ru0'
        SHEET_NAME = 'AAPL'
        url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet={SHEET_NAME}'
        df = pd.read_csv(url)

        #Set connection str for blob storage
        connect_str = 'DefaultEndpointsProtocol=https;AccountName=containeradf;AccountKey=CR7/tnqSnpIDa9r2YYjn3DhN+L5QiTq1sz6GZ         Xs4JeCn/Snwra+oyAjjWLOUPfOzNKKf96HKQaWR+ASthTX+AQ==;EndpointSuffix=core.windows.net'
        my_container='output'
        time_now = datetime.datetime.now().strftime('%m_%d_%Y_%H_%M_%S')
        file_name =  "data_"+time_now +".csv"
        blob = BlobClient.from_connection_string(conn_str=connect_str, container_name=my_container, blob_name=file_name)
        #upload data to blob account
        blob.upload_blob(df.to_csv(index=False))
        print(f'CSV file created and uploaded to Azure Blob Storage')

main()