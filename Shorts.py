# Function to calculate day count excluding weekends

def calculate_day_count(start_date, end_date):
    df = pd.DataFrame(pd.date_range(start=start_date, end=end_date), columns=['Date'])
    day_count = 1
    for index, row in df.iterrows():
        day_of_week = row['Date'].dayofweek
        if day_of_week < 5:
            df.loc[index, 'day_count'] = day_count
            day_count += 1
    return df
#--------------------------------------------------------------------------------------------------------------

#SQL connection by SQL alchemy : push the dataframe to SQL
server = 'tcp:137.135.93.131,1433'
database = 'JCGCS'
username = 'Saurabh_p'
password = 'sp@6awPe'
driver = '{ODBC Driver 18 for SQL Server}'
trust_server_certificate = 'yes'
conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate={trust_server_certificate}"
connection_url = URL.create(
    "mssql+pyodbc",
    query={"odbc_connect": conn_str}
)
# logging.info("connection successfully")
engine = create_engine(connection_url)
query = "SELECT * FROM [Daily_Attendance]"
db_df = pd.read_sql(query, engine)

#make unique key in Dataframe
db_df['Key'] = db_df['Students_id'].astype(str) + '_' + db_df['Date'].astype(str) + '_' + db_df['Class_name'].astype(str)
attendance_df['Key'] = attendance_df['Students_id'].astype(str) + '_' + attendance_df['Date'].astype(str) + '_' + attendance_df['Class_name'].astype(str)

# comparison : return only those which data is not in databse(db_df)
df_not_in_db = attendance_df[~attendance_df['Key'].isin(db_df['Key'])]

with engine.connect() as conn:
    with conn.begin():
        df_not_in_db.to_sql('Daily_Attendance', con=conn, if_exists='append', index=False)
        print("Row inserted successfully")

#--------------------------------------------------------------------------------------------------------------

#send dataframe in CSV format in azure blob 

def azure_upload_df(container=None, dataframe=None, filename=None):
    """
    Upload DataFrame to Azure Blob Storage for given container
    Keyword arguments:
    container -- the container name (default None)
    dataframe -- the dataframe(df) object (default None)
    filename -- the filename to use for the blob (default None)
    
    Function uses following enviornment variables 
    AZURE_STORAGE_CONNECTION_STRING -- the connection string for the account
    OUTPUT -- the ouput folder name
    eg: upload_file(container="test", dataframe=df, filename="test.csv")
    """
    if all([container, len(dataframe), filename]):
        file_path = f"{os.getenv('demographics')}"
        upload_file_path = os.path.join(file_path, f"{filename}.csv")
        connect_str = f"{os.environ['Azure_Storage']}"   #Azure_Storage has created in environment connection string
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        blob_client = blob_service_client.get_blob_client(
            container=container, blob=upload_file_path
        )

        output = dataframe.to_csv(index=False, encoding="utf-8")
        blob_client.upload_blob(output, blob_type="BlockBlob", overwrite=True)
    

azure_upload_df('files/JCGCS',demographic_df, 'demographics')

#--------------------------------------------------------------------------------------------------------------

# find Current year in between aug to aug

current_date = datetime.datetime.now()
if current_date.month >= 8:
    start_year = current_date.year
    end_year = current_date.year + 1
else:
    start_year = current_date.year - 1
    end_year = current_date.year
  
demographic_df['Year'] = f'{start_year}-{end_year}'


#-----------------------------------------------------------------------------------------
#SFTP to azure blob

#dependency 
import pysftp
from azure.storage.blob import BlobServiceClient

# SFTP configuration
sftp_host = 'your_sftp_host'
sftp_username = 'your_sftp_username'
sftp_password = 'your_sftp_password'
sftp_file_path = '/path/to/your/file.txt'
local_file_path = 'file.txt'  # Temporary local file path

# Azure Blob Storage configuration
connection_string = 'your_azure_connection_string'
container_name = 'your_container_name'
blob_name = 'file.txt'  # Name in Azure Blob

# Step 1: Download file from SFTP
with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password) as sftp:
    sftp.get(sftp_file_path, local_file_path)
    print(f'Downloaded {sftp_file_path} from SFTP to {local_file_path}')

# Step 2: Upload file to Azure Blob Storage
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

with open(local_file_path, 'rb') as data:
    blob_client.upload_blob(data, overwrite=True)
    print(f'Uploaded {local_file_path} to Azure Blob Storage as {blob_name}')

# Clean up: Optionally, delete the local file after uploading
import os
os.remove(local_file_path)
print(f'Removed temporary file {local_file_path}')




