import os.path
from utils.FileHandler import FileHandler
from utils.MssqlHandler import xlsx_to_sql
from datetime import datetime, timedelta
from pendulum import datetime
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator  

@dag('etl_process', 
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retry_delay': timedelta(minutes=5),
            "start_date": datetime(2022,5,12),
        },
        catchup = False,
        schedule_interval= '0 0 * * *'
)
def main():

    """
    
    Function principal main function that executes 
    each task of the instructions on the challenge

   """
   #Variables 
   
    path_data = os.path.join(os.environ["AIRFLOW_HOME"],'dags','data')
    file = FileHandler()
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_ipc_aperturas.xls"
    cheatsheet = "Índices aperturas"
    xls_name = "sh_ipc_aperturas.xls"
    regions_name = ["Región_GBA", 
                "Región_Pampeana", 
                "Región_Noroeste", 
                "Región_Noreste", 
                "Región_Cuyo",
                "Región_Patagonia"]
    bucket_name = "reba-test"
     
    # Tasks 
     
     
    #start etl 
    start = EmptyOperator(task_id ="Start")
           
    #Download xls file
    @task()
    def extract(url: str):
        return file.get_xls(url)
    
    
    #Detect xls file in localsystem
    wait_file = FileSensor(fs_conn_id="file-xls-system", 
                           task_id="wait_for_xls", 
                           filepath="{}/{}".format(path_data, xls_name))
    
    #Apply transforms and cleanup to xlsx files 
    @task()
    def transform(filename: str, cheatsheet: str):
        return file.get_cheatsheet(filename, cheatsheet)
    
    # Create table to load with data from xlsx files 
    create_table_region= MsSqlOperator(
                            task_id='create_table_regions',
                            mssql_conn_id='mssql-airflow',
                            sql="db/create_table_regions.sql"
                            )    
    
    #Read xlsx files and write then in table
    @task(max_active_tis_per_dag=1)
    def insert_regions_db(region):
        mssql_hook = MsSqlHook(mssql_conn_id="mssql-airflow")
        conn = mssql_hook.get_sqlalchemy_engine()
        xlsx_to_sql(path_data,region,conn)
    
    
    #Load xls file to Google cloud Storage
    load_xls_to_gcs = LocalFilesystemToGCSOperator(
                        task_id="load_xls_to_gcs",
                        gcp_conn_id="airflow-connection-gcp",
                        dst="data/",
                        src="{}/{}".format(path_data,xls_name),
                        bucket=bucket_name
                        )
    #Finish etl
    finish = EmptyOperator(task_id ="Finish")
    
    #Dependencies and orchestrations
    
    downmload_xls = extract(url) 
    clean_xlsx = transform(xls_name, cheatsheet)
    insert_rows = insert_regions_db.expand(region=regions_name)
    
    #Ordered dags 
    start >> downmload_xls >> wait_file >> clean_xlsx
    
    for region in regions_name:
        
        wait_file_regions = FileSensor(fs_conn_id="file-xlsx-system",
                                       task_id="wait_for_{}.xlsx".format(region), 
                                       filepath="{}/{}.xlsx".format(path_data, region))
        
        clean_xlsx >> wait_file_regions >> load_xls_to_gcs
 
        
    load_xls_to_gcs >> create_table_region >> insert_rows >> finish
   
#Call mayor function
main()