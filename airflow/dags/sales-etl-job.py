from __future__ import print_function

import os
from datetime import datetime, timedelta

from airflow.contrib.sensors.file_sensor import FileSensor
#from airflow.operators.sensors import HdfsSensor
from airflow.operators.sensors import HdfsSensor
from airflow.models import DAG
from airflow.operators import LivyOperator
from airflow.operators.bash_operator import BashOperator

"""
Pre-run Steps:
1. Open the Airflow WebServer
2. Navigate to Admin -> Connections
3. Add a new connection
    1. Set the Conn Id as "livy_http_conn"
    2. Set the Conn Type as "http"
    3. Set the host as spark-master
    4. Set the port as 8998
    5. Save
"""

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(""
                                                                ".py", "")

HTTP_CONN_ID = "livy_http_conn"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None,
          start_date=(datetime(2020, 3, 16, 0, 0, 0, 0)), catchup=False)

input_path = f'{os.environ["INPUT_PATH"]}/sales'
raw_path = f'{os.environ["RAW_PATH"]}/sales/' + '{{ ts_nodash }}'
destination_path = f'{os.environ["DESTINATION_PATH"]}/sales'
error_path = f'{os.environ["ERROR_PATH"]}/sales'

# file_sensor = FileSensor(task_id='file_exists',
#                          dag=dag,
#                          filepath=input_path,
#                          timeout=300,
#                          poke_interval=10)

file_sensor = HdfsSensor(
    task_id='file_exists',
    filepath=input_path,
    hdfs_conn_id='hdfs_default',
    dag=dag)

extract_data = BashOperator(
    task_id='extract_data',
    dag=dag,
    bash_command='mkdir -p ' + raw_path + ' && ' + ' cp ' + input_path + '/* ' + raw_path
)

transform_data = LivyOperator(
    task_id='transform_data',
    dag=dag,
    livy_conn_id=HTTP_CONN_ID,
    file='file:///tmp/jars/spark-etl-assembly-0.1.0-SNAPSHOT.jar',
    num_executors=1,
    conf={
        'spark.shuffle.compress': 'false',
    },
    class_name='thoughtworks.sales.TransformData',
    polling_interval=10,
#     args=['--inputPath=' + raw_path, f'--outputPath={destination_path}_TMP', '--errorPath=' + error_path]
    args=['--inputPath=' + input_path, f'--outputPath={destination_path}_TMP', '--errorPath=' + error_path]
)

load_data = BashOperator(
    task_id='load_data',
    dag=dag,
    bash_command=f'hdfs dfs -rm -R {destination_path} && hdfs dfs -mv {destination_path}_TMP {destination_path} && hdfs dfs -rm -r {destination_path}_TMP'
)

file_sensor >> extract_data >> transform_data >> load_data
