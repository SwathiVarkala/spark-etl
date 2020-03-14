from __future__ import print_function

from airflow.models import DAG
from datetime import datetime, timedelta
import os

from airflow.operators import LivyOperator

"""
Pre-run Steps:
1. Open the Airflow WebServer
2. Navigate to Admin -> Connections
3. Add a new connection
    1. Set the Conn Id as "livy_http_conn"
    2. Set the Conn Type as "http"
    3. Set the host
    4. Set the port (default for livy is 8998)
    5. Save
"""

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(""
                                                                ".py", "")

HTTP_CONN_ID = "livy_http_conn"
SESSION_TYPE = "spark"
SPARK_SCRIPT = """
import java.util
println("sc: " + sc)
val rdd = sc.parallelize(1 to 5)
val rddFiltered = rdd.filter(entry => entry > 3)
println("process started")
println(util.Arrays.toString(rddFiltered.collect()))
println("process ended")
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=None, start_date=(datetime.now() - timedelta(minutes=1)))

livy_java_task = LivyOperator(
    task_id='spark_job',
    dag=dag,
    livy_conn_id='livy_http_conn',
    file='file:///tmp/jars/tw-pipeline-assembly-0.1.0-SNAPSHOT.jar',
    num_executors=1,
    conf={
        'spark.shuffle.compress': 'false',
    },
    class_name='thoughtworks.wordcount.Test',
)