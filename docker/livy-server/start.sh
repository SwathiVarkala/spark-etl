export SPARK_MASTER_HOST=`hostname`

. "/spark/sbin/spark-config.sh"

. "/spark/bin/load-spark-env.sh"

mkdir -p $SPARK_MASTER_LOG

export SPARK_HOME=/spark

ln -sf /dev/stdout $SPARK_MASTER_LOG/spark-master.out

cd /spark/bin && /spark/sbin/../bin/spark-class org.apache.spark.deploy.master.Master \
    --ip $SPARK_MASTER_HOST --port $SPARK_MASTER_PORT --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG/spark-master.out &

echo "Starting the livy server"

echo "livy.spark.master = spark://spark-master:7077" > /livy/apache-livy-0.6.0-incubating-bin/conf/livy.conf

echo "livy.file.local-dir-whitelist = /tmp/jars" >>  /livy/apache-livy-0.6.0-incubating-bin/conf/livy.conf

/livy/apache-livy-0.6.0-incubating-bin/bin/livy-server start &

echo "Starting spark history server"
echo "
spark.eventLog.enabled           true
spark.eventLog.dir               file:///tmp/spark-events
spark.history.fs.logDirectory       file:///tmp/spark-events
" >> /spark/conf/spark-defaults.conf
mkdir -p /tmp/spark-events
. "/spark/sbin/start-history-server.sh" &

tail -f /dev/null
