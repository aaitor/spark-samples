# spark-samples

Some project samples playing with Spark and Scala

## Running the examples

### Running in Command Line
```
mvn package exec:java -Dexec.mainClass="net.foreach.analytics.spark.WordCount"
```

### Running in Spark (stand-alone)
```
spark-submit --class net.foreach.analytics.spark.WordCount target/samples-0.0.1-SNAPSHOT.jar 
```