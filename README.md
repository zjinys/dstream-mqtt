# MQTT data source for dstream (Spark Streaming)

A library for reading data from MQTT to [Spark Streaming](http://spark.apache.org/docs/latest/streaming-programming-guide.html).

[![Build Status](https://travis-ci.org/spark-packages/dstream-mqtt.svg?branch=master)](https://travis-ci.org/spark-packages/dstream-mqtt)
[![codecov.io](http://codecov.io/github/spark-packages/dstream-mqtt/coverage.svg?branch=master)](http://codecov.io/github/spark-packages/dstream-mqtt?branch=master)

## Requirements

This documentation is for Spark 2.0.0+. For old Spark versions, please use the old versions published with Spark.

## Linking

You can link against this library (for Spark 2.0.0+) in your program at the following coordinates:

Using SBT:

```
libraryDependencies += "org.spark-project" %% "dstream-mqtt" % "0.1.0"
```

Using Maven:

```xml
<dependency>
    <groupId>org.spark-project</groupId>
    <artifactId>dstream-mqtt_2.10</artifactId>
    <version>0.1.0</version>
</dependency>
```

This library can also be added to Spark jobs launched through `spark-shell` or `spark-submit` by using the `--packages` command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --packages org.spark-project:dstream-mqtt_2.10:0.1.0
```

Unlike using `--jars`, using `--packages` ensures that this library and its dependencies will be added to the classpath.
The `--packages` argument can also be used with `bin/spark-submit`.

This library is cross-published for Scala 2.11, so 2.11 users should replace 2.10 with 2.11 in the commands listed above.

## Examples

### Scala API

You need to extend `ActorReceiver` so as to store received data into Spark using `store(...)` methods. The supervisor strategy of
this actor can be configured to handle failures, etc.

```Scala
val lines = MQTTUtils.createStream(ssc, brokerUrl, topic)
```

See [MQTTWordCount.scala](examples/src/main/scala/org/apache/spark/examples/streaming/mqtt/MQTTWordCount.scala) for an end-to-end example.

### Java API

You need to extend `JavaActorReceiver` so as to store received data into Spark using `store(...)` methods. The supervisor strategy of
this actor can be configured to handle failures, etc.

```Java
JavaDStream<String> lines = MQTTUtils.createStream(jssc, brokerUrl, topic);
```

## Building From Source
This library is built with [SBT](http://www.scala-sbt.org/0.13/docs/Command-Line-Reference.html),
which is automatically downloaded by the included shell script. To build a JAR file simply run
`build/sbt package` from the project root.

## Testing

Just Run `build/sbt test examples/test`.
