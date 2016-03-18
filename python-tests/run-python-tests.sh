#!/bin/bash -e

TRAVIS_SCALA_VERSION=${TRAVIS_SCALA_VERSION:-2.11.7}

if [[ "$TRAVIS_SCALA_VERSION" != "2.11.7" ]]; then
    echo "TRAVIS_SCALA_VERSION must be 2.11.7 to test Python codes"
    exit 0
fi

TEST_SPARK_VERSION=${TEST_SPARK_VERSION:-2.0.0-SNAPSHOT}

cd $(dirname $0)

mkdir -p lib && cd lib && test -d spark-${TEST_SPARK_VERSION}-bin-hadoop2.4 || \
  wget https://people.apache.org/~pwendell/spark-nightly/spark-master-bin/latest/spark-${TEST_SPARK_VERSION}-bin-hadoop2.4.tgz && tar zxf spark-${TEST_SPARK_VERSION}-bin-hadoop2.4.tgz
cd ../..

VERSION=$(cat version.txt)

if [[ "$TRAVIS" = "true" ]]; then
    SBT=sbt
else
    SBT=build/sbt
fi

sbt -Dspark.testVersion=$TEST_SPARK_VERSION ++${TRAVIS_SCALA_VERSION} clean update package test:assembly
python-tests/lib/spark-${TEST_SPARK_VERSION}-bin-hadoop2.4/bin/spark-submit \
    --master local[*] \
    --driver-memory 512m \
    --jars target/scala-2.11/dstream-mqtt-assembly-test-${VERSION}.jar \
    --py-files target/scala-2.11/dstream-mqtt_2.11-${VERSION}.jar \
    python-tests/tests.py

