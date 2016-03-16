name := "dstream-mqtt"

organization := "org.spark-project"

scalaVersion in ThisBuild := "2.11.7"

crossScalaVersions in ThisBuild := Seq("2.10.5", "2.11.7")

version := io.Source.fromFile("version.txt").mkString.trim

spName := "org.spark-project/dstream-mqtt"

sparkVersion in ThisBuild := "2.0.0-SNAPSHOT"

val testSparkVersion = settingKey[String]("The version of Spark to test against.")

testSparkVersion in ThisBuild := sys.props.getOrElse("spark.testVersion", sparkVersion.value)

unmanagedResourceDirectories in Compile += baseDirectory.value / "python"

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

sparkComponents in ThisBuild := Seq("streaming")

libraryDependencies ++= Seq(
  "org.eclipse.paho" % "org.eclipse.paho.client.mqttv3" % "1.0.2",
  "org.apache.activemq" % "activemq-core" % "5.7.0" % "test",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "com.novocode" % "junit-interface" % "0.11" % "test",
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % "provided" classifier "tests"
)

// Display full-length stacktraces from ScalaTest:
testOptions in Test += Tests.Argument("-oF")
// Display the java unit tests in console
testOptions in Test += Tests.Argument(TestFrameworks.JUnit, "-v")

ScoverageSbtPlugin.ScoverageKeys.coverageHighlighting := {
  if (scalaBinaryVersion.value == "2.10") false
  else true
}

val root = project in file(".")

val examples = project in file("examples") dependsOn (root % "compile->compile") settings (
  libraryDependencies ++= Seq(
    // Explicitly declare them to run examples using run-main.
    "org.apache.spark" %% "spark-core" % testSparkVersion.value,
    "org.apache.spark" %% "spark-streaming" % testSparkVersion.value
  )
)

// Build a jar including test codes and dependencies for Python tests
Project.inConfig(Test)(baseAssemblySettings)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(
  includeScala = false
)

assemblyMergeStrategy in (Test, assembly) <<= (assemblyMergeStrategy in assembly) {
  (old) => {
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case PathList("META-INF", "services", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.last
  }
}

jarName in (Test, assembly) := s"${name.value}-assembly-test-${version.value}.jar"

// Remove this once Spark 2.0.0 is out
resolvers in ThisBuild += "apache-snapshots" at "https://repository.apache.org/snapshots/"

/********************
 * Release settings *
 ********************/

publishMavenStyle := true

releaseCrossBuild := true

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))

releasePublishArtifactsAction := PgpKeys.publishSigned.value

pomExtra :=
  <url>https://github.com/spark-packages/dstream-mqtt</url>
  <scm>
    <url>git@github.com:spark-packages/dstream-mqtt.git</url>
    <connection>scm:git:git@github.com:spark-packages/dstream-mqtt.git</connection>
  </scm>
  <developers>
    <developer>
      <id>marmbrus</id>
      <name>Michael Armbrust</name>
      <url>https://github.com/marmbrus</url>
    </developer>
    <developer>
      <id>tdas</id>
      <name>Tathagata Das</name>
      <url>https://github.com/tdas</url>
    </developer>
    <developer>
      <id>zsxwing</id>
      <name>Shixiong Zhu</name>
      <url>https://github.com/zsxwing</url>
    </developer>
  </developers>

bintrayReleaseOnPublish in ThisBuild := false

import ReleaseTransformations._

// Add publishing to spark packages as another step.
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion,
  pushChanges,
  releaseStepTask(spPublish)
)
