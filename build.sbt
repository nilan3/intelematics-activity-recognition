organization := "com.intelematics.spark-pipelines"

name := "intelematics"

version := "1.1.0-20190507-211500"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

val sparkKafkaVersion = "2.2.2"

val scalaTestVersion = "3.0.1"

val sparkKafkaStreaming = "1.6.1"

val snakeYaml = "1.23"

resolvers  += "MavenRepository" at "http://central.maven.org/maven2"

resolvers  +="Sbt plugins" at "https://dl.bintray.com/sbt/sbt-plugin-releases"

fork in Test := false

parallelExecution in Test := false

testForkedParallel in Test := false

lazy val excludeJars = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  ("org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion)
    .exclude("org.apache.spark", "spark-tags_2.11")
    .exclude("org.spark-project.spark", "unused")
    .exclude("org.slf4j", "slf4j-api")
    .exclude("org.slf4j", "slf4j-log4j12"),
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  ("org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion)
    .exclude("org.apache.spark", "spark-tags_2.11")
    .exclude("org.spark-project.spark", "unused")
    .exclude("org.slf4j", "slf4j-api")
    .exclude("org.slf4j", "slf4j-log4j12"),
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

libraryDependencies ++= Seq(
  "org.yaml" % "snakeyaml" % snakeYaml,
  "com.github.tototoshi" %% "scala-csv" % "1.3.5"
)

libraryDependencies ++= Seq(
  "org.nd4j" % "nd4j-native" % "0.9.1"
)

libraryDependencies += "org.scalaj" % "scalaj-http_2.11" % "2.3.0"

libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.4"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
  case x => MergeStrategy.first
}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   