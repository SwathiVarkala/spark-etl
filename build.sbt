
scalaVersion := "2.11.8"

val sparkVersion = "2.4.0"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.thoughtworks.cd.de",
      scalaVersion := "2.12.4",
      version := "0.1.0-SNAPSHOT"
    )),

    name := "spark-etl",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided,
      "com.typesafe" % "config" % "1.3.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
    )
  )