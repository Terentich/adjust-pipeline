ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "adjust-reader"
  )

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
