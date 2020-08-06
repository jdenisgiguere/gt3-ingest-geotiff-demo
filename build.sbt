lazy val root = (project in file("."))
  .settings(
    name := "gt3-ingest-geotiff-demo",
    scalaVersion := "2.12.10",
    version := "0.1.0-0"
  )
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-s3-spark" % "3.4.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"

