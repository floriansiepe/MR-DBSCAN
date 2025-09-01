name := "MR-DBSCAN"

scalaVersion := "2.13.16"

lazy val stark = (project in file("."))

libraryDependencies ++= Seq(
  //"com.vividsolutions" % "jts" % "1.13" withSources() withJavadoc(),
  "org.locationtech.jts" % "jts-core" % "1.20.0",
  "org.apache.spark" %% "spark-core" % "4.0.0" % "provided" withSources(),
  "org.apache.spark" %% "spark-mllib" % "4.0.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "4.0.0" % "provided",
  //"fm.void.jetm" % "jetm" % "1.2.3",
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "org.scalatestplus" %% "scalacheck-1-18" % "3.2.19.0" % "test", //"com.assembla.scala-incubator" %% "graph-core" % "1.11.0",
  "org.scala-graph" %% "graph-core" % "2.0.3",
  "com.github.scopt" %% "scopt" % "4.1.0"
)

(assembly / test) := {}

(Test / logBuffered) := false

(Test / parallelExecution) := false

(assembly / assemblyJarName) := "mr-dbscan.jar"

(assembly / assemblyOption) := (assembly / assemblyOption).value.withIncludeScala(false)
