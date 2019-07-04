name := "structured-streaming-sources"

lazy val commonSettings = Seq(
  version := "0.0.1",
  organization := "org.structured-streaming-sources",
  scalaVersion := "2.11.8"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val streaming_sources = project.in(file("streaming-sources")).
                        settings(commonSettings,
                          libraryDependencies ++= commonProvidedDependencies)

lazy val examples = project.in(file("examples")).
                          dependsOn(streaming_sources).
                          settings(commonSettings,
                            libraryDependencies ++= commonDependencies
                          )

val sparkVersion = "2.3.1"

lazy val commonProvidedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6"
)

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-catalyst" % sparkVersion,
  "org.twitter4j" % "twitter4j-stream" % "4.0.6"
)
