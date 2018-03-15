name := "structured-streaming-sources"

lazy val commonSettings = Seq(
  version := "0.0.1",
  organization := "org.structured-streaming-sources",
  scalaVersion := "2.11.8"
)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

lazy val receiver = project.in(file("streaming-sources")).
                        settings(commonSettings,
                          libraryDependencies ++= commonProvidedDependencies)

lazy val examples = project.in(file("examples")).
                          dependsOn(receiver).
                          settings(commonSettings,
                            libraryDependencies ++= commonDependencies
                          )


lazy val commonProvidedDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-catalyst" % "2.3.0" % "provided",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6"
)

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-streaming" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-catalyst" % "2.3.0",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6"
)
