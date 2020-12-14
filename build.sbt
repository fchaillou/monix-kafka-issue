name := "monix-kafka-issue"

scalaVersion := "2.12.11"

libraryDependencies ++= Seq(
  "io.monix" %% "monix" % "3.3.0",
  "io.monix" %% "monix-kafka-10" % "1.0.0-RC6",
  "io.github.embeddedkafka" %% "embedded-kafka" % "2.6.0",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "io.chrisdavenport" %% "log4cats-slf4j" % "1.1.1"
)
