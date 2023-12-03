val scala3Version = "3.3.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "ingestion-layer",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
  )
libraryDependencies += "com.lihaoyi" %% "requests" % "0.8.0"
libraryDependencies += "com.lihaoyi" %% "upickle" % "3.1.3"