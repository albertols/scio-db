import sbt.Keys._
import sbt._

val projectName = "scio-db"
// https://github.com/spotify/scio/releases/tag/v0.14.0
val scioVersion = "0.14.2"
val beamVersion = "2.54.0"
val scalaMayorVersion = "2.12"
val dataModellingVersion = "2.0.2-SNAPSHOT"
val scalaUtilsVersion = "2.0.0-SNAPSHOT"
val scalaMacrosVersion = "2.1.1"

logLevel := Level.Info
evictionErrorLevel := Level.Warn
coverageEnabled := false
coverageHighlighting := true
coverageFailOnMinimum := true
coverageMinimumStmtTotal := 80
//TODO: add https://github.com/pureconfig/pureconfig

ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

//lazy val enablingCoverageSettings = Seq(coverageEnabled in(Test, compile) := true, coverageEnabled in(Compile, compile) := false)


lazy val commonSettings = Def.settings(
  organization := "com.db.myproject",
  // Semantic versioning http://semver.org/
  version := "0.0.2-SNAPSHOT",
  scalaVersion := "2.12.17",
  // scala-steward:on
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
  resolvers += Resolver.mavenLocal
)

lazy val macroSettings = Def.settings(
  // see MacroSettings.scala
  scalacOptions += "-Xmacro-settings:cache-implicit-schemas=true"
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(macroSettings)
  .settings(
    name := projectName,
    description := projectName,
    publish / skip := true,
    run / classLoaderLayeringStrategy.withRank(KeyRanks.Invisible) := ClassLoaderLayeringStrategy.Flat,
    addCompilerPlugin("org.scalamacros" % "paradise" % scalaMacrosVersion cross CrossVersion.full),
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.spotify" %% "scio-parquet" % scioVersion,
      "com.spotify" %% "scio-avro" % scioVersion,
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      //      "org.slf4j" % "log4j-over-slf4j" % "2.0.9", // due to scio 0.13.3 bug: https://github.com/spotify/ratatool/pull/671/files
      "org.apache.kafka" % "kafka-clients" % "3.5.1",
      "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.10",
      "com.github.pureconfig" %% "pureconfig" % "0.17.2",
      "com.fasterxml.jackson.dataformat"% "jackson-dataformat-avro" % "2.15.2",

      /* GCP dependencies
      check versions from libraries-bom:
      https://storage.googleapis.com/cloud-opensource-java-dashboard/com.google.cloud/libraries-bom/26.23.0/index.html */
      "com.google.cloud" % "google-cloud-bigquery" % "2.32.0",
      "com.google.cloud" % "google-cloud-secretmanager" % "2.24.0",
      "com.google.cloud" % "google-cloud-storage" % "2.27.0",
      "com.google.cloud" % "google-cloud-logging-logback" % "0.130.22-alpha",

      /* AKKA */
      "com.typesafe.akka" %% "akka-stream" % "2.8.0",
      "com.typesafe.akka" %% "akka-http" % "10.5.0",
      "com.typesafe.akka" %% "akka-actor-typed" % "2.8.0",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.5.0",

      /* ZIO */
      "dev.zio" %% "zio" % "2.0.19",
      "dev.zio" %% "zio-http" % "3.0.0-RC4",
      "dev.zio" %% "zio-json" % "0.6.2",
      "org.apache.logging.log4j" % "log4j-api" % "2.20.0"
    ),
    // com.fasterxml.jackson.databind.JsonMappingException:Scala module 2.14.1 requires Jackson Databind version >= 2.14.0 and < 2.15.0 - Found jackson-databind version 2.15.2
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.14.1"
  )
  .enablePlugins(PackPlugin)


lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for sciotest",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true
  )
  .dependsOn(root)