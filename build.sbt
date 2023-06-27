import sbt.Keys._
import sbt._

val projectName = "pe-avro-producer-producer-scala-beam"

val scioVersion = "0.12.8"
val beamVersion = "2.46.0"
val scalaMayorVersion = "2.12"
val dataModellingVersion = "1.1.2"
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
  organization := "com.db",
  // Semantic versioning http://semver.org/
  version := "0.0.1-SNAPSHOT",
  scalaVersion := "2.12.17",
  // scala-steward:on
  resolvers += "confluent" at "https://packages.confluent.io/maven/",
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
      "com.spotify" %% "scio-avro" % scioVersion exclude ("org.apache.avro", "avro"),
      "com.spotify" %% "scio-core" % scioVersion exclude ("org.apache.avro", "avro"),
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "com.spotify" %% "scio-google-cloud-platform" % scioVersion,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.36",
      "org.apache.logging.log4j" % "log4j-core" % "2.19.0",
      "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
      "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.19.0",
//      "com.google.cloud" % "google-cloud-bigquery" % "2.25.0",
      "com.google.cloud" % "google-cloud-storage" % "2.18.0",
      "org.mockito" %% "mockito-scala" % "1.16.34" % Test,
      "org.mockito" %% "mockito-scala-scalatest" % "1.16.15" % Test,
      "org.mockito" %% "mockito-scala-specs2" % "1.16.15" % Test,
      "org.mockito" %% "mockito-scala-cats" % "1.16.15" % Test,
      "org.mockito" %% "mockito-scala-scalaz" % "1.16.15" % Test,
      "jp.ne.opt" %% "bigquery-fake-client" % "0.1.0" % Test,
      "org.apache.kafka" % "kafka-clients" % "2.4.1",
      ("com.db.myproject" % "myproject-data-modelling-ebm" % dataModellingVersion)
        .excludeAll(ExclusionRule(organization = "org.apache.avro")),
      "org.apache.avro" % "avro" % "1.11.1"
    ),
    libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.17.2"
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
