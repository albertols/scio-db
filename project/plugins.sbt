ThisBuild / libraryDependencySchemes += "org.scala-lang.modules" %% "scala-xml" % VersionScheme.Always

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.1")
addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.17")
addSbtPlugin("org.scoverage" % "sbt-scoverage" % "2.0.7")
//addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.7.3")
addDependencyTreePlugin // sbt dependencyBrowseTree