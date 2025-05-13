ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "AzEventHubClient",
    idePackagePrefix := Some("com.binvij")
  )

scalacOptions += "-Xasync"


libraryDependencies += "com.azure" % "azure-messaging-eventhubs" % "5.20.2"
libraryDependencies += "com.azure" % "azure-identity" % "1.16.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.5"
libraryDependencies += "org.scala-lang.modules" %% "scala-async" % "1.0.1"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.0"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.15.0"
