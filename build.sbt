name := "sort16"

version := "0.1"

run / javaOptions += "-Xmx16G"

javaOptions in(Test, run) += "-Xmx16G"

parallelExecution in Test := false

fork := true

lazy val commonSettings = Seq(
  organization := "ajtkulov.github.com",
  scalaVersion := "2.13.6",
  sources in(Compile, doc) := Seq.empty,
)

parallelExecution in Test := false


libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value,
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "dev.zio" %% "zio" % "2.0.18",
  "dev.zio" %% "zio-test" % "2.0.18" % Test,
  "dev.zio" %% "zio-test-sbt" % "2.0.18" % Test
)

libraryDependencies += "org.rogach" %% "scallop" % "5.1.0"

lazy val app = (project in file("."))
  .settings(
    assembly / assemblyJarName := "sort16.jar"
  )
