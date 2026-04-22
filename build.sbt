ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

scalacOptions ++= Seq("-language:implicitConversions", "-deprecation")
lazy val root = (project in file("."))
  .settings(
    name := "ScalaCode"
  )

libraryDependencies += "org.scalameta" %% "munit" % "0.7.29" % Test
libraryDependencies += "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"
libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.0",
    "org.apache.spark" %% "spark-sql" % "3.4.0",
)
testFrameworks += new TestFramework("munit.Framework")
libraryDependencies += "org.xerial" % "sqlite-jdbc" % "3.43.0.0"

