import sbt._
import Keys._

object CBuild  extends Build {
  val VERSION = "0.0.1-SNAPSHOT"
  
  lazy val common = project settings(commonSettings : _*)
  
  lazy val analysis = project settings(analysisSettings : _*) dependsOn(common)
  
  lazy val root = (project in file(".")).aggregate(common, analysis)
  
  def baseSettings = Defaults.defaultSettings ++ Seq(
    organization := "com.redhat.et",
    version := VERSION,
    scalaVersion := "2.10.4",
    resolvers ++= Seq(
      "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
      "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
      "Akka Repo" at "http://repo.akka.io/repository",
      "spray" at "http://repo.spray.io/"
    ),
    libraryDependencies ++= Seq(
        "com.github.nscala-time" %% "nscala-time" % "0.6.0",
        "io.spray" %%  "spray-json" % "1.2.5",
        "org.json4s" %%  "json4s-jackson" % "3.2.6"
    ),
    scalacOptions ++= Seq("-feature", "-Yrepl-sync", "-target:jvm-1.7")
  )
  
  def sparkSettings = Seq(
    libraryDependencies ++= Seq(
        "org.apache.spark" %% "spark-core" % sparkVersion,
        "org.apache.spark" %% "spark-sql" % sparkVersion,
        "org.apache.spark" %% "spark-mllib" % sparkVersion
    )
  )
  
  def breezeSettings = Seq(
    libraryDependencies ++= Seq(
      "org.scalanlp" %% "breeze" % "0.6"
    )
  )
  
  def testSettings = Seq(
    fork := true,
    libraryDependencies ++= Seq(
      "org.scalacheck" %% "scalacheck" % "1.11.3" % "test"
    )
  )
  
  def jsonSettings = Seq(
    libraryDependencies ++= Seq(
      "org.json4s" %% "json4s-jackson" % "3.2.11",
      "org.json4s" %% "json4s-ext" % "3.2.11",
      "joda-time" % "joda-time" % "2.5"
    ) 
  )
  
  def dispatchSettings = Seq(
    libraryDependencies += 
      "net.databinder.dispatch" %% "dispatch-core" % "0.11.1"
  )
  
  def commonSettings = baseSettings ++ sparkSettings ++ jsonSettings
  
  def analysisSettings = baseSettings ++ sparkSettings ++ breezeSettings ++ testSettings ++ Seq(
    initialCommands in console :=
      """
        |import org.apache.spark.SparkConf
        |import org.apache.spark.SparkContext
        |import org.apache.spark.rdd.RDD
        |val app = new com.redhat.et.consigliere.common.ConsoleApp()
        |val spark = app.context
        |import app.sqlContext._
        |
      """.stripMargin,
    cleanupCommands in console := "spark.stop"
  )
  
  val sparkVersion = "1.1.0"
  val scalatraVersion = "2.2.2"
  val scalateVersion = "1.7.0-SNAPSHOT"
}
