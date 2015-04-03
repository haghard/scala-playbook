import sbt.Keys._
import sbt._

import com.typesafe.sbt.SbtScalariform.ScalariformKeys
import scalariform.formatter.preferences._


scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(RewriteArrowSymbols, true)
  .setPreference(AlignParameters, true)
  .setPreference(AlignSingleLineCaseStatements, true)


organization := "github.com/haghard"

name := "scala-playbook"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.6"

val Akka = "2.3.9"
val Doobie = "0.2.1"

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "haghard-bintray"    at "http://dl.bintray.com/haghard/releases"

resolvers += "tpolecat" at "http://dl.bintray.com/tpolecat/maven"

libraryDependencies ++= Seq(
    "org.scalaz"          %% "scalaz-core"         %  "7.1.0"   withSources(),
    "org.scalaz"          %% "scalaz-concurrent"   %  "7.1.0"   withSources(),
    "org.mongodb"         %  "mongo-java-driver"   %  "2.13.0"  withSources(),
    "org.scalaz.stream"   %% "scalaz-stream"       %  "0.7a"    withSources(),
    "com.typesafe.akka"   %% "akka-actor"          %  Akka      withSources(),
    "com.typesafe.akka"   %% "akka-testkit"        %  Akka,
    "com.typesafe.akka"   %% "akka-stream-experimental" % "1.0-M5",
    //"org.mongo.scalaz"    %% "mongo-query-streams" %  "0.5.1" exclude ("org.specs2", "*"),
    //"net.fehmicansaglam"  %% "tepkin"              %  "0.2-SNAPSHOT",
    "org.tpolecat"   %% "doobie-core"               % Doobie,
    "org.tpolecat"   %% "doobie-contrib-postgresql" % Doobie,
    "org.tpolecat"   %% "doobie-contrib-specs2"     % Doobie,
    "org.tpolecat"   %% "doobie-contrib-h2"         % Doobie,
    "org.scodec"     %% "scodec-bits"               % "1.0.6",
    "log4j"               %  "log4j"               %  "1.2.14")


libraryDependencies ++= Seq(
  "de.bwaldvogel"       %   "mongo-java-server"   %   "1.2.0",
  "org.scalatest"       %%  "scalatest"           %   "2.2.0"   %   "test",
  "org.specs2"          %%  "specs2"              %   "2.4.15"  %   "test"
)


scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-target:jvm-1.7",
  "-deprecation",
  "-unchecked",
  "-Ywarn-dead-code",
  "-feature",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-language:existentials")

javacOptions ++= Seq(
  "-source", "1.7",
  "-target", "1.7",
  "-Xlint:unchecked",
  "-Xlint:deprecation")

javaOptions ++= Seq("-Xms226m", "-Xmx756m")