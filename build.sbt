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

scalaVersion := "2.11.8"

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Sonatype" at "https://oss.sonatype.org/content/groups/public/"

resolvers += "octalmind"             at "https://dl.bintray.com/guillaumebreton/maven"

resolvers += "fristi at bintray" at "http://dl.bintray.com/fristi/maven"

resolvers += "oncue.releases" at "http://dl.bintray.com/oncue/releases/"

libraryDependencies ++= Seq("com.lihaoyi" %% "fastparse" % "0.3.7")

libraryDependencies ++= Seq("org.scalatest" %%  "scalatest"  %   "2.2.5"     %   "test")

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