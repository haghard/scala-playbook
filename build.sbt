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

val Akka = "2.4.14"
val Doobie = "0.2.2"
val Origami = "1.0-20150902134048-8d00462"

parallelExecution in Test := false

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "Sonatype" at "https://oss.sonatype.org/content/groups/public/"

resolvers += "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases"

resolvers += "tpolecat" at "http://dl.bintray.com/tpolecat/maven"

resolvers += "rbmhtechnology" at "https://dl.bintray.com/rbmhtechnology/maven"

resolvers += "octalmind"         at "https://dl.bintray.com/guillaumebreton/maven"

resolvers += "fristi at bintray" at "http://dl.bintray.com/fristi/maven"

resolvers += "Twitter Maven"     at "http://maven.twttr.com"

resolvers += "oncue.releases" at "http://dl.bintray.com/oncue/releases/"

resolvers += Resolver.url("ambiata-oss", new URL("https://ambiata-oss.s3.amazonaws.com"))(Resolver.ivyStylePatterns)

libraryDependencies ++= Seq(
    "org.mongodb"         %  "mongo-java-driver"   %  "3.0.2"  withSources(),
    "org.scalaz.stream"   %% "scalaz-stream"       %  "0.8"    withSources(), //"0.7.3a"
    "com.typesafe.akka"   %% "akka-actor"          %    Akka   withSources(),
    "com.typesafe.akka"   %% "akka-testkit"        %    Akka,
    "com.typesafe.akka"   %% "akka-stream-experimental" % "1.0",
    "io.reactivex"        %% "rxscala"             % "0.25.0",
    "org.monifu"          %% "monifu"              % "1.0-M1",
    "log4j"               %  "log4j"               % "1.2.14",
    //"org.scodec"          %% "scodec-stream"       % "0.10.0",
    "com.twitter"         %% "util-core"           % "6.38.0",
    "com.google.guava"    %  "guava"               % "18.0",
    "com.rbmhtechnology"  %% "eventuate-crdt"      % "0.6",
    "com.github.patriknw" %% "akka-data-replication" % "0.11",
    "com.twitter"         %% "algebird-core"         % "0.12.1",
    "oncue.quiver"        %% "core"                % "3.2.1",
    //"com.nrinaudo"        %% "scalaz-stream-csv"   % "0.1.3",
    //"au.com.langdale"     %% "flowlib"             % "0.9"
    //"org.scalaz.netty"    %% "scalaz-netty"        % "0.2.1",
    "co.fs2"              %% "fs2-io"              % "0.9.2",
    "io.dmitryivanov"     %% "scala-crdt"          % "1.0", //local build from source
    //"com.rklaehn"         %% "radixtree_sjs0.6"    % "0.3.0", //local build
    "com.rklaehn"         %% "radixtree"           % "0.4.0",
    "com.propensive"      %% "rapture-base"        % "2.0.0-M2",
    "com.propensive"      %% "rapture-cli"         % "2.0.0-M2",
    "com.propensive"      %% "rapture-json"        % "2.0.0-M2",
    "com.propensive"      %% "rapture-json-spray"  % "2.0.0-M2",
    "com.propensive"      %% "rapture-i18n"        % "2.0.0-M2"
)


libraryDependencies ++= Seq(
  "org.tpolecat"        %% "doobie-core"         % Doobie,
  "org.tpolecat"        %% "doobie-contrib-h2"   % Doobie,
  "org.tpolecat"        %% "doobie-contrib-hikari" % Doobie
)


libraryDependencies ++= Seq(
  "de.bwaldvogel"       %   "mongo-java-server"     %   "1.4.1",
  "org.scalatest"       %%  "scalatest"             %   "2.2.5"     %   "test",
  "org.specs2"          %%  "specs2"                %   "2.4.15"    %   "test",
  "org.scalacheck"      %%  "scalacheck"            %   "1.12.4"    %   "test",
  "org.reactivestreams" %   "reactive-streams-tck"  %   "1.0.0"     %   "test"
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

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.8.0")