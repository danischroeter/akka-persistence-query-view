import Dependencies._
import com.typesafe.sbt.GitPlugin.autoImport._
import com.typesafe.sbt.{GitBranchPrompt, GitVersioning}
import com.typesafe.sbt.git._
import de.heikoseeberger.sbtheader.license.Apache2_0

lazy val `akka-persistence-query-view` = (project in file("."))
  .enablePlugins(GitVersioning, GitBranchPrompt, BuildInfoPlugin)
  .settings(
    organization := "com.ovoenergy",
    organizationHomepage := Some(url("https://www.ovoenergy.com/")),
    description := "An Akka PersistentView replacement",
    name := "akka-persistence-query-view",
    homepage := Some(url("https://github.com/ovotech/akka-persistence-query-view")),
    startYear := Some(2016),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    developers := List(
      Developer(
        "filippo.deluca@ovoenergy.com",
        "Filippo De Luca",
        "filippo.deluca@ovoenergy.com",
        url("https://filippodeluca.com")
      )
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/ovotech/akka-persistence-query-view"),
        "git@github.com:ovotech/akka-persistence-query-view.git"
      )
    ),
    git.remoteRepo := "origin",
    git.runner := ConsoleGitRunner,
    git.baseVersion := "0.1.0",
    git.useGitDescribe := true,
    scalaVersion := "2.12.1",
    crossScalaVersions := Seq(scalaVersion.value, "2.11.8"),
    resolvers ++= Seq(Resolver.mavenLocal, Resolver.typesafeRepo("releases")),
    // THe scaladoc is causing issue when generating doc around the snapshot format
    publishArtifact in (Compile, packageDoc) := false,
    libraryDependencies ++= Seq(
      typesafe.config,
      slf4j.api,
      // -- Akka
      akka.actor,
      akka.stream,
      akka.persistence,
      akka.persistenceQuery,
      akka.protobuf,
      // -- Testing --
      scalaTest % Test,
      scalaCheck % Test,
      scalaMock.scalaTestSupport % Test,
      akka.streamTestKit % Test,
      akka.slf4j % Test,
      logback.classic % Test,
      LevelDb.levelDb % Test
    ),
    headers := Map(
      "java" -> Apache2_0("2016", "OVO Energy"),
      "proto" -> Apache2_0("2016", "OVO Energy", "//"),
      "scala" -> Apache2_0("2016", "OVO Energy"),
      "conf" -> Apache2_0("2016", "OVO Energy", "#")
    )
//    ,
//    tutSettings,
//    tutTargetDirectory := baseDirectory.value,
//    bintrayOrganization := Some("ovotech"),
//    bintrayRepository := "maven",
//    bintrayPackageLabels := Seq("akka", "akka-persistence", "event-sourcing", "cqrs")
  )
