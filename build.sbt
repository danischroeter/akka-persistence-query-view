val homepageUrl = url("https://github.com/firstbirdtech/akka-persistence-query-view")
val scmUrl ="https://github.com/firstbirdtech/akka-persistence-query-view.git"

lazy val root = project
  .in(file("."))
  .settings(
    organization := "com.firstbird",
    organizationName := "Firstbird GmbH",
    organizationHomepage := Some(url("https://www.firstbird.com")),
    name := "akka-persistence-query-view",
    description := "An Akka PersistentView replacement",
    homepage := Some(homepageUrl),
    scmInfo := Some(
      ScmInfo(homepageUrl, scmUrl)
    ),
    startYear := Some(2016),
    licenses := Seq(("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    developers += Developer(
      "contributors",
      "Contributors",
      "hello@firstbird.com",
      homepageUrl
    ),
    scalacOptions ++= Seq(
      "-deprecation",                  // Emit warning and location for usages of deprecated APIs.
      "-explaintypes",                 // Explain type errors in more detail.
      "-feature",                      // Emit warning and location for usages of features that should be imported explicitly.
      "-unchecked",                    // Enable additional warnings where generated code depends on assumptions.
      "-Xcheckinit",                   // Wrap field accessors to throw an exception on uninitialized access.
      "-Xfatal-warnings",              // Fail the compilation if there are any warnings.
      "-Xlint:adapted-args",           // Warn if an argument list is modified to match the receiver.
      "-Xlint:constant",               // Evaluation of a constant arithmetic expression results in an error.
      "-Xlint:delayedinit-select",     // Selecting member of DelayedInit.
      "-Xlint:doc-detached",           // A Scaladoc comment appears to be detached from its element.
      "-Xlint:inaccessible",           // Warn about inaccessible types in method signatures.
      "-Xlint:infer-any",              // Warn when a type argument is inferred to be `Any`.
      "-Xlint:missing-interpolator",   // A string literal appears to be missing an interpolator id.
      "-Xlint:nullary-override",       // Warn when non-nullary `def f()' overrides nullary `def f'.
      "-Xlint:nullary-unit",           // Warn when nullary methods return Unit.
      "-Xlint:option-implicit",        // Option.apply used implicit view.
      "-Xlint:package-object-classes", // Class or object defined in package object.
      "-Xlint:poly-implicit-overload", // Parameterized overloaded implicit methods are not visible as view bounds.
      "-Xlint:private-shadow",         // A private field (or class parameter) shadows a superclass field.
      "-Xlint:stars-align",            // Pattern sequence wildcard must align with sequence component.
      "-Xlint:type-parameter-shadow",  // A local type parameter shadows a type already in scope.
      "-Ywarn-dead-code",              // Warn when dead code is identified.
      "-Ywarn-extra-implicit",         // Warn when more than one implicit parameter section is defined.
      "-Ywarn-numeric-widen",          // Warn when numerics are widened.
      "-Ywarn-unused:implicits",       // Warn if an implicit parameter is unused.
      "-Ywarn-unused:imports",         // Warn if an import selector is not referenced.
      "-Ywarn-unused:locals",          // Warn if a local definition is unused.
      "-Ywarn-unused:params",          // Warn if a value parameter is unused.
      "-Ywarn-unused:patvars",         // Warn if a variable bound in a pattern is unused.
      "-Ywarn-unused:privates",        // Warn if a private member is unused.
      "-Ywarn-value-discard"           // Warn when non-Unit expression results are unused.
    ),
    javacOptions ++= Seq(
      "-Xlint:unchecked",
      "-Xlint:deprecation"
    ),
    libraryDependencies ++= Dependencies.core,
    // Needed because the API doc for Akka-Persistence can't be found and the warning would abort publishing
    Compile / doc / scalacOptions := Seq()
  )

lazy val docs = project
  .in(file("mdocs"))
  .settings(
    mdocOut := new File("."),
    mdocVariables := Map(
      "VERSION" -> version.value
    )
  )
  .dependsOn(root)
  .enablePlugins(MdocPlugin)
