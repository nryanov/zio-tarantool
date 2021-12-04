val zioVersion = "1.0.12"
val shapelessVersion = "2.3.7"
val msgpackVersion = "0.9.0"
val testContainersVersion = "0.39.12"
val logbackVersion = "1.2.7"

val scala2_12 = "2.12.13"
val scala2_13 = "2.13.6"

val compileAndTest = "compile->compile;test->test"

def compilerOptions(scalaVersion: String): Seq[String] = Seq(
  "-deprecation",
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:existentials",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Xlint",
  "-Xlint:constant",
  "-Xlint:inaccessible",
  "-Xlint:nullary-unit",
  "-Xlint:type-parameter-shadow"
//  "-Xlog-implicits"
) ++ (CrossVersion.partialVersion(scalaVersion) match {
  case Some((2, scalaMajor)) if scalaMajor == 12 => scala212CompilerOptions
  case Some((2, scalaMajor)) if scalaMajor == 13 => scala213CompilerOptions
})

lazy val scala212CompilerOptions = Seq(
  "-Yno-adapted-args",
  "-Xfuture",
  "-Ypartial-unification",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:patvars",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard",
  "-Xlint:unsound-match"
)

lazy val scala213CompilerOptions = Seq(
  "-Xlint:_,-byname-implicit",
  "-Ymacro-annotations",
  "-Wdead-code",
  "-Wnumeric-widen",
  "-Wunused:explicits",
  "-Wunused:implicits",
  "-Wunused:imports",
  "-Wunused:locals",
  "-Wunused:patvars",
  "-Wunused:privates",
  "-Wvalue-discard",
  "-Xlint:deprecation",
  "-Xlint:eta-sam",
  "-Xlint:eta-zero",
  "-Xlint:implicit-not-found",
  "-Xlint:infer-any",
  "-Xlint:nonlocal-return",
  "-Xlint:unused",
  "-Xlint:valpattern"
)

lazy val noPublish = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val buildSettings = Seq(
  sonatypeProfileName := "com.nryanov",
  organization := "com.nryanov.zio-tarantool",
  homepage := Some(url("https://github.com/nryanov/zio-tarantool")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer(
      "nryanov",
      "Nikita Ryanov",
      "ryanov.nikita@gmail.com",
      url("https://nryanov.com")
    )
  ),
  scalaVersion := scala2_13,
  crossScalaVersions := Seq(scala2_12, scala2_13),
  scalacOptions ++= compilerOptions(scalaVersion.value),
  Test / parallelExecution := false
)

lazy val macroSettings: Seq[Setting[_]] = Seq(
  libraryDependencies ++= Seq(
    scalaOrganization.value % "scala-compiler" % scalaVersion.value % Provided,
    scalaOrganization.value % "scala-reflect" % scalaVersion.value % Provided
  )
)

lazy val allSettings = buildSettings

lazy val zioTarantool =
  project
    .in(file("."))
    .settings(moduleName := "zio-tarantool")
    .settings(allSettings)
    .settings(noPublish)
    .aggregate(core, examples)

lazy val core = project
  .in(file("core"))
  .settings(allSettings)
  .settings(macroSettings)
  .settings(
    moduleName := "zio-tarantool-core",
    libraryDependencies ++= Seq(
      "org.msgpack" % "msgpack-core" % msgpackVersion,
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
      "com.dimafeng" %% "testcontainers-scala" % testContainersVersion % Test,
      "ch.qos.logback" % "logback-classic" % logbackVersion % Test
    ),
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val examples =
  project.in(file("examples")).settings(allSettings).settings(noPublish).dependsOn(core)

lazy val benchmarks =
  project.in(file("benchmarks")).enablePlugins(JmhPlugin).settings(allSettings).settings(noPublish).dependsOn(core)
