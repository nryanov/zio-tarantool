val zioVersion = "2.1.26"
val shapelessVersion = "2.3.12"
val shapeless3Version = "3.6.0"
val msgpackVersion = "0.9.8"
val testContainersVersion = "0.44.1"
val logbackVersion = "1.5.18"

val scala2_12 = "2.12.20"
val scala2_13 = "2.13.18"
val scala3 = "3.3.6"

val compileAndTest = "compile->compile;test->test"

def compilerOptions(scalaVersion: String): Seq[String] =
  CrossVersion.partialVersion(scalaVersion) match {
    case Some((3, _)) =>
      Seq(
        "-deprecation",
        "-unchecked",
        "-encoding",
        "UTF-8",
        "-feature",
        "-language:implicitConversions",
        "-language:higherKinds",
        "-Wunused:all",
        "-Wvalue-discard"
      )
    case Some((2, 12)) =>
      sharedScala2CompilerOptions ++ scala212CompilerOptions
    case Some((2, 13)) =>
      sharedScala2CompilerOptions ++ scala213CompilerOptions
    case _ =>
      Seq.empty
  }

lazy val sharedScala2CompilerOptions = Seq(
  "-deprecation",
  "-unchecked",
  "-encoding",
  "UTF-8",
  "-explaintypes",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-language:postfixOps",
  "-Ywarn-dead-code",
  "-Xlint",
  "-Xlint:constant",
  "-Xlint:inaccessible",
  "-Xlint:nullary-unit",
  "-Xlint:type-parameter-shadow"
)

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
  scalaVersion := scala3,
  crossScalaVersions := Seq(scala2_12, scala2_13, scala3),
  scalacOptions ++= compilerOptions(scalaVersion.value),
  Test / parallelExecution := false
)

lazy val macroSettings: Seq[Setting[_]] = Seq(
  libraryDependencies ++= {
    CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, _)) =>
        Seq(scalaOrganization.value % "scala-reflect" % scalaVersion.value % Provided)
      case _ =>
        Seq.empty
    }
  }
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
    libraryDependencies ++= {
      val shapelessDeps = CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((3, _)) =>
          Seq("org.typelevel" %% "shapeless3-deriving" % shapeless3Version)
        case _ =>
          Seq("com.chuusai" %% "shapeless" % shapelessVersion)
      }

      shapelessDeps ++ Seq(
        "org.msgpack" % "msgpack-core" % msgpackVersion,
        "dev.zio" %% "zio-streams" % zioVersion,
        "dev.zio" %% "zio-test" % zioVersion % Test,
        "dev.zio" %% "zio-test-sbt" % zioVersion % Test,
        "com.dimafeng" %% "testcontainers-scala" % testContainersVersion % Test,
        "ch.qos.logback" % "logback-classic" % logbackVersion % Test
      )
    },
    testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
  )

lazy val examples =
  project.in(file("examples")).settings(allSettings).settings(noPublish).dependsOn(core)

lazy val benchmarks =
  project.in(file("benchmarks")).enablePlugins(JmhPlugin).settings(allSettings).settings(noPublish).dependsOn(core)
