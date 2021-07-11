val zioVersion = "1.0.3"
val scodecVersion = "1.11.7"
val testContainersVersion = "0.39.1"
val logbackVersion = "1.2.3"

val scala2_12 = "2.12.13"
val scala2_13 = "2.13.5"

val compileAndTest = "compile->compile;test->test"

def compilerOptions(scalaVersion: String): Seq[String] = Seq(
  "-deprecation",
  "-encoding",
  "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Ywarn-dead-code",
  "-Xlint",
  "-language:existentials",
  "-language:postfixOps",
  "-Xlint:_,-byname-implicit"
//  "-Xlog-implicits"
) ++ (CrossVersion.partialVersion(scalaVersion) match {
  case Some((2, scalaMajor)) if scalaMajor == 12 => scala212CompilerOptions
  case Some((2, scalaMajor)) if scalaMajor == 13 => scala213CompilerOptions
})

lazy val scala212CompilerOptions = Seq(
  "-Yno-adapted-args",
  "-Ywarn-unused-import",
  "-Xfuture"
)

lazy val scala213CompilerOptions = Seq(
  "-Wunused:imports"
)

lazy val noPublish = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val buildSettings = Seq(
  sonatypeProfileName := "com.nryanov",
  organization := "com.nryanov.tarantool",
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

lazy val allSettings = buildSettings

lazy val zioTarantool =
  project.in(file(".")).settings(noPublish).aggregate(core, examples)

lazy val core = project
  .in(file("core"))
  .settings(allSettings)
  .settings(
    moduleName := "zio-tarantool-core",
    libraryDependencies ++= Seq(
      "org.scodec" %% "scodec-core" % scodecVersion,
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
