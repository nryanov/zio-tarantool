val zioVersion = "1.0.3"
val scodecVersion = "1.11.7"
val scodecBitsVersion = "1.1.17"
val shapelessVersion = "2.3.3"
val slf4jVersion = "1.7.30"
val scalatestVersion = "3.2.0"
val scalacheckPlusVersion = "3.2.0.0"
val scalamockVersion = "5.0.0"
val scalacheckVersion = "1.14.3"
val testContainersVersion = "0.38.7"
val logbackVersion = "1.2.3"

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
  "-Ywarn-numeric-widen",
  "-Xlint",
  "-language:existentials",
  "-language:postfixOps",
  "-Xlog-implicits"
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

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    "org.scalatestplus" %% "scalacheck-1-14" % scalacheckPlusVersion % Test,
    "org.scalamock" %% "scalamock" % scalamockVersion % Test,
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
    "ch.qos.logback" % "logback-classic" % logbackVersion % Test
  ),
  scalacOptions ++= compilerOptions(scalaVersion.value),
  organization := "",
  scalaVersion := "2.13.3",
  crossScalaVersions := Seq("2.12.10", "2.13.3"),
  Test / parallelExecution := false
)

lazy val root =
  project.in(file(".")).settings(skip in publish := true).aggregate(msgpack, core, auto)

lazy val msgpack = project
  .in(file("msgpack"))
  .settings(commonSettings)
  .settings(
    name := "zio-tarantool-msgpack",
    libraryDependencies ++= Seq(
      "org.scodec" %% "scodec-core" % scodecVersion,
      "org.scodec" %% "scodec-bits" % scodecBitsVersion
    )
  )

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    name := "zio-tarantool",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "com.dimafeng" %% "testcontainers-scala" % testContainersVersion % Test
    )
  )
  .dependsOn(msgpack)

lazy val auto = project
  .in(file("auto"))
  .settings(commonSettings)
  .settings(
    name := "zio-tarantool-auto",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion
    )
  )
  .dependsOn(core)
