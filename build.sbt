val zioVersion = "1.0.3"
val zioLoggingVersion = "0.5.3"
val scodecVersion = "1.11.7"
val scodecBitsVersion = "1.1.17"
val shapelessVersion = "2.3.3"
val enumeratumVersion = "1.6.1"
val slf4jVersion = "1.7.30"
val scalatestVersion = "3.2.0"
val scalacheckPlusVersion = "3.2.0.0"
val scalamockVersion = "5.0.0"
val scalacheckVersion = "1.14.3"
val testContainersVersion = "0.38.7"
val logbackVersion = "1.2.3"

val compilerOptions = Seq(
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
  "-Xlog-implicits",
  "-Xlint",
  "-language:postfixOps",
  //      "-Ymacro-annotations", // for scala 2.13
  "-Xlog-implicits"
)

lazy val commonSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scalatest" %% "scalatest" % scalatestVersion % Test,
    "org.scalatestplus" %% "scalacheck-1-14" % scalacheckPlusVersion % Test,
    "org.scalamock" %% "scalamock" % scalamockVersion % Test,
    "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test,
    "ch.qos.logback" % "logback-classic" % logbackVersion % Test
  ),
  scalacOptions ++= compilerOptions,
  Test / parallelExecution := false
)

lazy val root =
  project
    .in(file("."))
    .settings(
      scalaVersion := "2.13.3", // todo: cross-build
      skip in publish := true
    )
    .aggregate(msgpack, core, auto)

lazy val msgpack = project
  .in(file("msgpack"))
  .settings(commonSettings)
  .settings(
    name := "zio-tarantool-msgpack",
    libraryDependencies ++= Seq(
      "org.scodec" %% "scodec-core" % scodecVersion,
      "org.scodec" %% "scodec-bits" % scodecBitsVersion,
      "com.beachape" %% "enumeratum" % enumeratumVersion
    ),
    scalacOptions ++= compilerOptions
  )

lazy val core = project
  .in(file("core"))
  .settings(commonSettings)
  .settings(
    addCompilerPlugin(("org.scalamacros" % "paradise" % "2.1.1").cross(CrossVersion.full)),
    name := "zio-tarantool",
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-logging" % zioLoggingVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "com.dimafeng" %% "testcontainers-scala" % testContainersVersion % Test
    ),
    scalacOptions ++= compilerOptions
  )
  .dependsOn(msgpack)

lazy val auto = project
  .in(file("auto"))
  .settings(commonSettings)
  .settings(
    name := "zio-tarantool-auto",
    libraryDependencies ++= Seq(
      "com.chuusai" %% "shapeless" % shapelessVersion
    ),
    scalacOptions ++= compilerOptions
  )
  .dependsOn(msgpack)
