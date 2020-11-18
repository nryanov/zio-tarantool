val zioVersion = "1.0.3"
val zioLoggingVersion = "0.5.3"
val scodecVersion = "1.11.7"
val scodecBitsVersion = "1.1.17"
val shapelessVersion = "2.3.3"
val enumeratumVersion = "1.6.1"
val scalatestVersion = "3.2.0"
val scalacheckPlusVersion = "3.2.0.0"
val scalamockVersion = "5.0.0"
val scalacheckVersion = "1.14.3"

lazy val root =
  project
    .in(file("."))
    .settings(
      name := "zio-tarantool",
      scalaVersion := "2.13.3",
      skip in publish := true
    )
    .aggregate(core)

lazy val core = project
  .in(file("core"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion,
      "dev.zio" %% "zio-streams" % zioVersion,
      "dev.zio" %% "zio-logging" % zioLoggingVersion,
      "org.scodec" %% "scodec-core" % scodecVersion,
      "org.scodec" %% "scodec-bits" % scodecBitsVersion,
      "com.chuusai" %% "shapeless" % shapelessVersion,
      "com.beachape" %% "enumeratum" % enumeratumVersion,
      "dev.zio" %% "zio-test" % zioVersion % Test,
      "org.scalatest" %% "scalatest" % scalatestVersion % Test,
      "org.scalatestplus" %% "scalacheck-1-14" % scalacheckPlusVersion % Test,
      "org.scalamock" %% "scalamock" % scalamockVersion % Test,
      "org.scalacheck" %% "scalacheck" % scalacheckVersion % Test
    ),
    scalacOptions ++= Seq(
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
      "-language:existentials",
      "-language:postfixOps"
    )
  )
