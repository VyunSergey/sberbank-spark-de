name := "sberbank-spark-de"

version := "0.1"

scalaVersion := "2.13.4"
val catsVersion = "2.2.0"
val catsEffectVersion = "2.3.0-M1"
val circeVersion = "0.12.3"
val fs2Version = "3.0.0-M1"
val fs2IoVersion = "3.0.0-M1"
val pureconfigVersion = "0.14.0"
val scallopVersion = "3.5.1"
val tofuVersion = "0.8.0"
val derevoVersion = "0.11.5"
val scalacticVersion = "3.2.2"
val scalatestVersion = "3.2.2"

libraryDependencies ++= Seq(
  "org.typelevel"         %% "cats-core"           % catsVersion,
  "org.typelevel"         %% "cats-effect"         % catsEffectVersion,
  "io.circe"              %% "circe-core"          % circeVersion,
  "io.circe"              %% "circe-generic"       % circeVersion,
  "io.circe"              %% "circe-parser"        % circeVersion,
  "co.fs2"                %% "fs2-core"            % fs2Version,
  "co.fs2"                %% "fs2-io"              % fs2IoVersion,
  "com.github.pureconfig" %% "pureconfig"          % pureconfigVersion,
  "org.rogach"            %% "scallop"             % scallopVersion,
  "ru.tinkoff"            %% "tofu"                % tofuVersion,
  "org.manatki"           %% "derevo-cats"         % derevoVersion,
  "org.manatki"           %% "derevo-circe"        % derevoVersion,
  "org.scalactic"         %% "scalactic"           % scalacticVersion,
  "org.scalatest"         %% "scalatest"           % scalatestVersion % Test
)

scalacOptions ++= Seq(
  "-encoding",
  "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:existentials",
  "-language:higherKinds",
  "-Xfatal-warnings",
  "-Ymacro-annotations",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen"
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
