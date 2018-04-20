/*
scalafmt: {
  style = defaultWithAlign
  maxColumn = 150
  align.tokens = [
    { code = "=>", owner = "Case" }
    { code = "?", owner = "Case" }
    { code = "extends", owner = "Defn.(Class|Trait|Object)" }
    { code = "//", owner = ".*" }
    { code = "{", owner = "Template" }
    { code = "}", owner = "Template" }
    { code = ":=", owner = "Term.ApplyInfix" }
    { code = "++=", owner = "Term.ApplyInfix" }
    { code = "+=", owner = "Term.ApplyInfix" }
    { code = "%", owner = "Term.ApplyInfix" }
    { code = "%%", owner = "Term.ApplyInfix" }
    { code = "%%%", owner = "Term.ApplyInfix" }
    { code = "->", owner = "Term.ApplyInfix" }
    { code = "?", owner = "Term.ApplyInfix" }
    { code = "<-", owner = "Enumerator.Generator" }
    { code = "?", owner = "Enumerator.Generator" }
    { code = "=", owner = "(Enumerator.Val|Defn.(Va(l|r)|Def|Type))" }
  ]
}
 */
val commonsVersion  = "0.10.9"
val serviceVersion  = "0.10.8"
val sourcingVersion = "0.10.5"

val akkaVersion                 = "2.5.12"
val akkaHttpVersion             = "10.1.1"
val akkaHttpCorsVersion         = "0.3.0"
val akkaHttpCirceVersion        = "1.20.0"
val akkaPersistenceInMemVersion = "2.5.1.1"
val akkaStreamKafkaVersion      = "0.19"

val catsVersion  = "1.0.1"
val circeVersion = "0.9.3"

val logbackVersion = "1.2.3"
val journalVersion = "3.0.19"

val metricsCoreVersion       = "3.2.6"
val mockitoVersion           = "2.18.0"
val jenaVersion              = "3.6.0"
val blazegraphVersion        = "2.1.4"
val scalaTestVersion         = "3.0.5"
val scalaTestEmbeddedVersion = "1.1.0"

val pureconfigVersion = "0.9.1"
val refinedVersion    = "0.8.7"

lazy val akkaDistributed      = "com.typesafe.akka"   %% "akka-distributed-data"     % akkaVersion
lazy val akkaHttpCors         = "ch.megard"           %% "akka-http-cors"            % akkaHttpCorsVersion
lazy val akkaHttp             = "com.typesafe.akka"   %% "akka-http"                 % akkaHttpVersion
lazy val akkaHttpTestKit      = "com.typesafe.akka"   %% "akka-http-testkit"         % akkaHttpVersion
lazy val akkaPersistence      = "com.typesafe.akka"   %% "akka-persistence"          % akkaVersion
lazy val akkaPersistenceInMem = "com.github.dnvriend" %% "akka-persistence-inmemory" % akkaPersistenceInMemVersion
lazy val akkaSlf4j            = "com.typesafe.akka"   %% "akka-slf4j"                % akkaVersion
lazy val akkaStreamKafka      = "com.typesafe.akka"   %% "akka-stream-kafka"         % akkaStreamKafkaVersion
lazy val circeRefined         = "io.circe"            %% "circe-refined"             % circeVersion

lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion

lazy val sourcingAkka = "ch.epfl.bluebrain.nexus" %% "sourcing-akka" % sourcingVersion
lazy val sourcingCore = "ch.epfl.bluebrain.nexus" %% "sourcing-core" % sourcingVersion
lazy val sourcingMem  = "ch.epfl.bluebrain.nexus" %% "sourcing-mem"  % sourcingVersion

lazy val serviceHttp     = "ch.epfl.bluebrain.nexus" %% "service-http"     % serviceVersion
lazy val serviceIndexing = "ch.epfl.bluebrain.nexus" %% "service-indexing" % serviceVersion
lazy val serviceKamon    = "ch.epfl.bluebrain.nexus" %% "service-kamon"    % serviceVersion

lazy val commonsIam        = "ch.epfl.bluebrain.nexus" %% "iam"                  % commonsVersion
lazy val commonsQueryTypes = "ch.epfl.bluebrain.nexus" %% "commons-query-types"  % commonsVersion
lazy val commonsSchemas    = "ch.epfl.bluebrain.nexus" %% "commons-schemas"      % commonsVersion
lazy val commonsTest       = "ch.epfl.bluebrain.nexus" %% "commons-test"         % commonsVersion
lazy val commonsTypes      = "ch.epfl.bluebrain.nexus" %% "commons-types"        % commonsVersion
lazy val shaclValidator    = "ch.epfl.bluebrain.nexus" %% "shacl-validator"      % commonsVersion
lazy val sparqlClient      = "ch.epfl.bluebrain.nexus" %% "sparql-client"        % commonsVersion
lazy val elasticClient     = "ch.epfl.bluebrain.nexus" %% "elastic-client"       % commonsVersion
lazy val elasticEmbed      = "ch.epfl.bluebrain.nexus" %% "elastic-server-embed" % commonsVersion
lazy val mockitoCore       = "org.mockito"             % "mockito-core"          % mockitoVersion

lazy val pureconfig        = "com.github.pureconfig" %% "pureconfig"         % pureconfigVersion
lazy val refined           = "eu.timepit"            %% "refined"            % refinedVersion
lazy val refinedPureConfig = "eu.timepit"            %% "refined-pureconfig" % refinedVersion

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(
    name                         := "kg-docs",
    moduleName                   := "kg-docs",
    paradoxTheme                 := Some(builtinParadoxTheme("generic")),
    paradoxProperties in Compile ++= Map("extref.service.base_url" -> "../"),
    target in (Compile, paradox) := (resourceManaged in Compile).value / "docs",
    resourceGenerators in Compile += {
      (paradox in Compile).map { parent =>
        (parent ** "*").get
      }.taskValue
    }
  )

lazy val core = project
  .in(file("modules/core"))
  .enablePlugins(BuildInfoPlugin)
  .settings(buildInfoSettings)
  .settings(
    name       := "kg-core",
    moduleName := "kg-core",
    resolvers  += Resolver.bintrayRepo("bogdanromanx", "maven"),
    libraryDependencies ++= Seq(
      akkaPersistence,
      akkaSlf4j,
      circeRefined,
      commonsIam,
      commonsQueryTypes,
      commonsTest,
      pureconfig,
      refined,
      refinedPureConfig,
      sourcingCore,
      akkaDistributed      % Test,
      akkaHttpTestKit      % Test,
      akkaPersistenceInMem % Test,
      mockitoCore          % Test,
      sourcingMem          % Test,
      scalaTest            % Test
    )
  )

lazy val service = project
  .in(file("modules/service"))
  .dependsOn(core, docs)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(buildInfoSettings)
  .settings(
    name       := "kg-service",
    moduleName := "kg-service",
    resolvers  += Resolver.bintrayRepo("bogdanromanx", "maven"),
    libraryDependencies ++= Seq(
      akkaDistributed,
      akkaHttpCors,
      akkaSlf4j,
      commonsIam,
      commonsQueryTypes,
      commonsTest,
      pureconfig,
      refinedPureConfig,
      serviceHttp,
      serviceKamon,
      sourcingAkka,
      akkaHttpTestKit % Test,
      sourcingMem     % Test,
      scalaTest       % Test
    )
  )

lazy val root = project
  .in(file("."))
  .settings(noPublish)
  .settings(
    name       := "kg",
    moduleName := "kg"
  )
  .aggregate(docs, core, service)

lazy val noPublish = Seq(publishLocal := {}, publish := {}, publishArtifact := false)

inThisBuild(
  List(
    homepage := Some(url("https://github.com/BlueBrain/nexus-kg")),
    licenses := Seq("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
    scmInfo  := Some(ScmInfo(url("https://github.com/BlueBrain/nexus-kg"), "scm:git:git@github.com:BlueBrain/nexus-kg.git")),
    developers := List(
      Developer("bogdanromanx", "Bogdan Roman", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("hygt", "Henry Genet", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("umbreak", "Didac Montero Mendez", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/")),
      Developer("wwajerowicz", "Wojtek Wajerowicz", "noreply@epfl.ch", url("https://bluebrain.epfl.ch/"))
    ),
    // These are the sbt-release-early settings to configure
    releaseEarlyWith              := BintrayPublisher,
    releaseEarlyNoGpg             := true,
    releaseEarlyEnableSyncToMaven := false,
    // Jena workaround
    parallelExecution in Test := false
  )
)

lazy val buildInfoSettings = Seq(buildInfoKeys := Seq[BuildInfoKey](version), buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.service.config")

addCommandAlias("review", ";clean;scalafmtSbt;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate;doc")
