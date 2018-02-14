val akkaVersion                     = "2.5.9"
val akkaHttpVersion                 = "10.0.11"
val akkaHttpCorsVersion             = "0.2.2"
val akkaPersistenceCassandraVersion = "0.55"
val akkaPersistenceInMemVersion     = "2.5.1.1"
val akkaHttpCirceVersion            = "1.19.0"
val catsVersion                     = "1.0.1"
val circeVersion                    = "0.9.0"
val logbackVersion                  = "1.2.3"
val journalVersion                  = "3.0.19"
val commonsVersion                  = "0.7.4"
val metricsCoreVersion              = "3.2.6"
val scalaTestVersion                = "3.0.5"
val pureconfigVersion               = "0.8.0"

lazy val sourcingCore      = nexusDep("sourcing-core", commonsVersion)
lazy val sourcingAkka      = nexusDep("sourcing-akka", commonsVersion)
lazy val sourcingMem       = nexusDep("sourcing-mem", commonsVersion)
lazy val commonsService    = nexusDep("commons-service", commonsVersion)
lazy val commonsSchemas    = nexusDep("commons-schemas", commonsVersion)
lazy val commonsTest       = nexusDep("commons-test", commonsVersion)
lazy val shaclValidator    = nexusDep("shacl-validator", commonsVersion)
lazy val commonsQueryTypes = nexusDep("commons-query-types", commonsVersion)
lazy val iamCommons        = nexusDep("iam", commonsVersion)

lazy val akkaClusterSharding           = "com.typesafe.akka"     %% "akka-cluster-sharding"      % akkaVersion
lazy val akkaCors                      = "ch.megard"             %% "akka-http-cors"             % akkaHttpCorsVersion
lazy val akkaDistributed               = "com.typesafe.akka"     %% "akka-distributed-data"      % akkaVersion
lazy val akkaHttp                      = "com.typesafe.akka"     %% "akka-http"                  % akkaHttpVersion
lazy val akkaHttpCirce                 = "de.heikoseeberger"     %% "akka-http-circe"            % akkaHttpCirceVersion
lazy val akkaHttpTestKit               = "com.typesafe.akka"     %% "akka-http-testkit"          % akkaHttpVersion
lazy val akkaPersistenceCassandra      = "com.typesafe.akka"     %% "akka-persistence-cassandra" % akkaPersistenceCassandraVersion
lazy val akkaPersistenceCassandraInMem = "com.github.dnvriend"   %% "akka-persistence-inmemory"  % akkaPersistenceInMemVersion
lazy val akkaTestKit                   = "com.typesafe.akka"     %% "akka-testkit"               % akkaVersion
lazy val circeCore                     = "io.circe"              %% "circe-core"                 % circeVersion
lazy val circeExtras                   = "io.circe"              %% "circe-generic-extras"       % circeVersion
lazy val circeJava8                    = "io.circe"              %% "circe-java8"                % circeVersion
lazy val circeParser                   = "io.circe"              %% "circe-parser"               % circeVersion
lazy val logback                       = "ch.qos.logback"        % "logback-classic"             % logbackVersion
lazy val scalaTest                     = "org.scalatest"         %% "scalatest"                  % scalaTestVersion
lazy val slf4j                         = "com.typesafe.akka"     %% "akka-slf4j"                 % akkaVersion
lazy val metricsCore                   = "io.dropwizard.metrics" % "metrics-core"                % metricsCoreVersion
lazy val pureconfig                    = "com.github.pureconfig" %% "pureconfig"                 % pureconfigVersion

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(common, noPublish)
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

lazy val schemas = project
  .in(file("modules/kg-schemas"))
  .settings(common)
  .enablePlugins(WorkbenchPlugin)
  .disablePlugins(ScapegoatSbtPlugin, DocumentationPlugin)
  .settings(
    name       := "kg-schemas",
    moduleName := "kg-schemas",
    libraryDependencies ++= Seq(
      commonsSchemas
    )
  )

lazy val service = project
  .in(file("modules/service"))
  .dependsOn(schemas)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(common, buildInfoSettings, packagingSettings, noCoverage)
  .settings(
    name       := "kg-service",
    moduleName := "kg-service",
    libraryDependencies ++= Seq(
      akkaCors,
      akkaDistributed,
      akkaHttp,
      akkaHttpCirce,
      akkaPersistenceCassandra,
      circeCore,
      circeExtras,
      circeJava8,
      circeParser,
      commonsService,
      commonsTest,
      iamCommons,
      logback,
      metricsCore, // for cassandra client, or fails at runtime
      pureconfig,
      sourcingAkka,
      akkaHttpTestKit               % Test,
      akkaPersistenceCassandraInMem % Test,
      akkaTestKit                   % Test,
      scalaTest                     % Test,
      slf4j                         % Test,
      sourcingMem                   % Test
    )
  )
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val root = project
  .in(file("."))
  .settings(common, noPublish)
  .settings(
    name       := "kg",
    moduleName := "kg"
  )
  .aggregate(docs, service, schemas)

lazy val noPublish = Seq(publishLocal := {}, publish := {})

lazy val common = Seq(
  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Xfatal-warnings")),
  workbenchVersion := "0.3.0",
  homepage         := Some(url("https://github.com/BlueBrain/nexus-kg")),
  licenses         := Seq("Apache-2.0" -> url("https://github.com/BlueBrain/nexus-kg/blob/master/LICENSE")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/BlueBrain/nexus-kg"), "scm:git:git@github.com:BlueBrain/nexus-kg.git"))
)

lazy val noCoverage = Seq(coverageFailOnMinimum := false)

lazy val buildInfoSettings =
  Seq(buildInfoKeys := Seq[BuildInfoKey](version), buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.service.config")

lazy val packagingSettings = packageName in Docker := "kg"

def nexusDep(name: String, version: String): ModuleID = "ch.epfl.bluebrain.nexus" %% name % version

addCommandAlias("review", ";clean;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate;doc")
addCommandAlias("rel", ";release with-defaults skip-tests")
