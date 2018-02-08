val akkaVersion                     = "2.5.9"
val akkaHttpVersion                 = "10.0.11"
val akkaHttpCorsVersion             = "0.2.2"
val akkaPersistenceCassandraVersion = "0.55"
val akkaPersistenceInMemVersion     = "2.5.1.1"
val akkaHttpCirceVersion            = "1.19.0"
val akkaStreamKafkaVersion          = "0.18"
val catsVersion                     = "1.0.1"
val circeVersion                    = "0.9.0"
val logbackVersion                  = "1.2.3"
val journalVersion                  = "3.0.19"
val commonsVersion                  = "0.7.3"
val metricsCoreVersion              = "3.2.6"
val jenaVersion                     = "3.6.0"
val blazegraphVersion               = "2.1.4"
val jacksonVersion                  = "2.8.10"
val scalaTestVersion                = "3.0.4"
val asmVersion                      = "5.1"

lazy val akkaClusterSharding = "com.typesafe.akka" %% "akka-cluster-sharding" % akkaVersion
lazy val sourcingCore        = nexusDep("sourcing-core", commonsVersion)
lazy val sourcingAkka        = nexusDep("sourcing-akka", commonsVersion)
lazy val sourcingMem         = nexusDep("sourcing-mem", commonsVersion)
lazy val commonsService      = nexusDep("commons-service", commonsVersion)
lazy val commonsSchemas      = nexusDep("commons-schemas", commonsVersion)
lazy val commonsTest         = nexusDep("commons-test", commonsVersion)
lazy val shaclValidator      = nexusDep("shacl-validator", commonsVersion)
lazy val sparqlClient        = nexusDep("sparql-client", commonsVersion)
lazy val elasticClient       = nexusDep("elastic-client", commonsVersion)
lazy val elasticEmbed        = nexusDep("elastic-server-embed", commonsVersion)
lazy val commonsQueryTypes   = nexusDep("commons-query-types", commonsVersion)
lazy val asm                 = "org.ow2.asm" % "asm" % asmVersion

lazy val iamCommons = nexusDep("iam", commonsVersion)

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(common, noPublish)
  .settings(
    name := "kg-docs",
    moduleName := "kg-docs",
    paradoxTheme := Some(builtinParadoxTheme("generic")),
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
    name := "kg-schemas",
    moduleName := "kg-schemas",
    libraryDependencies ++= Seq(
      commonsSchemas
    )
  )

lazy val core = project
  .in(file("modules/core"))
  .dependsOn(schemas)
  .settings(common)
  .settings(
    name := "kg-core",
    moduleName := "kg-core",
    libraryDependencies ++= Seq(
      iamCommons,
      sourcingCore,
      shaclValidator,
      commonsQueryTypes,
      akkaClusterSharding,
      sourcingMem          % Test,
      commonsTest          % Test,
      "io.circe"           %% "circe-core" % circeVersion,
      "io.circe"           %% "circe-optics" % circeVersion,
      "io.circe"           %% "circe-parser" % circeVersion,
      "io.verizon.journal" %% "core" % journalVersion,
      "org.apache.jena"    % "jena-arq" % jenaVersion,
      "org.apache.jena"    % "jena-querybuilder" % jenaVersion,
      "org.scalatest"      %% "scalatest" % scalaTestVersion % Test,
      "com.typesafe.akka"  %% "akka-http-testkit" % akkaHttpVersion % Test,
      "com.typesafe.akka"  %% "akka-testkit" % akkaVersion % Test
    )
  )

lazy val sparql = project
  .in(file("modules/sparql"))
  .dependsOn(core)
  .settings(common)
  .settings(
    name := "kg-sparql",
    moduleName := "kg-sparql",
    libraryDependencies ++= Seq(
      iamCommons,
      sourcingCore,
      sourcingMem % Test,
      commonsTest % Test,
      sparqlClient,
      "com.typesafe.akka"          %% "akka-stream"        % akkaVersion,
      "com.typesafe.akka"          %% "akka-http"          % akkaHttpVersion,
      "de.heikoseeberger"          %% "akka-http-circe"    % akkaHttpCirceVersion,
      "io.circe"                   %% "circe-core"         % circeVersion,
      "io.circe"                   %% "circe-parser"       % circeVersion,
      "io.verizon.journal"         %% "core"               % journalVersion,
      "com.blazegraph"             % "blazegraph-jar"      % blazegraphVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-core"        % jacksonVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonVersion % Test,
      "com.typesafe.akka"          %% "akka-testkit"       % akkaVersion % Test,
      "org.scalatest"              %% "scalatest"          % scalaTestVersion % Test
    )
  )

lazy val elastic = project
  .in(file("modules/elastic"))
  .dependsOn(core)
  .settings(common)
  .settings(
    name := "kg-elastic",
    moduleName := "kg-elastic",
    libraryDependencies ++= Seq(
      iamCommons,
      sourcingCore,
      sourcingMem  % Test,
      asm          % Test,
      commonsTest  % Test,
      elasticEmbed % Test,
      elasticClient,
      "com.typesafe.akka"  %% "akka-stream"     % akkaVersion,
      "com.typesafe.akka"  %% "akka-http"       % akkaHttpVersion,
      "de.heikoseeberger"  %% "akka-http-circe" % akkaHttpCirceVersion,
      "io.circe"           %% "circe-core"      % circeVersion,
      "io.circe"           %% "circe-parser"    % circeVersion,
      "io.verizon.journal" %% "core"            % journalVersion,
      "com.typesafe.akka"  %% "akka-testkit"    % akkaVersion % Test,
      "org.scalatest"      %% "scalatest"       % scalaTestVersion % Test
    )
  )
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val service = project
  .in(file("modules/service"))
  .dependsOn(core % "test->test;compile->compile", sparql, elastic, docs)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(common, buildInfoSettings, packagingSettings, noCoverage)
  .settings(
    name := "kg-service",
    moduleName := "kg-service",
    libraryDependencies ++= Seq(
      iamCommons,
      commonsService,
      sourcingAkka,
      commonsTest,
      sourcingMem                  % Test,
      "ch.megard"                  %% "akka-http-cors" % akkaHttpCorsVersion,
      "ch.qos.logback"             % "logback-classic" % logbackVersion,
      "com.typesafe.akka"          %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka"          %% "akka-http" % akkaHttpVersion,
      "com.typesafe.akka"          %% "akka-distributed-data" % akkaVersion,
      "com.typesafe.akka"          %% "akka-persistence-cassandra" % akkaPersistenceCassandraVersion,
      "com.typesafe.akka"          %% "akka-stream-kafka" % akkaStreamKafkaVersion,
      "io.dropwizard.metrics"      % "metrics-core" % metricsCoreVersion, // for cassandra client, or fails at runtime
      "de.heikoseeberger"          %% "akka-http-circe" % akkaHttpCirceVersion,
      "io.circe"                   %% "circe-core" % circeVersion,
      "io.circe"                   %% "circe-parser" % circeVersion,
      "io.circe"                   %% "circe-generic-extras" % circeVersion,
      "io.circe"                   %% "circe-java8" % circeVersion,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion % Test,
      "com.blazegraph"             % "blazegraph-jar" % blazegraphVersion % Test,
      "com.github.dnvriend"        %% "akka-persistence-inmemory" % akkaPersistenceInMemVersion % Test,
      "com.typesafe.akka"          %% "akka-http-testkit" % akkaHttpVersion % Test,
      "com.typesafe.akka"          %% "akka-testkit" % akkaVersion % Test,
      "net.manub"                  %% "scalatest-embedded-kafka" % "1.0.0" % Test,
      "org.scalatest"              %% "scalatest" % scalaTestVersion % Test
    )
  )
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val tests = project
  .in(file("modules/tests"))
  .dependsOn(core % "test->test;compile->compile", service % "test->test;compile->compile")
  .settings(common)
  .settings(
    name := "kg-tests",
    moduleName := "kg-tests",
    libraryDependencies ++= Seq(
      commonsTest         % Test,
      "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % akkaPersistenceCassandraVersion % Test
    )
  )
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val root = project
  .in(file("."))
  .settings(common, noPublish)
  .settings(
    name := "kg",
    moduleName := "kg"
  )
  .aggregate(docs, core, sparql, elastic, service, tests, schemas)

lazy val noPublish = Seq(publishLocal := {}, publish := {})

lazy val common = Seq(
  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Xfatal-warnings")),
  workbenchVersion := "0.3.0",
  homepage := Some(url("https://github.com/BlueBrain/nexus-kg")),
  licenses := Seq("Apache-2.0" -> url("https://github.com/BlueBrain/nexus-kg/blob/master/LICENSE")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/BlueBrain/nexus-kg"), "scm:git:git@github.com:BlueBrain/nexus-kg.git"))
)

lazy val noCoverage = Seq(coverageFailOnMinimum := false)

lazy val buildInfoSettings =
  Seq(buildInfoKeys := Seq[BuildInfoKey](version), buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.service.config")

lazy val packagingSettings = packageName in Docker := "kg"

def nexusDep(name: String, version: String): ModuleID =
  "ch.epfl.bluebrain.nexus" %% name % version

addCommandAlias("review", ";clean;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate;doc")
addCommandAlias("rel", ";release with-defaults skip-tests")
