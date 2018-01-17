val akkaVersion                     = "2.5.4"
val akkaHttpVersion                 = "10.0.10"
val akkaHttpCorsVersion             = "0.2.1"
val akkaPersistenceCassandraVersion = "0.55"
val akkaPersistenceInMemVersion     = "2.5.1.1"
val akkaHttpCirceVersion            = "1.18.0"
val akkaStreamKafkaVersion          = "0.17"
val catsVersion                     = "0.9.0"
val circeVersion                    = "0.8.0"
val logbackVersion                  = "1.2.3"
val journalVersion                  = "3.0.19"
val commonsVersion                  = "0.5.30"
val metricsCoreVersion              = "3.2.2"
val jenaVersion                     = "3.3.0"
val jsonldJavaVersion               = "0.9.0" // TODO: remove once we upgrade to Jena 3.4
val blazegraphVersion               = "2.1.4"
val jacksonVersion                  = "2.9.0"
val scalaTestVersion                = "3.0.4"

lazy val sourcingCore   = nexusDep("sourcing-core", commonsVersion)
lazy val sourcingAkka   = nexusDep("sourcing-akka", commonsVersion)
lazy val sourcingMem    = nexusDep("sourcing-mem", commonsVersion)
lazy val commonsService = nexusDep("commons-service", commonsVersion)
lazy val commonsSchemas = nexusDep("commons-schemas", commonsVersion)
lazy val commonsTest    = nexusDep("commons-test", commonsVersion)
lazy val shaclValidator = nexusDep("shacl-validator", commonsVersion)
lazy val sparqlClient   = nexusDep("sparql-client", commonsVersion)
lazy val iamCommons     = nexusDep("iam", commonsVersion)

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

lazy val core = project
  .in(file("modules/core"))
  .settings(common)
  .settings(
    name := "kg-core",
    moduleName := "kg-core",
    libraryDependencies ++= Seq(
      iamCommons,
      sourcingCore,
      shaclValidator,
      sourcingMem          % Test,
      commonsTest          % Test,
      "io.circe"           %% "circe-core" % circeVersion,
      "io.circe"           %% "circe-optics" % circeVersion,
      "io.circe"           %% "circe-parser" % circeVersion,
      "io.verizon.journal" %% "core" % journalVersion,
      "org.scalatest"      %% "scalatest" % scalaTestVersion % Test
    )
  )

lazy val schemas = project
  .in(file("modules/kg-schemas"))
  .settings(common)
  .enablePlugins(WorkbenchPlugin)
  .disablePlugins(ScapegoatSbtPlugin, DocumentationPlugin)
  .settings(publishSettings)
  .settings(
    name := "kg-schemas",
    moduleName := "kg-schemas",
    libraryDependencies ++= Seq(
      commonsSchemas
    )
  )

lazy val indexing = project
  .in(file("modules/indexing"))
  .dependsOn(core)
  .settings(publishSettings)
  .settings(common)
  .settings(
    name := "kg-indexing",
    moduleName := "kg-indexing",
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
      "org.apache.jena"            % "jena-arq"            % jenaVersion,
      "org.apache.jena"            % "jena-querybuilder"   % jenaVersion,
      "com.blazegraph"             % "blazegraph-jar"      % blazegraphVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-core"        % jacksonVersion % Test,
      "com.fasterxml.jackson.core" % "jackson-databind"    % jacksonVersion % Test,
      "com.typesafe.akka"          %% "akka-testkit"       % akkaVersion % Test,
      "org.scalatest"              %% "scalatest"          % scalaTestVersion % Test
    )
  )
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val service = project
  .in(file("modules/service"))
  .dependsOn(core % "test->test;compile->compile", indexing, docs)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(publishSettings)
  .settings(common, buildInfoSettings, packagingSettings, noCoverage)
  .settings(
    name := "kg-service",
    moduleName := "kg-service",
    libraryDependencies ++= kamonDeps ++ Seq(
      iamCommons,
      commonsService,
      sourcingAkka,
      sourcingMem                  % Test,
      commonsTest                  % Test,
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
  .settings(
    bashScriptExtraDefines ++= Seq(
      """addJava "-javaagent:$lib_dir/org.aspectj.aspectjweaver-1.8.10.jar"""",
      """addJava "-javaagent:$lib_dir/io.kamon.sigar-loader-1.6.6-rev002.jar""""
    )
  )
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
  .aggregate(docs, core, indexing, service, tests, schemas)

lazy val noPublish = Seq(publishLocal := {}, publish := {})

lazy val publishSettings = Seq(
  homepage := Some(url("https://github.com/BlueBrain/nexus-kg")),
  licenses := Seq("Apache-2.0" -> url("https://github.com/BlueBrain/nexus-kg/blob/master/LICENSE")),
  scmInfo := Some(
    ScmInfo(url("https://github.com/BlueBrain/nexus-kg"), "scm:git:git@github.com:BlueBrain/nexus-kg.git"))
)

lazy val common = Seq(
  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Xfatal-warnings")),
  resolvers += Resolver.bintrayRepo("bogdanromanx", "maven"),
  workbenchVersion := "0.2.0"
)

lazy val noCoverage = Seq(coverageFailOnMinimum := false)

lazy val buildInfoSettings =
  Seq(buildInfoKeys := Seq[BuildInfoKey](version), buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.service.config")

lazy val packagingSettings = packageName in Docker := "kg"

def nexusDep(name: String, version: String): ModuleID =
  "ch.epfl.bluebrain.nexus" %% name % version

lazy val kamonDeps = Seq(
  "io.kamon"    %% "kamon-core"            % "0.6.7",
  "io.kamon"    %% "kamon-akka-http"       % "0.6.8",
  "io.kamon"    %% "kamon-statsd"          % "0.6.7" % Runtime,
  "io.kamon"    %% "kamon-system-metrics"  % "0.6.7" % Runtime,
  "io.kamon"    %% "kamon-akka-2.5"        % "0.6.8" % Runtime,
  "io.kamon"    %% "kamon-akka-remote-2.4" % "0.6.7" % Runtime,
  "io.kamon"    %% "kamon-autoweave"       % "0.6.5" % Runtime,
  "io.kamon"    % "sigar-loader"           % "1.6.6-rev002" % Runtime,
  "org.aspectj" % "aspectjweaver"          % "1.8.10" % Runtime
)

addCommandAlias("review", ";clean;coverage;scapegoat;test;coverageReport;coverageAggregate")
addCommandAlias("rel", ";release with-defaults skip-tests")
