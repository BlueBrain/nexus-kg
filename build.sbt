val commonsVersion     = "0.4.3"
val metricsCoreVersion = "3.2.2"
val jenaVersion        = "3.4.0"
val blazegraphVersion  = "2.1.4"
val jacksonVersion     = "2.9.0"

lazy val sourcingCore   = nexusDep("sourcing-core",   commonsVersion)
lazy val sourcingAkka   = nexusDep("sourcing-akka",   commonsVersion)
lazy val sourcingMem    = nexusDep("sourcing-mem",    commonsVersion)
lazy val serviceCommon  = nexusDep("service-commons", commonsVersion)
lazy val shaclValidator = nexusDep("shacl-validator", commonsVersion)
lazy val sparqlClient   = nexusDep("sparql-client",   commonsVersion)

lazy val docs = project.in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(common, noPublish)
  .settings(
    name                          := "kg-docs",
    moduleName                    := "kg-docs",
    paradoxTheme                  := Some(builtinParadoxTheme("generic")),
    target in (Compile, paradox)  := (resourceManaged in Compile).value / "docs",
    resourceGenerators in Compile += {
      (paradox in Compile).map { parent =>
        (parent ** "*").get
      }.taskValue
    })

lazy val core = project.in(file("modules/core"))
  .settings(common)
  .settings(
    name                 := "kg-core",
    moduleName           := "kg-core",
    libraryDependencies ++= Seq(
      sourcingCore, shaclValidator, sourcingMem % Test,
      "io.circe"           %% "circe-core"   % circeVersion.value,
      "io.circe"           %% "circe-optics" % circeVersion.value,
      "io.circe"           %% "circe-parser" % circeVersion.value,
      "io.verizon.journal" %% "core"         % journalVersion.value,
      "org.scalatest"      %% "scalatest"    % scalaTestVersion.value % Test
    ))

lazy val indexing = project.in(file("modules/indexing"))
  .dependsOn(core % "test->test;compile->compile")
  .settings(common)
  .settings(
    name                 := "kg-indexing",
    moduleName           := "kg-indexing",
    libraryDependencies ++= Seq(
      sourcingCore, sourcingMem % Test,
      sparqlClient,
      "com.typesafe.akka"          %% "akka-stream"         % akkaVersion.value,
      "com.typesafe.akka"          %% "akka-http"           % akkaHttpVersion.value,
      "de.heikoseeberger"          %% "akka-http-circe"     % akkaHttpCirceVersion.value,
      "io.circe"                   %% "circe-core"          % circeVersion.value,
      "io.circe"                   %% "circe-parser"        % circeVersion.value,
      "io.verizon.journal"         %% "core"                % journalVersion.value,
      "org.apache.jena"             % "jena-arq"            % jenaVersion,
      "org.apache.jena"             % "jena-querybuilder"   % jenaVersion,
      "com.blazegraph"              % "blazegraph-jar"      % blazegraphVersion           % Test,
      "com.fasterxml.jackson.core"  % "jackson-annotations" % jacksonVersion              % Test,
      "com.fasterxml.jackson.core"  % "jackson-core"        % jacksonVersion              % Test,
      "com.fasterxml.jackson.core"  % "jackson-databind"    % jacksonVersion              % Test,
      "com.typesafe.akka"          %% "akka-testkit"        % akkaVersion.value           % Test,
      "org.scalatest"              %% "scalatest"           % scalaTestVersion.value      % Test
    ))
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val service = project.in(file("modules/service"))
  .dependsOn(core % "test->test;compile->compile", indexing % "test->test;compile->compile", docs)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(common, buildInfoSettings, packagingSettings, noCoverage)
  .settings(
    name                 := "kg-service",
    moduleName           := "kg-service",
    libraryDependencies ++= kamonDeps ++ Seq(
      serviceCommon, sourcingAkka, sourcingMem % Test,
      "ch.megard"                  %% "akka-http-cors"             % akkaHttpCorsVersion.value,
      "ch.qos.logback"              % "logback-classic"            % logbackVersion.value,
      "com.typesafe.akka"          %% "akka-slf4j"                 % akkaVersion.value,
      "com.typesafe.akka"          %% "akka-http"                  % akkaHttpVersion.value,
      "com.typesafe.akka"          %% "akka-distributed-data"      % akkaVersion.value,
      "com.typesafe.akka"          %% "akka-persistence-cassandra" % akkaPersistenceCassandraVersion.value,
      "io.dropwizard.metrics"       % "metrics-core"               % metricsCoreVersion, // for cassandra client, or fails at runtime
      "de.heikoseeberger"          %% "akka-http-circe"            % akkaHttpCirceVersion.value,
      "io.circe"                   %% "circe-core"                 % circeVersion.value,
      "io.circe"                   %% "circe-parser"               % circeVersion.value,
      "io.circe"                   %% "circe-generic-extras"       % circeVersion.value,
      "io.circe"                   %% "circe-java8"                % circeVersion.value,
      "com.fasterxml.jackson.core"  % "jackson-annotations"        % jacksonVersion                    % Test,
      "com.fasterxml.jackson.core"  % "jackson-core"               % jacksonVersion                    % Test,
      "com.fasterxml.jackson.core"  % "jackson-databind"           % jacksonVersion                    % Test,
      "com.blazegraph"              % "blazegraph-jar"             % blazegraphVersion                 % Test,
      "com.github.dnvriend"        %% "akka-persistence-inmemory"  % akkaPersistenceInMemVersion.value % Test,
      "com.typesafe.akka"          %% "akka-http-testkit"          % akkaHttpVersion.value             % Test,
      "com.typesafe.akka"          %% "akka-testkit"               % akkaVersion.value                 % Test,
      "org.scalatest"              %% "scalatest"                  % scalaTestVersion.value            % Test
    ))
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)
  .settings(
    bashScriptExtraDefines ++= Seq(
      """addJava "-javaagent:$lib_dir/org.aspectj.aspectjweaver-1.8.10.jar"""",
      """addJava "-javaagent:$lib_dir/io.kamon.sigar-loader-1.6.6-rev002.jar""""
    )
  )
lazy val tests = project.in(file("modules/tests"))
  .dependsOn(core % "test->test;compile->compile", service % "test->test;compile->compile")
  .settings(common)
  .settings(
    name                  := "kg-tests",
    moduleName            := "kg-tests",
    libraryDependencies  ++= Seq(
      "com.typesafe.akka" %% "akka-persistence-cassandra-launcher" % akkaPersistenceCassandraVersion.value % Test
    ))
  // IMPORTANT! Jena initialization system fails miserably in concurrent scenarios. Disabling parallel execution for
  // tests reduces false negatives.
  .settings(parallelExecution in Test := false)

lazy val root = project.in(file("."))
  .settings(common, noPublish)
  .settings(
    name        := "kg",
    moduleName  := "kg",
    homepage    := Some (new URL("https://github.com/BlueBrain/nexus-kg")),
    description := "Nexus KnowledgeGraph",
    licenses    := Seq(
      ("Apache 2.0", new URL("https://github.com/BlueBrain/nexus-kg/blob/master/LICENSE"))))
  .aggregate(docs, core, indexing, service, tests)

lazy val noPublish = Seq(
  publishLocal := {},
  publish      := {})

lazy val common = Seq(
  scalacOptions in (Compile, console) ~= (_ filterNot (_ == "-Xfatal-warnings")),
  resolvers                           += Resolver.bintrayRepo("bogdanromanx", "maven"))

lazy val noCoverage = Seq(
  coverageFailOnMinimum := false)

lazy val buildInfoSettings = Seq(
  buildInfoKeys    := Seq[BuildInfoKey](version),
  buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.service.config")

lazy val packagingSettings = packageName in Docker := "kg"

def nexusDep(name: String, version: String): ModuleID =
  "ch.epfl.bluebrain.nexus" %% name % version

lazy val kamonDeps = Seq(
  "io.kamon"    %% "kamon-core"            % "0.6.7",
  "io.kamon"    %% "kamon-akka-http"       % "0.6.8",
  "io.kamon"    %% "kamon-statsd"          % "0.6.7"        % Runtime,
  "io.kamon"    %% "kamon-system-metrics"  % "0.6.7"        % Runtime,
  "io.kamon"    %% "kamon-akka-2.5"        % "0.6.8"        % Runtime,
  "io.kamon"    %% "kamon-akka-remote-2.4" % "0.6.7"        % Runtime,
  "io.kamon"    %% "kamon-autoweave"       % "0.6.5"        % Runtime,
  "io.kamon"     % "sigar-loader"          % "1.6.6-rev002" % Runtime,
  "org.aspectj"  % "aspectjweaver"         % "1.8.10"       % Runtime
)

addCommandAlias("review", ";clean;coverage;scapegoat;test;coverageReport;coverageAggregate")
addCommandAlias("rel",    ";release with-defaults skip-tests")
