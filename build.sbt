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

// Library dependency versions
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
val metricsCoreVersion              = "3.2.6"
val jenaVersion                     = "3.6.0"
val blazegraphVersion               = "2.1.4"
val jacksonVersion                  = "2.8.10"
val scalaTestVersion                = "3.0.4"
val asmVersion                      = "5.1"
val nettyVersion                    = "3.10.6.Final"
val luceneVersion                   = "7.1.0"
val elasticSearchVersion            = "6.1.2"

// Nexus dependency versions
val commonsVersion = "0.7.15"

// Library dependencies
lazy val akkaClusterSharding      = "com.typesafe.akka"          %% "akka-cluster-sharding"               % akkaVersion
lazy val akkaDistributedData      = "com.typesafe.akka"          %% "akka-distributed-data"               % akkaVersion
lazy val akkaHttp                 = "com.typesafe.akka"          %% "akka-http"                           % akkaHttpVersion
lazy val akkaHttpCirce            = "de.heikoseeberger"          %% "akka-http-circe"                     % akkaHttpCirceVersion
lazy val akkaHttpCors             = "ch.megard"                  %% "akka-http-cors"                      % akkaHttpCorsVersion
lazy val akkaHttpTestkit          = "com.typesafe.akka"          %% "akka-http-testkit"                   % akkaHttpVersion
lazy val akkaPersistenceCassandra = "com.typesafe.akka"          %% "akka-persistence-cassandra"          % akkaPersistenceCassandraVersion
lazy val akkaPersistenceInMem     = "com.github.dnvriend"        %% "akka-persistence-inmemory"           % akkaPersistenceInMemVersion
lazy val akkaSlf4j                = "com.typesafe.akka"          %% "akka-slf4j"                          % akkaVersion
lazy val akkaStream               = "com.typesafe.akka"          %% "akka-stream"                         % akkaVersion
lazy val akkaTestkit              = "com.typesafe.akka"          %% "akka-testkit"                        % akkaVersion
lazy val asm                      = "org.ow2.asm"                % "asm"                                  % asmVersion
lazy val blazegraph               = "com.blazegraph"             % "blazegraph-jar"                       % blazegraphVersion
lazy val cassandraLauncher        = "com.typesafe.akka"          %% "akka-persistence-cassandra-launcher" % akkaPersistenceCassandraVersion
lazy val circeCore                = "io.circe"                   %% "circe-core"                          % circeVersion
lazy val circeJava8               = "io.circe"                   %% "circe-java8"                         % circeVersion
lazy val circeGenericExtras       = "io.circe"                   %% "circe-generic-extras"                % circeVersion
lazy val circeOptics              = "io.circe"                   %% "circe-optics"                        % circeVersion
lazy val circeParser              = "io.circe"                   %% "circe-parser"                        % circeVersion
lazy val elasticSearch            = "org.elasticsearch"          % "elasticsearch"                        % elasticSearchVersion
lazy val elasticSearchNettyClient = "org.elasticsearch.plugin"   % "transport-netty4-client"              % elasticSearchVersion
lazy val jacksonCore              = "com.fasterxml.jackson.core" % "jackson-core"                         % jacksonVersion
lazy val jacksonDatabind          = "com.fasterxml.jackson.core" % "jackson-databind"                     % jacksonVersion
lazy val jacksonAnnotations       = "com.fasterxml.jackson.core" % "jackson-annotations"                  % jacksonVersion
lazy val jenaArq                  = "org.apache.jena"            % "jena-arq"                             % jenaVersion
lazy val jenaQueryBuilder         = "org.apache.jena"            % "jena-querybuilder"                    % jenaVersion
lazy val journalCore              = "io.verizon.journal"         %% "core"                                % journalVersion
lazy val logbackClassic           = "ch.qos.logback"             % "logback-classic"                      % logbackVersion
lazy val luceneCore               = "org.apache.lucene"          % "lucene-core"                          % luceneVersion
lazy val metricsCore              = "io.dropwizard.metrics"      % "metrics-core"                         % metricsCoreVersion
lazy val netty                    = "io.netty"                   % "netty"                                % nettyVersion
lazy val scalaTest                = "org.scalatest"              %% "scalatest"                           % scalaTestVersion

// Nexus dependencies
lazy val commonsIAM        = "ch.epfl.bluebrain.nexus" %% "iam"                  % commonsVersion
lazy val commonsKamon      = "ch.epfl.bluebrain.nexus" %% "commons-kamon"        % commonsVersion
lazy val commonsQueryTypes = "ch.epfl.bluebrain.nexus" %% "commons-query-types"  % commonsVersion
lazy val commonsSchemas    = "ch.epfl.bluebrain.nexus" %% "commons-schemas"      % commonsVersion
lazy val commonsService    = "ch.epfl.bluebrain.nexus" %% "commons-service"      % commonsVersion
lazy val commonsTest       = "ch.epfl.bluebrain.nexus" %% "commons-test"         % commonsVersion
lazy val elasticClient     = "ch.epfl.bluebrain.nexus" %% "elastic-client"       % commonsVersion
lazy val elasticEmbed      = "ch.epfl.bluebrain.nexus" %% "elastic-server-embed" % commonsVersion
lazy val shaclValidator    = "ch.epfl.bluebrain.nexus" %% "shacl-validator"      % commonsVersion
lazy val sourcingAkka      = "ch.epfl.bluebrain.nexus" %% "sourcing-akka"        % commonsVersion
lazy val sourcingCore      = "ch.epfl.bluebrain.nexus" %% "sourcing-core"        % commonsVersion
lazy val sourcingMem       = "ch.epfl.bluebrain.nexus" %% "sourcing-mem"         % commonsVersion
lazy val sparqlClient      = "ch.epfl.bluebrain.nexus" %% "sparql-client"        % commonsVersion

lazy val docs = project
  .in(file("docs"))
  .enablePlugins(ParadoxPlugin)
  .settings(common)
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
  .enablePlugins(WorkbenchPlugin)
  .disablePlugins(ScapegoatSbtPlugin, DocumentationPlugin)
  .settings(common)
  .settings(
    name                := "kg-schemas",
    moduleName          := "kg-schemas",
    libraryDependencies += commonsSchemas
  )

lazy val core = project
  .in(file("modules/core"))
  .dependsOn(schemas)
  .settings(common)
  .settings(
    name       := "kg-core",
    moduleName := "kg-core",
    libraryDependencies ++= Seq(
      akkaClusterSharding,
      commonsIAM,
      commonsQueryTypes,
      circeCore,
      circeOptics,
      circeParser,
      jenaArq,
      jenaQueryBuilder,
      journalCore,
      shaclValidator,
      sourcingCore,
      sourcingMem     % Test,
      commonsTest     % Test,
      scalaTest       % Test,
      akkaHttpTestkit % Test,
      akkaTestkit     % Test
    ),
    Test / fork              := true,
    Test / parallelExecution := false // workaround for jena initialization
  )

lazy val sparql = project
  .in(file("modules/sparql"))
  .dependsOn(core)
  .settings(common)
  .settings(
    name       := "kg-sparql",
    moduleName := "kg-sparql",
    libraryDependencies ++= Seq(
      akkaHttp,
      akkaHttpCirce,
      akkaStream,
      circeCore,
      circeParser,
      journalCore,
      commonsIAM,
      sourcingCore,
      sparqlClient,
      akkaSlf4j          % Test,
      akkaTestkit        % Test,
      blazegraph         % Test,
      commonsTest        % Test,
      jacksonAnnotations % Test,
      jacksonCore        % Test,
      jacksonDatabind    % Test,
      scalaTest          % Test,
      sourcingMem        % Test
    ),
    Test / fork              := true,
    Test / parallelExecution := false // workaround for jena initialization
  )

lazy val elastic = project
  .in(file("modules/elastic"))
  .dependsOn(core)
  .settings(common)
  .settings(
    name       := "kg-elastic",
    moduleName := "kg-elastic",
    libraryDependencies ++= Seq(
      akkaHttp,
      akkaHttpCirce,
      akkaStream,
      circeCore,
      circeParser,
      commonsIAM,
      elasticClient,
      journalCore,
      sourcingCore,
      akkaTestkit  % Test,
      asm          % Test,
      commonsTest  % Test,
      elasticEmbed % Test,
      scalaTest    % Test,
      sourcingMem  % Test
    ),
    Test / fork              := true,
    Test / parallelExecution := false // workaround for jena initialization
  )

lazy val service = project
  .in(file("modules/service"))
  .dependsOn(core % "test->test;compile->compile", sparql, elastic, docs)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(common, buildInfoSettings)
  .settings(
    name                  := "kg-service",
    moduleName            := "kg-service",
    packageName in Docker := "kg",
    coverageFailOnMinimum := false,
    libraryDependencies ++= Seq(
      akkaDistributedData,
      akkaHttp,
      akkaHttpCirce,
      akkaHttpCors,
      akkaPersistenceCassandra,
      akkaSlf4j,
      circeCore,
      circeGenericExtras,
      circeJava8,
      circeParser,
      commonsIAM,
      commonsKamon,
      commonsService,
      commonsTest,
      logbackClassic,
      metricsCore, // for cassandra client, or fails at runtime
      sourcingAkka,
      sourcingMem          % Test,
      akkaHttpTestkit      % Test,
      akkaPersistenceInMem % Test,
      akkaTestkit          % Test,
      blazegraph           % Test,
      jacksonAnnotations   % Test,
      jacksonCore          % Test,
      jacksonDatabind      % Test,
      scalaTest            % Test
    ),
    Test / fork              := true,
    Test / parallelExecution := false // workaround for jena initialization
  )

lazy val testsBlazegraph = project
  .in(file("modules/tests-blazegraph"))
  .dependsOn(core % "test->test;compile->compile", service % "test->test;compile->compile")
  .settings(common, noPublish)
  .settings(
    name       := "kg-tests-blazegraph",
    moduleName := "kg-tests-blazegraph",
    libraryDependencies ++= Seq(
      cassandraLauncher % Test,
      commonsTest       % Test
    ),
    Test / fork              := true,
    Test / parallelExecution := false // workaround for jena initialization
  )

lazy val testsElastic = project
  .in(file("modules/tests-elastic"))
  .dependsOn(core % "test->test;compile->compile", service % "test->test;compile->compile")
  .settings(common, noPublish)
  .settings(
    name       := "kg-tests-elastic",
    moduleName := "kg-tests-elastic",
    libraryDependencies ++= Seq(
      elasticSearch,
      elasticSearchNettyClient,
      luceneCore,
      cassandraLauncher % Test,
      commonsTest       % Test,
//      jacksonAnnotations % Test,
//      jacksonCore        % Test,
//      jacksonDatabind    % Test,
      netty % Test
    ),
    dependencyOverrides ++= Seq(
      elasticSearch,
      luceneCore
//      jacksonAnnotations % Test,
//      jacksonCore        % Test,
//      jacksonDatabind    % Test,
    ),
    Test / fork              := true,
    Test / parallelExecution := false // workaround for jena initialization
  )

lazy val root = project
  .in(file("."))
  .settings(common, noPublish)
  .settings(
    name       := "kg",
    moduleName := "kg"
  )
  .aggregate(docs, core, sparql, elastic, service, testsBlazegraph, testsElastic, schemas)

/* ********************************************************
 ******************** Grouped Settings ********************
 **********************************************************/

lazy val common = Seq(
  workbenchVersion := "0.3.0",
  resolvers        += Resolver.bintrayRepo("bogdanromanx", "maven")
)

lazy val buildInfoSettings = Seq(
  buildInfoKeys    := Seq[BuildInfoKey](version),
  buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.service.config"
)

lazy val noPublish = Seq(
  publishLocal    := {},
  publish         := {},
  publishArtifact := false
)

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
    releaseEarlyEnableSyncToMaven := false
  ))

addCommandAlias("review", ";clean;scalafmtCheck;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate;doc")
