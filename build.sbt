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

// Dependency versions
val adminVersion                = "1.0.3"
val iamVersion                  = "1.0.2"
val commonsVersion              = "0.10.44"
val rdfVersion                  = "0.2.34"
val serviceVersion              = "0.10.34"
val sourcingVersion             = "0.12.4"
val akkaVersion                 = "2.5.21"
val akkaCorsVersion             = "0.3.4"
val akkaHttpVersion             = "10.1.7"
val akkaPersistenceInMemVersion = "2.5.1.1"
val akkaPersistenceCassVersion  = "0.97"
val catsVersion                 = "1.6.0"
val catsEffectVersion           = "1.2.0"
val circeVersion                = "0.11.1"
val journalVersion              = "3.0.19"
val logbackVersion              = "1.2.3"
val mockitoVersion              = "1.1.4"
val monixVersion                = "3.0.0-RC2"
val pureconfigVersion           = "0.10.2"
val shapelessVersion            = "2.3.3"
val scalaTestVersion            = "3.0.5"
val kryoVersion                 = "0.5.2"

// Dependencies modules
lazy val adminClient          = "ch.epfl.bluebrain.nexus" %% "admin-client"                % adminVersion
lazy val iamClient            = "ch.epfl.bluebrain.nexus" %% "iam-client"                  % iamVersion
lazy val elasticSearchClient  = "ch.epfl.bluebrain.nexus" %% "elastic-search-client"       % commonsVersion
lazy val rdfCore              = "ch.epfl.bluebrain.nexus" %% "rdf-core"                    % rdfVersion
lazy val rdfDot               = "ch.epfl.bluebrain.nexus" %% "rdf-dot"                     % rdfVersion
lazy val rdfJena              = "ch.epfl.bluebrain.nexus" %% "rdf-jena"                    % rdfVersion
lazy val rdfAkka              = "ch.epfl.bluebrain.nexus" %% "rdf-akka"                    % rdfVersion
lazy val rdfCirce             = "ch.epfl.bluebrain.nexus" %% "rdf-circe"                   % rdfVersion
lazy val rdfNexus             = "ch.epfl.bluebrain.nexus" %% "rdf-nexus"                   % rdfVersion
lazy val serviceIndexing      = "ch.epfl.bluebrain.nexus" %% "service-indexing"            % serviceVersion
lazy val serviceTest          = "ch.epfl.bluebrain.nexus" %% "service-test"                % serviceVersion
lazy val serviceKamon         = "ch.epfl.bluebrain.nexus" %% "service-kamon"               % serviceVersion
lazy val serviceHttp          = "ch.epfl.bluebrain.nexus" %% "service-http"                % serviceVersion
lazy val serviceSerialization = "ch.epfl.bluebrain.nexus" %% "service-serialization"       % serviceVersion
lazy val sourcingCore         = "ch.epfl.bluebrain.nexus" %% "sourcing-core"               % sourcingVersion
lazy val sourcingAkka         = "ch.epfl.bluebrain.nexus" %% "sourcing-akka"               % sourcingVersion
lazy val shaclValidator       = "ch.epfl.bluebrain.nexus" %% "shacl-validator"             % commonsVersion
lazy val sparqlClient         = "ch.epfl.bluebrain.nexus" %% "sparql-client"               % commonsVersion
lazy val akkaCluster          = "com.typesafe.akka"       %% "akka-cluster"                % akkaVersion
lazy val akkaClusterSharding  = "com.typesafe.akka"       %% "akka-cluster-sharding"       % akkaVersion
lazy val akkaDistributedData  = "com.typesafe.akka"       %% "akka-distributed-data"       % akkaVersion
lazy val akkaHttp             = "com.typesafe.akka"       %% "akka-http"                   % akkaHttpVersion
lazy val akkaHttpCors         = "ch.megard"               %% "akka-http-cors"              % akkaCorsVersion
lazy val akkaHttpTestKit      = "com.typesafe.akka"       %% "akka-http-testkit"           % akkaHttpVersion
lazy val akkaPersistence      = "com.typesafe.akka"       %% "akka-persistence"            % akkaVersion
lazy val akkaPersistenceCass  = "com.typesafe.akka"       %% "akka-persistence-cassandra"  % akkaPersistenceCassVersion
lazy val akkaPersistenceInMem = "com.github.dnvriend"     %% "akka-persistence-inmemory"   % akkaPersistenceInMemVersion
lazy val akkaSlf4j            = "com.typesafe.akka"       %% "akka-slf4j"                  % akkaVersion
lazy val akkaStream           = "com.typesafe.akka"       %% "akka-stream"                 % akkaVersion
lazy val catsCore             = "org.typelevel"           %% "cats-core"                   % catsVersion
lazy val catsEffect           = "org.typelevel"           %% "cats-effect"                 % catsEffectVersion
lazy val circeCore            = "io.circe"                %% "circe-core"                  % circeVersion
lazy val journalCore          = "io.verizon.journal"      %% "core"                        % journalVersion
lazy val mockito              = "org.mockito"             %% "mockito-scala"               % mockitoVersion
lazy val logbackClassic       = "ch.qos.logback"          % "logback-classic"              % logbackVersion
lazy val monixTail            = "io.monix"                %% "monix-tail"                  % monixVersion
lazy val pureconfig           = "com.github.pureconfig"   %% "pureconfig"                  % pureconfigVersion
lazy val scalaTest            = "org.scalatest"           %% "scalatest"                   % scalaTestVersion
lazy val shapeless            = "com.chuusai"             %% "shapeless"                   % shapelessVersion
lazy val topQuadrantShacl     = "ch.epfl.bluebrain.nexus" %% "shacl-topquadrant-validator" % commonsVersion
lazy val kryo                 = "com.github.romix.akka"   %% "akka-kryo-serialization"     % kryoVersion

lazy val kg = project
  .in(file("."))
  .settings(testSettings, buildInfoSettings)
  .enablePlugins(BuildInfoPlugin, ServicePackagingPlugin)
  .settings(
    name       := "kg",
    moduleName := "kg",
    libraryDependencies ++= Seq(
      adminClient,
      iamClient,
      rdfAkka,
      rdfCore,
      rdfCirce,
      rdfDot,
      rdfJena,
      rdfNexus,
      serviceIndexing,
      sourcingAkka,
      akkaDistributedData,
      akkaHttp,
      akkaHttpCors,
      akkaPersistenceCass,
      akkaStream,
      akkaSlf4j,
      akkaCluster,
      catsCore,
      catsEffect,
      circeCore,
      elasticSearchClient,
      journalCore,
      kryo,
      logbackClassic,
      monixTail,
      pureconfig,
      sparqlClient,
      serviceKamon,
      serviceHttp,
      serviceSerialization,
      topQuadrantShacl,
      akkaHttpTestKit      % Test,
      akkaPersistenceInMem % Test,
      mockito              % Test,
      scalaTest            % Test,
      serviceTest          % Test
    ),
    cleanFiles ++= (baseDirectory.value * "ddata*").get
  )

lazy val testSettings = Seq(
  Test / testOptions       += Tests.Argument(TestFrameworks.ScalaTest, "-o", "-u", "target/test-reports"),
  Test / parallelExecution := false
)

lazy val buildInfoSettings = Seq(
  buildInfoKeys    := Seq[BuildInfoKey](version),
  buildInfoPackage := "ch.epfl.bluebrain.nexus.kg.config"
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
  )
)

addCommandAlias("review", ";clean;scalafmtSbt;test:scalafmtCheck;scalafmtSbtCheck;coverage;scapegoat;test;coverageReport;coverageAggregate")
