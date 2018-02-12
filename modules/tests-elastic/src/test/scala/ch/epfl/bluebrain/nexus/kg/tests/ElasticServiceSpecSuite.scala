package ch.epfl.bluebrain.nexus.kg.tests

import java.io.File
import java.nio.file.Files
import java.util
import java.util.Arrays.asList

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.model.Uri
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.commons.service.persistence.ProjectionStorage
import ch.epfl.bluebrain.nexus.commons.test.Randomness.freePort
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.routes.MockedIAMClient
import ch.epfl.bluebrain.nexus.kg.service.{BootstrapService, StartElasticIndexers}
import ch.epfl.bluebrain.nexus.kg.tests.integration._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingAkkaSettings
import org.apache.commons.io.FileUtils
import org.elasticsearch.common.settings.{Settings => ElasticSettings}
import org.elasticsearch.node.{InternalSettingsPreparer, Node}
import org.elasticsearch.plugins.Plugin
import org.elasticsearch.transport.Netty4Plugin
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.util.Try

class ElasticServiceSpecSuite
    extends Suites
    with SequentialNestedSuiteExecution
    with BeforeAndAfterAll
    with CassandraBoot
    with ElasticBoot
    with MockedIAMClient {

  implicit lazy val system: ActorSystem =
    SystemBuilder.initConfig("BootstrapServices", cassandraPort, elasticPort)

  val settings: Settings = new Settings(system.settings.config)

  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val mt: ActorMaterializer        = ActorMaterializer()

  val logger = Logging(system, getClass)

  val pluginId         = "cassandra-query-journal"
  val sourcingSettings = SourcingAkkaSettings(journalPluginId = pluginId)

  val bootstrap = BootstrapService(settings)

  bootstrap.cluster.registerOnMemberUp {
    logger.info("==== Cluster is Live ====")
    StartElasticIndexers(settings, bootstrap.elasticClient, bootstrap.contexts, bootstrap.apiUri)
  }

  override val nestedSuites = Vector(
    new ElasticOrgIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new ElasticDomainIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new ElasticContextsIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new ElasticSchemasIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new ElasticInstanceIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes, bootstrap.instances)
  )

  override def run(testName: Option[String], args: Args): Status = super.run(testName, args)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cassandraStart()
    elasticStart()
    // ensures the keyspace and tables are created before the tests
    val _ = Await.result(ProjectionStorage(system).fetchLatestOffset("random"), 10 seconds)
    bootstrap.joinCluster()
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    cassandraStop()
    elasticStop()
    super.afterAll()
  }
}

trait ElasticBoot {

  lazy val elasticPort: Int = freePort()
  lazy val endPort          = elasticPort + 100

  lazy val esUri = Uri(s"http://localhost:$elasticPort")

  private lazy val dataDir = Files.createTempDirectory("elasticsearch_data_").toFile

  private lazy val elasticSettings = ElasticSettings
    .builder()
    .put("path.home", dataDir.toString)
    .put("http.port", s"$elasticPort-$endPort")
    .put("http.enabled", true)
    .put("cluster.name", "elasticsearch")
    .put("http.type", "netty4")
    .build

  private lazy val node =
    new ElasticNode(elasticSettings, asList(classOf[Netty4Plugin]))

  def elasticStart(): Unit = {
    System.setProperty("es.set.netty.runtime.available.processors", "false")
    node.start()
    ()
  }

  def elasticStop(): Unit = {
    node.close()
    Try(FileUtils.forceDelete(dataDir))
    ()
  }

  private class ElasticNode(preparedSettings: ElasticSettings, classpathPlugins: util.Collection[Class[_ <: Plugin]])
      extends Node(InternalSettingsPreparer.prepareEnvironment(preparedSettings, null), classpathPlugins)

}

trait CassandraBoot {

  lazy val cassandraPort: Int = CassandraLauncher.randomPort

  def cassandraStart(): Unit = {
    CassandraLauncher.start(
      new File("target/cassandra"),
      configResource = CassandraLauncher.DefaultTestConfigResource,
      clean = true,
      port = cassandraPort,
      CassandraLauncher.classpathForResources("logback-test.xml")
    )
  }

  def cassandraStop(): Unit = CassandraLauncher.stop()
}
