package ch.epfl.bluebrain.nexus.kg.tests

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.persistence.cassandra.testkit.CassandraLauncher
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.commons.service.persistence.ProjectionStorage
import ch.epfl.bluebrain.nexus.commons.test.Randomness.freePort
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import ch.epfl.bluebrain.nexus.kg.service.routes.MockedIAMClient
import ch.epfl.bluebrain.nexus.kg.service.{BootstrapService, StartSparqlIndexers}
import ch.epfl.bluebrain.nexus.kg.tests.integration._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingAkkaSettings
import com.bigdata.rdf.sail.webapp.NanoSparqlServer
import org.scalatest._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

class BlazegraphServiceSpecSuite
    extends Suites
    with SequentialNestedSuiteExecution
    with BeforeAndAfterAll
    with CassandraBoot
    with BlazegraphBoot
    with MockedIAMClient {

  implicit lazy val system: ActorSystem =
    SystemBuilder.initConfig("BootstrapServices", cassandraPort, blazegraphPort)

  val settings: Settings = new Settings(system.settings.config)

  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val mt: ActorMaterializer        = ActorMaterializer()

  val logger = Logging(system, getClass)

  val pluginId         = "cassandra-query-journal"
  val sourcingSettings = SourcingAkkaSettings(journalPluginId = pluginId)

  val bootstrap = BootstrapService(settings)

  bootstrap.cluster.registerOnMemberUp {
    logger.info("==== Cluster is Live ====")
    StartSparqlIndexers(settings, bootstrap.sparqlClient, bootstrap.contexts, bootstrap.apiUri)
  }

  implicit val instanceResolver = bootstrap.instanceImportResolver
  override val nestedSuites = Vector(
    new BlazegraphOrgIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new BlazegraphDomainIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new BlazegraphContextsIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new BlazegraphSchemasIntegrationSpec(bootstrap.apiUri, settings.Prefixes, bootstrap.routes),
    new BlazegraphInstanceIntegrationSpec(bootstrap.apiUri,
                                          settings.Prefixes,
                                          bootstrap.routes,
                                          bootstrap.instances,
                                          bootstrap.validator)
  )

  override def run(testName: Option[String], args: Args): Status = super.run(testName, args)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    cassandraStart()
    blazegraphStart()
    // ensures the keyspace and tables are created before the tests
    val _ = Await.result(ProjectionStorage(system).fetchLatestOffset("random"), 10 seconds)
    bootstrap.joinCluster()
  }

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    blazegraphStop()
    cassandraStop()
    super.afterAll()
  }
}

trait BlazegraphBoot {

  lazy val blazegraphPort: Int = freePort()

  private val server = {
    System.setProperty("jetty.home", getClass.getResource("/war").toExternalForm)
    NanoSparqlServer.newInstance(blazegraphPort, null, null)
  }

  def blazegraphStart(): Unit = {
    new File("blazegraph.jnl").delete()
    server.start()
  }

  def blazegraphStop(): Unit = {
    server.stop()
    new File("blazegraph.jnl").delete()
    ()
  }
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
