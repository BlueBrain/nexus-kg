package ch.epfl.bluebrain.nexus.kg

import java.nio.file.Paths
import java.time.Clock

import akka.actor.{ActorSystem, Address, AddressFromURIString}
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.headers.Location
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.{IamClient, IamUri}
import ch.epfl.bluebrain.nexus.kg.acls.{AclsActor, AclsOps}
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{ElasticConfig, SparqlConfig}
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.Indexing
import ch.epfl.bluebrain.nexus.kg.resolve.ProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.{Repo, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes.HealthStatusGroup
import ch.epfl.bluebrain.nexus.kg.routes.HealthStatus._
import ch.epfl.bluebrain.nexus.kg.routes.{AppInfoRoutes, Clients, CombinedRoutes}
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives.{cors, corsRejectionHandler}
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import com.github.jsonldjava.core.DocumentLoader
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import kamon.Kamon
import kamon.system.SystemMetrics
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.CanBlock
import org.apache.jena.query.ResultSet

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

//noinspection TypeAnnotation
// $COVERAGE-OFF$
object Main {

  def loadConfig(): Config = {
    val cfg = sys.env.get("KG_CONFIG_FILE") orElse sys.props.get("kg.config.file") map { str =>
      val file = Paths.get(str).toAbsolutePath.toFile
      ConfigFactory.parseFile(file)
    } getOrElse ConfigFactory.empty()
    (cfg withFallback ConfigFactory.load()).resolve()
  }

  def setupMonitoring(config: Config): Unit = {
    Kamon.reconfigure(config)
    SystemMetrics.startCollecting()
    Kamon.loadReportersFromConfig()
  }

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def main(args: Array[String]): Unit = {
    val config = loadConfig()
    setupMonitoring(config)

    implicit val appConfig = Settings(config).appConfig

    implicit val as                = ActorSystem(appConfig.description.fullName, config)
    implicit val ec                = as.dispatcher
    implicit val mt                = ActorMaterializer()
    implicit val tm                = Timeout(appConfig.cluster.replicationTimeout)
    implicit val eff: Effect[Task] = Task.catsEffect(Scheduler.global)

    implicit val utClient   = untyped[Task]
    implicit val jsonClient = withUnmarshaller[Task, Json]
    implicit val rsClient   = withUnmarshaller[Task, ResultSet]
    implicit val esDecoders = ElasticDecoder[Json]
    implicit val qrClient   = withUnmarshaller[Task, QueryResults[Json]]

    def clients(implicit elasticConfig: ElasticConfig, sparqlConfig: SparqlConfig): Clients[Task] = {
      val sparql           = BlazegraphClient[Task](sparqlConfig.base, sparqlConfig.defaultIndex, sparqlConfig.akkaCredentials)
      implicit val elastic = ElasticClient[Task](elasticConfig.base)
      implicit val cl      = untyped[Future]

      implicit val adminClient = AdminClient.task(appConfig.admin)
      implicit val iamClient   = IamClient.task()(IamUri(appConfig.iam.baseUri), as)
      Clients(sparql)
    }

    val cluster = Cluster(as)
    val seeds: List[Address] = appConfig.cluster.seeds.toList
      .flatMap(_.split(","))
      .map(addr => AddressFromURIString(s"akka.tcp://${appConfig.description.fullName}@$addr")) match {
      case Nil      => List(cluster.selfAddress)
      case nonEmpty => nonEmpty
    }

    implicit val clock = Clock.systemUTC
    implicit val pm    = CanBlock.permit

    implicit val repo              = Repo[Task].runSyncUnsafe()(Scheduler.global, pm)
    implicit val attConfig         = appConfig.files
    implicit val lc                = FileStore.LocationResolver[Task]()
    implicit val stream            = FileStore.Stream.task(appConfig.files)
    implicit val store             = new FileStore[Task, AkkaIn, AkkaOut]
    implicit val indexers          = clients
    implicit val cache             = DistributedCache.task()
    implicit val iam               = clients.iamClient
    implicit val aclsOps           = new AclsOps(AclsActor.start)
    implicit val projectResolution = ProjectResolution.task(cache, aclsOps)
    val resources: Resources[Task] = Resources[Task]
    val resourceRoutes             = CombinedRoutes(resources)
    val apiRoutes                  = uriPrefix(appConfig.http.publicUri)(resourceRoutes)
    val healthStatusGroup = HealthStatusGroup(
      new CassandraHealthStatus(),
      new ClusterHealthStatus(cluster),
      new IamHealthStatus(iam),
      new AdminHealthStatus(clients.adminClient),
      new ElasticSearchHealthStatus(clients.elastic),
      new SparqlHealthStatus(clients.sparql)
    )
    val appInfoRoutes = AppInfoRoutes(appConfig.description, healthStatusGroup).routes

    val logger = Logging(as, getClass)
    System.setProperty(DocumentLoader.DISALLOW_REMOTE_CONTEXT_LOADING, "true")

    val corsSettings = CorsSettings.defaultSettings
      .withAllowedMethods(List(GET, PUT, POST, DELETE, OPTIONS, HEAD))
      .withExposedHeaders(List(Location.name))

    cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val routes: Route =
        handleRejections(corsRejectionHandler)(cors(corsSettings)(apiRoutes ~ appInfoRoutes))

      val httpBinding = {
        Http().bindAndHandle(routes, appConfig.http.interface, appConfig.http.port)
      }
      httpBinding onComplete {
        case Success(binding) =>
          logger.info(s"Bound to ${binding.localAddress.getHostString}: ${binding.localAddress.getPort}")
        case Failure(th) =>
          logger.error(th, "Failed to perform an http binding on {}:{}", appConfig.http.interface, appConfig.http.port)
          Await.result(as.terminate(), 10 seconds)
      }

      Indexing.start(resources, cache)
    }

    cluster.joinSeedNodes(seeds)

    as.registerOnTermination {
      cluster.leave(cluster.selfAddress)
      Kamon.stopAllReporters()
      SystemMetrics.stopCollecting()
    }
    // attempt to leave the cluster before shutting down
    val _ = sys.addShutdownHook {
      Await.result(as.terminate().map(_ => ()), 10 seconds)
    }
  }
}
// $COVERAGE-ON$
