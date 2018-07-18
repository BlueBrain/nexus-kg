package ch.epfl.bluebrain.nexus.kg

import java.time.Clock

import akka.actor.{ActorSystem, Address, AddressFromURIString}
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.util.Timeout
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.{IamClient, IamUri}
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.{ElasticConfig, SparqlConfig}
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.Indexing
import ch.epfl.bluebrain.nexus.kg.persistence.TaskAggregate
import ch.epfl.bluebrain.nexus.kg.resolve.ProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources.Repo.Agg
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.resources.{Repo, Resources}
import ch.epfl.bluebrain.nexus.kg.routes.{Clients, ResourceRoutes, ServiceDescriptionRoutes}
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives._
import ch.epfl.bluebrain.nexus.sourcing.akka.{ShardingAggregate, SourcingAkkaSettings}
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.generic.auto._
import kamon.Kamon
import kamon.system.SystemMetrics
import monix.eval.Task
import org.apache.jena.query.ResultSet

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

//noinspection TypeAnnotation
// $COVERAGE-OFF$
object Main {

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def main(args: Array[String]): Unit = {
    SystemMetrics.startCollecting()
    Kamon.loadReportersFromConfig()
    val config             = ConfigFactory.load()
    implicit val appConfig = Settings(config).appConfig

    implicit val as = ActorSystem(appConfig.description.fullName, config)
    implicit val ec = as.dispatcher
    implicit val mt = ActorMaterializer()
    implicit val tm = Timeout(appConfig.cluster.replicationTimeout)

    implicit val utClient   = HttpClient.taskHttpClient
    implicit val jsonClient = HttpClient.withTaskUnmarshaller[Json]
    implicit val rsClient   = HttpClient.withTaskUnmarshaller[ResultSet]
    implicit val qrClient   = HttpClient.withTaskUnmarshaller[QueryResults[Json]]

    def clients(implicit elasticConfig: ElasticConfig, sparqlConfig: SparqlConfig): Clients[Task] = {
      val sparql           = BlazegraphClient[Task](sparqlConfig.base, sparqlConfig.defaultIndex, sparqlConfig.akkaCredentials)
      implicit val elastic = ElasticClient[Task](elasticConfig.base)
      implicit val cl      = HttpClient.akkaHttpClient

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

    val sourcingSettings = SourcingAkkaSettings(journalPluginId = appConfig.persistence.queryJournalPlugin)

    val resourceAggregate: Agg[Task] =
      TaskAggregate.fromFuture(ShardingAggregate("resources", sourcingSettings)(Repo.initial, Repo.next, Repo.eval))
    implicit val repo              = Repo(resourceAggregate, clock)
    implicit val attConfig         = appConfig.attachments
    implicit val lc                = AttachmentStore.LocationResolver[Task]()
    implicit val stream            = AttachmentStore.Stream.task(appConfig.attachments)
    implicit val store             = new AttachmentStore[Task, AkkaIn, AkkaOut]
    implicit val indexers          = clients
    implicit val cache             = DistributedCache.task()
    implicit val projectResolution = ProjectResolution.task(cache)
    val resources: Resources[Task] = Resources[Task]
    val resourceRoutes             = ResourceRoutes(resources).routes
    val apiRoutes                  = uriPrefix(appConfig.http.publicUri)(resourceRoutes)
    val serviceDesc                = ServiceDescriptionRoutes(appConfig.description).routes

    val logger = Logging(as, getClass)

    cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val httpBinding = {
        Http().bindAndHandle(apiRoutes ~ serviceDesc, appConfig.http.interface, appConfig.http.port)
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
