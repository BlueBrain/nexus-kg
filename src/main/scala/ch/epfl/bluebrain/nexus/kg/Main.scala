package ch.epfl.bluebrain.nexus.kg

import java.nio.file.Paths
import java.time.Clock

import akka.actor.{ActorSystem, Address, AddressFromURIString}
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.effect.Effect
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticSearchClient, ElasticSearchDecoder}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.indexing.Indexing
import ch.epfl.bluebrain.nexus.kg.resolve.{Materializer, ProjectResolution}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.routes.{Clients, Routes}
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import com.github.jsonldjava.core.DocumentLoader
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import kamon.Kamon
import kamon.bundle.Bundle
import monix.eval.Task
import monix.execution.Scheduler
import monix.execution.schedulers.CanBlock

import scala.concurrent.Await
import scala.concurrent.duration._
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
    Bundle.attach()
    Kamon.reconfigure(config)
    Kamon.loadModules()
  }

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def main(args: Array[String]): Unit = {
    val config = loadConfig()
    setupMonitoring(config)

    implicit val appConfig = Settings(config).appConfig

    implicit val as                = ActorSystem(appConfig.description.fullName, config)
    implicit val ec                = as.dispatcher
    implicit val mt                = ActorMaterializer()
    implicit val eff: Effect[Task] = Task.catsEffect(Scheduler.global)

    implicit val utClient            = untyped[Task]
    implicit val jsonClient          = withUnmarshaller[Task, Json]
    implicit val sparqlResultsClient = withUnmarshaller[Task, SparqlResults]
    implicit val esDecoders          = ElasticSearchDecoder[Json]
    implicit val qrClient            = withUnmarshaller[Task, QueryResults[Json]]

    def clients(implicit elasticSearchConfig: ElasticSearchConfig, sparqlConfig: SparqlConfig): Clients[Task] = {
      val sparql                 = BlazegraphClient[Task](sparqlConfig.base, sparqlConfig.defaultIndex, sparqlConfig.akkaCredentials)
      implicit val elasticSearch = ElasticSearchClient[Task](elasticSearchConfig.base)

      implicit val adminClient  = AdminClient[Task](appConfig.admin)
      implicit val iamClient    = IamClient[Task]
      implicit val sparqlClient = sparql
      Clients()
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

    implicit val repo     = Repo[Task].runSyncUnsafe()(Scheduler.global, pm)
    implicit val indexers = clients
    implicit val cache =
      Caches(ProjectCache[Task], ViewCache[Task], ResolverCache[Task], StorageCache[Task])
    implicit val aclCache                         = AclsCache[Task](clients.iamClient)
    implicit val projectResolution                = ProjectResolution.task(repo, cache.resolver, cache.project, aclCache)
    implicit val materializer: Materializer[Task] = new Materializer[Task](projectResolution, cache.project)
    implicit val projectCache                     = cache.project
    implicit val viewCache                        = cache.view
    implicit val storageCache                     = cache.storage
    import indexers.elasticSearch

    val resources: Resources[Task] = Resources[Task]
    val storages: Storages[Task]   = Storages[Task]
    val files: Files[Task]         = Files[Task]
    val views: Views[Task]         = Views[Task]
    val resolvers: Resolvers[Task] = Resolvers[Task]
    val schemas: Schemas[Task]     = Schemas[Task]
    val tags: Tags[Task]           = Tags[Task]

    implicit val projections: Projections[Task, Event] = {
      import ch.epfl.bluebrain.nexus.kg.serializers.Serializer._
      Projections[Task, Event].runSyncUnsafe(10 seconds)(Scheduler.global, CanBlock.permit)
    }

    val logger = Logging(as, getClass)
    System.setProperty(DocumentLoader.DISALLOW_REMOTE_CONTEXT_LOADING, "true")

    cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      if (sys.env.getOrElse("MIGRATE_V10_TO_V11", "false").toBoolean) {
        Migrations.V1ToV11.migrate(appConfig)(as, mt, Scheduler.global, CanBlock.permit)
        RepairFromMessages.repair(repo)(as, mt, Scheduler.global, CanBlock.permit)
      }

      if (sys.env.getOrElse("REPAIR_FROM_MESSAGES", "false").toBoolean) {
        RepairFromMessages.repair(repo)(as, mt, Scheduler.global, CanBlock.permit)
      }

      val projectCoordinator = Indexing.start(resources, storages, views, resolvers, indexers.adminClient)
      val routes: Route      = Routes(resources, resolvers, views, storages, schemas, files, tags, projectCoordinator)

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
    }

    cluster.joinSeedNodes(seeds)

    as.registerOnTermination {
      cluster.leave(cluster.selfAddress)
      Await.result(Kamon.stopModules(), 10 seconds)
    }
    // attempt to leave the cluster before shutting down
    val _ = sys.addShutdownHook {
      Await.result(as.terminate().map(_ => ()), 10 seconds)
    }
  }
}
// $COVERAGE-ON$
