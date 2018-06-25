package ch.epfl.bluebrain.nexus.kg

import java.time.Clock

import akka.actor.{ActorSystem, Address, AddressFromURIString}
import akka.cluster.Cluster
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.iam.client.{IamClient, IamUri}
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.persistence.TaskAggregate
import ch.epfl.bluebrain.nexus.kg.resources.Repo
import ch.epfl.bluebrain.nexus.kg.resources.Repo.Agg
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes
import ch.epfl.bluebrain.nexus.service.http.directives.PrefixDirectives._
import ch.epfl.bluebrain.nexus.sourcing.akka.{ShardingAggregate, SourcingAkkaSettings}
import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.system.SystemMetrics
import monix.eval.Task

import scala.concurrent.duration._
import scala.concurrent.Await
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

    implicit val as = ActorSystem(appConfig.description.name, config)
    implicit val ec = as.dispatcher
    implicit val mt = ActorMaterializer()

    val cluster = Cluster(as)
    val seeds: List[Address] = appConfig.cluster.seeds.toList
      .flatMap(_.split(","))
      .map(addr => AddressFromURIString(s"akka.tcp://${appConfig.description.version}@$addr")) match {
      case Nil      => List(cluster.selfAddress)
      case nonEmpty => nonEmpty
    }

    implicit val cl    = akkaHttpClient
    implicit val clock = Clock.systemUTC

    val sourcingSettings     = SourcingAkkaSettings(journalPluginId = appConfig.persistence.queryJournalPlugin)
    implicit val adminClient = AdminClient(appConfig.admin)
    implicit val iamClient   = IamClient()(IamUri(appConfig.iam.baseUri), as)

    val resourceAggregate: Agg[Task] =
      TaskAggregate.fromFuture(ShardingAggregate("resources", sourcingSettings)(Repo.initial, Repo.next, Repo.eval))
    implicit val repo      = Repo(resourceAggregate, clock)
    implicit val attConfig = appConfig.attachments
    implicit val lc        = AttachmentStore.LocationResolver[Task]()
    implicit val stream    = AttachmentStore.Stream.task(appConfig.attachments)
    implicit val store     = new AttachmentStore[Task, AkkaIn, AkkaOut]
    val resourceRoutes     = ResourceRoutes().routes
    val apiRoutes          = uriPrefix(appConfig.http.publicUri)(resourceRoutes)

    val logger = Logging(as, getClass)

    cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val httpBinding = {
        Http().bindAndHandle(apiRoutes, appConfig.http.interface, appConfig.http.port)
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
