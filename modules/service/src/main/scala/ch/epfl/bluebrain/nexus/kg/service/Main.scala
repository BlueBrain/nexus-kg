package ch.epfl.bluebrain.nexus.kg.service

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.kg.core.config.Settings
import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.system.SystemMetrics

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
    val config    = ConfigFactory.load()
    val appConfig = Settings(config).appConfig

    implicit val as = ActorSystem(appConfig.description.actorSystemName, config)
    implicit val ec = as.dispatcher
    implicit val mt = ActorMaterializer()

    val logger = Logging(as, getClass)

    val bootstrap = BootstrapService(appConfig)
    bootstrap.cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val httpBinding = Http().bindAndHandle(bootstrap.routes, appConfig.instance.interface, appConfig.http.port)

      httpBinding.onComplete {
        case Success(binding) =>
          logger.info(s"Bound to ${binding.localAddress.getHostString}: ${binding.localAddress.getPort}")
        case Failure(th) =>
          logger.error(th, "Failed to perform an http binding on {}:{}", appConfig.http.interface, appConfig.http.port)
          Await.result(as.terminate(), 10 seconds)
      }

    }

    bootstrap.joinCluster()
    as.registerOnTermination {
      bootstrap.leaveCluster()
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
