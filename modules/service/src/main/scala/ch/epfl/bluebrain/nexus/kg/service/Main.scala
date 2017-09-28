package ch.epfl.bluebrain.nexus.kg.service

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import com.typesafe.config.ConfigFactory
import kamon.Kamon

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

//noinspection TypeAnnotation
// $COVERAGE-OFF$
object Main {

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def main(args: Array[String]): Unit = {
    Kamon.start()
    val config = ConfigFactory.load()
    val settings = new Settings(config)

    implicit val as = ActorSystem(settings.Description.ActorSystemName, config)
    implicit val ec = as.dispatcher
    implicit val mt = ActorMaterializer()

    val logger = Logging(as, getClass)

    val bootstrap = BootstrapService(settings)
    bootstrap.cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val httpBinding = {
        Http().bindAndHandle(bootstrap.routes, settings.Http.Interface, settings.Http.Port)
      }

      httpBinding onComplete {
        case Success(binding) =>
          logger.info(s"Bound to ${binding.localAddress.getHostString}: ${binding.localAddress.getPort}")
        case Failure(th)      =>
          logger.error(th, "Failed to perform an http binding on {}:{}", settings.Http.Interface, settings.Http.Port)
          Await.result(as.terminate(), 10 seconds)
      }

      BootstrapIndexing.startIndexing(settings, bootstrap.sparqlClient, bootstrap.apiUri)
    }

    bootstrap.cluster.joinSeedNodes(bootstrap.seeds.toList)
    as.registerOnTermination {
      bootstrap.cluster.leave(bootstrap.cluster.selfAddress)
      Kamon.shutdown()
    }


    // attempt to leave the cluster before shutting down
    val _ = sys.addShutdownHook {
      Await.result(as.terminate().map(_ => ()), 10 seconds)
    }
  }
}
// $COVERAGE-ON$