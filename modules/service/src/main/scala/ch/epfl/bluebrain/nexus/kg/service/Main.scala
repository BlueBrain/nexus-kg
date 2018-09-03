package ch.epfl.bluebrain.nexus.kg.service

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.kg.service.config.Settings
import com.github.jsonldjava.core.DocumentLoader
import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.system.SystemMetrics

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

//noinspection TypeAnnotation
// $COVERAGE-OFF$
object Main {

  @SuppressWarnings(Array("UnusedMethodParameter"))
  def main(args: Array[String]): Unit = {
    SystemMetrics.startCollecting()
    Kamon.loadReportersFromConfig()
    val config   = ConfigFactory.load()
    val settings = new Settings(config)

    implicit val as                            = ActorSystem(settings.Description.ActorSystemName, config)
    implicit val ec                            = as.dispatcher
    implicit val mt                            = ActorMaterializer()
    implicit val cl: UntypedHttpClient[Future] = HttpClient.akkaHttpClient

    val logger = Logging(as, getClass)

    System.setProperty(DocumentLoader.DISALLOW_REMOTE_CONTEXT_LOADING, "true")
    val bootstrap = BootstrapService(settings)
    bootstrap.cluster.registerOnMemberUp {
      logger.info("==== Cluster is Live ====")

      val httpBinding = {
        Http().bindAndHandle(bootstrap.routes, settings.Http.Interface, settings.Http.Port)
      }

      httpBinding onComplete {
        case Success(binding) =>
          logger.info(s"Bound to ${binding.localAddress.getHostString}: ${binding.localAddress.getPort}")
        case Failure(th) =>
          logger.error(th, "Failed to perform an http binding on {}:{}", settings.Http.Interface, settings.Http.Port)
          Await.result(as.terminate(), 10 seconds)
      }

      StartSparqlIndexers(settings, bootstrap.sparqlClient, bootstrap.contexts, bootstrap.apiUri)
      StartElasticIndexers(settings, bootstrap.elasticClient, bootstrap.contexts, bootstrap.apiUri)

      if (settings.Kafka.Enabled) StartKafkaPublishers(settings.Kafka.Topic, settings.Persistence.QueryJournalPlugin)
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
