package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{DownstreamServiceError, Unexpected}
import io.circe.parser.parse
import journal.Logger

import scala.util.Try

/**
  * It provides an exception handler implementation that ensures
  * all unexpected failures are gracefully handled
  * and presented to the caller.
  */
object ExceptionHandling {

  private val logger = Logger[this.type]

  /**
    * @return an ExceptionHandler that ensures a descriptive message is returned to the caller
    */
  final def apply(): ExceptionHandler =
    ExceptionHandler {
      case rej: Rejection => complete(rej)
      case ElasticClientError(status, body) =>
        parse(body) match {
          case Right(json) => complete(status -> json)
          case Left(_)     => complete(status -> body)
        }
      case f: ElasticFailure =>
        logger.error(s"Received unexpected response from ES: '${f.message}' with body: '${f.body}'")
        complete(DownstreamServiceError("Error communicating with ElasticSearch"))
      case err =>
        logger.error("Exception caught during routes processing ", err)
        val msg = Try(err.getMessage).filter(_ != null).getOrElse("Something went wrong. Please, try again later.")
        complete(Unexpected(msg))
    }

}
