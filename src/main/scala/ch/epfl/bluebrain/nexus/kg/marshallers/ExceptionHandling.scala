package ch.epfl.bluebrain.nexus.kg.marshallers

import akka.http.scaladsl.server.Directives.complete
import akka.http.scaladsl.server.ExceptionHandler
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.Unexpected
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
      case err =>
        logger.error("Exception caught during routes processing ", err)
        val msg = Try(err.getMessage).filter(_ != null).getOrElse("Something went wrong. Please, try again later.")
        complete(Unexpected(msg))
    }

}
