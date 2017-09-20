package ch.epfl.bluebrain.nexus.kg.service.directives

import akka.http.scaladsl.server.{Directive1, ValidationRejection}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.PathMatchers.Segment
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.service.routes.CommonRejections.IllegalVersionFormat

/**
  * Collection of path specific directives.
  */
trait PathDirectives {

  /**
    * Extracts the ''version'' path parameter and attempts to convert
    * it into a [[Version]]
    */
  def versioned: Directive1[Version] =
    pathPrefix(Segment).flatMap { versionString =>
      Version(versionString) match {
        case None          =>
          reject(new ValidationRejection("Illegal version format", Some(IllegalVersionFormat("Illegal version format"))))
        case Some(version) =>
          provide(version)
      }
    }
}

object PathDirectives extends PathDirectives
