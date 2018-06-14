package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.server.{Directive1, ValidationRejection}
import akka.http.scaladsl.server.Directives._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.admin.refined.organization._
import ch.epfl.bluebrain.nexus.admin.refined.project._
import eu.timepit.refined.api.RefType.applyRef

import scala.concurrent.Future

object ProjectDirectives {

  /**
    * Extracts a [[ProjectReference]] from two consecutive path segments.
    */
  def projectReference(): Directive1[ProjectReference] = {
    pathPrefix(Segment / Segment).tflatMap {
      case (seg1, seg2) =>
        (for {
          org  <- applyRef[OrganizationReference](seg1)
          proj <- applyRef[ProjectLabel](seg2)
        } yield ProjectReference(org, proj)) match {
          case Right(r)  => provide(r)
          case Left(err) => reject(ValidationRejection(err, None))
        }
    }
  }

  /**
    * Fetches project configuration from nexus admin
    */
  def project(implicit adminClient: AdminClient[Future],
              credentials: Option[OAuth2BearerToken]): Directive1[Project] = {
    projectReference().flatMap { ref =>
      onSuccess(adminClient.getProject(ref)).flatMap { project =>
        provide(project)

      }
    }
  }
}
