package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive1, ValidationRejection}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.admin.refined.organization._
import ch.epfl.bluebrain.nexus.admin.refined.project._
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives.CustomAuthRejection
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.ProjectNotFound
import eu.timepit.refined.api.RefType.applyRef
import monix.eval.Task
import monix.execution.Scheduler

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
  def project(implicit client: AdminClient[Task], cred: Option[AuthToken], s: Scheduler): Directive1[Project] = {
    projectReference().flatMap { ref =>
      onSuccess(client.getProject(ref).runAsync).flatMap {
        case Some(project) => provide(project)
        case _             => reject(CustomAuthRejection(ProjectNotFound(ref)))

      }
    }
  }
}
