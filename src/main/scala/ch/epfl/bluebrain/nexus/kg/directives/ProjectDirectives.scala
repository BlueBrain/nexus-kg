package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive0, Directive1}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.async.Projects
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives.{authorizationRejection, CustomAuthRejection}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectLabel
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{ProjectIsDeprecated, ProjectNotFound}
import monix.eval.Task
import monix.execution.Scheduler

import scala.util.{Failure, Success}

object ProjectDirectives {

  /**
    * Fetches project configuration from the cache if possible, from nexus admin otherwise.
    */
  def project(implicit projects: Projects[Task],
              client: AdminClient[Task],
              cred: Option[AuthToken],
              s: Scheduler): Directive1[LabeledProject] =
    pathPrefix(Segment / Segment).tflatMap {
      case (accountLabel, projectLabel) =>
        val label = ProjectLabel(accountLabel, projectLabel)
        val result = projects.project(label).flatMap {
          case value @ Some(_) => Task.pure(value)
          case _               => client.getProject(accountLabel, projectLabel)
        }
        onComplete(result.onErrorRecoverWith { case _ => client.getProject(accountLabel, projectLabel) }.runAsync)
          .flatMap {
            case Failure(UnauthorizedAccess) => reject(AuthorizationFailedRejection)
            case Failure(err)                => reject(authorizationRejection(err))
            case Success(None)               => reject(CustomAuthRejection(ProjectNotFound(label)))
            case Success(Some(value))        => provide(LabeledProject(label, value))
          }
    }

  /**
    * @return pass when the project is not deprecated, rejects when project is deprecated
    */
  def projectNotDeprecated(implicit proj: Project, ref: ProjectLabel): Directive0 =
    if (proj.deprecated) reject(CustomAuthRejection(ProjectIsDeprecated(ref)))
    else pass
}
