package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{AuthorizationFailedRejection, Directive0, Directive1}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{Contexts, Schemas}
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives.{authorizationRejection, CustomAuthRejection}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import monix.eval.Task
import monix.execution.Scheduler

import scala.util.{Failure, Success}

object ProjectDirectives {
  private val defaultPrefixMapping: Map[String, AbsoluteIri] = Map(
    "nxv"       -> nxv.base,
    "nxs"       -> Schemas.base,
    "nxc"       -> Contexts.base,
    "resource"  -> Schemas.resourceSchemaUri,
    "documents" -> nxv.defaultElasticIndex,
    "graph"     -> nxv.defaultSparqlIndex
  )

  /**
    * Fetches project configuration from the cache if possible, from nexus admin otherwise.
    */
  def project(implicit cache: DistributedCache[Task],
              client: AdminClient[Task],
              cred: Option[AuthToken],
              s: Scheduler): Directive1[LabeledProject] = {

    pathPrefix(Segment / Segment).tflatMap {
      case (accountLabel, projectLabel) =>
        val label = ProjectLabel(accountLabel, projectLabel)
        val result = cache
          .project(label)
          .flatMap {
            case value @ Some(_) => Task.pure(value)
            case _               => client.getProject(accountLabel, projectLabel)
          }
          .onErrorRecoverWith {
            case _ => client.getProject(accountLabel, projectLabel)
          }
          .flatMap {
            case value @ Some(project) => cache.accountRef(ProjectRef(project.uuid)).map(_ -> value)
            case _                     => Task.pure(None                                   -> None)
          }
          .flatMap {
            case (None, proj @ Some(_)) => client.getAccount(accountLabel).map(_.map(ac => AccountRef(ac.uuid)) -> proj)
            case o                      => Task.pure(o)
          }
        onComplete(result.runAsync)
          .flatMap {
            case Failure(UnauthorizedAccess)       => reject(AuthorizationFailedRejection)
            case Failure(err)                      => reject(authorizationRejection(err))
            case Success((_, None))                => reject(CustomAuthRejection(ProjectsNotFound(Set(label))))
            case Success((None, Some(value)))      => reject(CustomAuthRejection(AccountNotFound(ProjectRef(value.uuid))))
            case Success((Some(ref), Some(value))) => provide(LabeledProject(label, addDefaultMappings(value), ref))
          }
    }
  }

  private def addDefaultMappings(project: Project) =
    project.copy(prefixMappings = project.prefixMappings ++ defaultPrefixMapping + ("base" -> project.base))

  /**
    * @return pass when the project is not deprecated, rejects when project is deprecated
    */
  def projectNotDeprecated(implicit proj: Project, ref: ProjectLabel): Directive0 =
    if (proj.deprecated) reject(CustomAuthRejection(ProjectIsDeprecated(ref)))
    else pass
}
