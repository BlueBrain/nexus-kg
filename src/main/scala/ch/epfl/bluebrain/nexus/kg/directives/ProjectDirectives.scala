package ch.epfl.bluebrain.nexus.kg.directives

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, Directive1}
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.KgError.{ProjectIsDeprecated, ProjectNotFound}
import ch.epfl.bluebrain.nexus.kg.async.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{Contexts, Schemas}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectLabel
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import monix.eval.Task
import monix.execution.Scheduler

object ProjectDirectives {
  private val defaultPrefixMapping: Map[String, AbsoluteIri] = Map(
    "nxc"       -> Contexts.base,
    "nxs"       -> Schemas.base,
    "resource"  -> Schemas.resourceSchemaUri,
    "schema"    -> Schemas.shaclSchemaUri,
    "view"      -> Schemas.viewSchemaUri,
    "resolver"  -> Schemas.resolverSchemaUri,
    "file"      -> Schemas.fileSchemaUri,
    "nxv"       -> nxv.base,
    "documents" -> nxv.defaultElasticIndex,
    "graph"     -> nxv.defaultSparqlIndex,
    "account"   -> nxv.defaultResolver
  )

  /**
    * Fetches project configuration from the cache if possible, from nexus admin otherwise.
    */
  def project(implicit projectCache: ProjectCache[Task],
              client: AdminClient[Task],
              cred: Option[AuthToken],
              s: Scheduler): Directive1[Project] = {

    pathPrefix(Segment / Segment).tflatMap {
      case (orgLabel, projectLabel) =>
        val label = ProjectLabel(orgLabel, projectLabel)
        val result = projectCache
          .getBy(label)
          .flatMap {
            case value @ Some(_) => Task.pure(value)
            case _               => client.fetchProject(orgLabel, projectLabel)
          }
          .onErrorRecoverWith {
            case _ => client.fetchProject(orgLabel, projectLabel)
          }
        onSuccess(result.runToFuture)
          .flatMap {
            case None          => failWith(ProjectNotFound(label))
            case Some(project) => provide(addDefaultMappings(project))
          }
    }
  }

  private def addDefaultMappings(project: Project) =
    project.copy(apiMappings = project.apiMappings ++ defaultPrefixMapping + ("base" -> project.base))

  /**
    * @return pass when the project is not deprecated, rejects when project is deprecated
    */
  def projectNotDeprecated(implicit proj: Project): Directive0 =
    if (proj.deprecated) failWith(ProjectIsDeprecated(ProjectLabel(proj.organizationLabel, proj.label)))
    else pass
}
