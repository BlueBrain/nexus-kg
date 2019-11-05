package ch.epfl.bluebrain.nexus.kg

import cats.MonadError
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.indexing.View.FilteredView
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectInitializer, ProjectRef, ResourceV}

package object indexing {
  implicit class ListResourcesSyntax[A](private val events: List[(ResourceV, A)]) extends AnyVal {

    /**
      * Remove events with duplicated ''id''. In case of duplication found, the last element is kept and the previous removed.
      *
      * @return a new list without duplicated ids
      */
    def removeDupIds: List[A] =
      events.groupBy { case (res, _) => res.id }.values.flatMap(_.lastOption.map { case (_, elem) => elem }).toList

  }

  /**
    * Attempts to fetch the project from the cache and retries until it is found.
    *
    * @param organizationRef the organization unique reference
    * @param projectRef      the project unique reference
    * @param subject         the subject of the event
    * @tparam F the effect type
    * @return the project wrapped on the effect type
    */
  def fetchProject[F[_]](
      organizationRef: OrganizationRef,
      projectRef: ProjectRef,
      subject: Subject
  )(
      implicit projectCache: ProjectCache[F],
      adminClient: AdminClient[F],
      cred: Option[AuthToken],
      initializer: ProjectInitializer[F],
      F: MonadError[F, KgError]
  ): F[Project] = {

    def initializeOrError(projectOpt: Option[Project]): F[Project] =
      projectOpt match {
        case Some(project) => initializer(project, subject) >> F.pure(project)
        case _             => F.raiseError(KgError.NotFound(Some(projectRef.show)): KgError)
      }

    projectCache
      .get(projectRef)
      .flatMap {
        case Some(project) => F.pure(project)
        case _             => adminClient.fetchProject(organizationRef.id, projectRef.id).flatMap(initializeOrError)
      }
  }

  private[indexing] def validTypes(view: FilteredView, resource: ResourceV): Boolean =
    view.filter.resourceTypes.isEmpty || view.filter.resourceTypes.intersect(resource.types).nonEmpty

  private[indexing] def validSchema(view: FilteredView, resource: ResourceV): Boolean =
    view.filter.resourceSchemas.isEmpty || view.filter.resourceSchemas.contains(resource.schema.iri)
}
