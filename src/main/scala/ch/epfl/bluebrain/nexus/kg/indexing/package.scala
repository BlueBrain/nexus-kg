package ch.epfl.bluebrain.nexus.kg

import akka.NotUsed
import akka.actor.ActorSystem
import akka.persistence.query.scaladsl.EventsByTagQuery
import akka.persistence.query.{NoOffset, Offset, PersistenceQuery}
import akka.stream.scaladsl.Source
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.PersistenceConfig
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectInitializer, ResourceV}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.sourcing.projections.Message
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressFlow.PairMsg

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

  def cassandraSource(tag: String, projectionId: String, offset: Offset = NoOffset)(
      implicit config: PersistenceConfig,
      as: ActorSystem
  ): Source[PairMsg[Any], NotUsed] =
    PersistenceQuery(as)
      .readJournalFor[EventsByTagQuery](config.queryJournalPlugin)
      .eventsByTag(tag, offset)
      .map[PairMsg[Any]](e => Right(Message(e, projectionId)))

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
      F: Effect[F]
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
}
