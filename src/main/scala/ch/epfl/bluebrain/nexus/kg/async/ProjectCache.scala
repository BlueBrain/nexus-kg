package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.ActorSystem
import cats.Monad
import cats.effect.{Async, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.async.Cache.storeWrappedError
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.service.indexer.cache.{KeyValueStore, KeyValueStoreConfig}

/**
  * The project cache backed by a KeyValueStore using akka Distributed Data
  *
  * @param store the underlying Distributed Data LWWMap store.
  */
class ProjectCache[F[_]] private (store: KeyValueStore[F, UUID, Project])(implicit F: Monad[F])
    extends Cache[F, Project](store) {

  private implicit val ordering: Ordering[Project] = Ordering.by { proj =>
    s"${proj.organizationLabel}/${proj.label}"
  }

  /**
    * Attempts to fetch the project resource with the provided ''label''
    *
    * @param label the organization and project labels
    */
  def getBy(label: ProjectLabel): F[Option[Project]] =
    store.findValue(p => ProjectLabel(p.organizationLabel, p.label) == label)

  /**
    * Attempts to fetch the project with the provided ''ref''
    *
    * @param ref the project unique reference
    */
  def get(ref: ProjectRef): F[Option[Project]] = super.get(ref.id)

  /**
    * Attempts to fetch the project label with the provided ''ref''
    *
    * @param ref the project unique reference
    */
  def getLabel(ref: ProjectRef): F[Option[ProjectLabel]] =
    get(ref.id).map(_.map(proj => ProjectLabel(proj.organizationLabel, proj.label)))

  /**
    * Attempts to convert the set of ''ProjectRef'' to ''ProjectLabel'' looking up at each ref.
    *
    * @param refs the set of ''ProjectRef''
    */
  def getProjectLabels(refs: Set[ProjectRef]): F[Map[ProjectRef, Option[ProjectLabel]]] =
    refs.map(ref => getLabel(ref).map(ref -> _)).toList.sequence.map(_.toMap)

  /**
    * Attempts to convert the set of ''ProjectLabel'' to ''ProjectRef'' looking up at each label.
    *
    * @param labels the set of ''ProjectLabel''
    */
  def getProjectRefs(labels: Set[ProjectLabel]): F[Map[ProjectLabel, Option[ProjectRef]]] =
    labels.map(label => getBy(label).map(label -> _.map(_.ref))).toList.sequence.map(_.toMap)

  /**
    * Fetches all the projects
    */
  def list(): F[List[Project]] = store.values.map(_.toList.sorted)

  /**
    * Fetches all the projects that belong to the provided organization
    *
    * @param organizationRef the organization to filter the projects
    */
  def list(organizationRef: OrganizationRef): F[List[Project]] =
    store.values.map(_.filter(_.organizationUuid == organizationRef.id).toList.sorted)

  /**
    * Creates or replaces the project with key provided uuid and value.
    *
    * @param value the project value
    */
  def replace(value: Project): F[Unit] = super.replace(value.uuid, value)

  /**
    * Deprecates the project with the provided ref
    *
    * @param ref the project unique reference
    * @param rev the project new revision
    */
  def deprecate(ref: ProjectRef, rev: Long): F[Unit] =
    store.computeIfPresent(ref.id, c => c.copy(rev = rev, deprecated = true)) *> F.unit

}

object ProjectCache {

  /**
    * Creates a new project index.
    */
  def apply[F[_]: Timer](implicit as: ActorSystem, config: KeyValueStoreConfig, F: Async[F]): ProjectCache[F] =
    new ProjectCache[F](storeWrappedError[F, Project]("projects", _.rev))
}
