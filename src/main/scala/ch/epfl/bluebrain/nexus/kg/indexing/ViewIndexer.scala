package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import cats.MonadError
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressStorage.Volatile
import ch.epfl.bluebrain.nexus.sourcing.projections._
import journal.Logger

private class ViewIndexerMapping[F[_]](views: Views[F])(
    implicit projectCache: ProjectCache[F],
    adminClient: AdminClient[F],
    projectInitializer: ProjectInitializer[F],
    serviceAccountToken: Option[AuthToken],
    F: MonadError[F, KgError]
) {

  private implicit val log: Logger = Logger[this.type]

  /**
    * Fetches the view which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a view the event is dropped silently.
    *
    * @param event event to be mapped to a view
    */
  def apply(event: Event): F[Option[View]] =
    fetchProject(event.organization, event.id.parent, event.subject).flatMap { implicit project =>
      views.fetchView(event.id).value.map {
        case Left(err) =>
          log.error(s"Error on event '${event.id.show} (rev = ${event.rev})', cause: '${err.msg}'")
          None
        case Right(view) => Some(view)
      }
    }
}

object ViewIndexer {

  // $COVERAGE-OFF$
  /**
    * Starts the index process for views across all projects in the system.
    *
    * @param views the views operations
    * @param viewCache the distributed cache
    */
  final def start[F[_]: Timer](views: Views[F], viewCache: ViewCache[F])(
      implicit projectCache: ProjectCache[F],
      as: ActorSystem,
      F: Effect[F],
      projectInitializer: ProjectInitializer[F],
      adminClient: AdminClient[F],
      config: AppConfig
  ): StreamSupervisor[F, ProjectionProgress] = {

    val kgErrorMonadError        = ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    val indexing: IndexingConfig = config.keyValueStore.indexing

    val mapper = new ViewIndexerMapping(views)(
      projectCache,
      adminClient,
      projectInitializer,
      config.iam.serviceAccountToken,
      kgErrorMonadError
    )
    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name("view-indexer")
        .tag(s"type=${nxv.View.value.show}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[KgError](indexing.retry.retryStrategy)(kgErrorMonadError)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .mapping(mapper.apply)
        .index(_.traverse(viewCache.put)(F) >> F.unit)
        .build
    )
  }

  /**
    * Starts the index process for views across all projects in the system.
    *
    * @param views     the views operations
    * @param viewCache the distributed cache
    */
  final def delay[F[_]: Timer: Effect](views: Views[F], viewCache: ViewCache[F])(
      implicit projectCache: ProjectCache[F],
      projectInitializer: ProjectInitializer[F],
      adminClient: AdminClient[F],
      as: ActorSystem,
      config: AppConfig
  ): F[StreamSupervisor[F, ProjectionProgress]] =
    Effect[F].delay(start(views, viewCache))
  // $COVERAGE-ON$
}
