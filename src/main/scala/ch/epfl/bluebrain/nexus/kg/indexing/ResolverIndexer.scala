package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import cats.MonadError
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.cache.{ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressStorage.Volatile
import ch.epfl.bluebrain.nexus.sourcing.projections._
import journal.Logger

private class ResolverIndexerMapping[F[_]](resolvers: Resolvers[F])(
    implicit projectCache: ProjectCache[F],
    adminClient: AdminClient[F],
    projectInitializer: ProjectInitializer[F],
    serviceAccountToken: Option[AuthToken],
    F: MonadError[F, KgError]
) {

  private implicit val log: Logger = Logger[this.type]

  /**
    * Fetches the resolver which corresponds to the argument event. If the resource is not found, or it's not
    * compatible to a resolver the event is dropped silently.
    *
    * @param event event to be mapped to a resolver
    */
  def apply(event: Event): F[Option[Resolver]] =
    fetchProject(event.organization, event.id.parent, event.subject).flatMap { implicit project =>
      resolvers.fetchResolver(event.id).value.map {
        case Left(err) =>
          log.error(s"Error on event '${event.id.show} (rev = ${event.rev})', cause: '${err.msg}'")
          None
        case Right(resolver) => Some(resolver)
      }
    }
}

object ResolverIndexer {

  // $COVERAGE-OFF$
  /**
    * Starts the index process for resolvers across all projects in the system.
    *
    * @param resolvers     the resolvers operations
    * @param resolverCache the distributed cache
    */
  final def start[F[_]: Timer](resolvers: Resolvers[F], resolverCache: ResolverCache[F])(
      implicit
      projectCache: ProjectCache[F],
      as: ActorSystem,
      F: Effect[F],
      projectInitializer: ProjectInitializer[F],
      adminClient: AdminClient[F],
      config: AppConfig
  ): StreamSupervisor[F, ProjectionProgress] = {

    val kgErrorMonadError        = ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
    val indexing: IndexingConfig = config.keyValueStore.indexing

    val mapper = new ResolverIndexerMapping(resolvers)(
      projectCache,
      adminClient,
      projectInitializer,
      config.iam.serviceAccountToken,
      kgErrorMonadError
    )
    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name("resolver-indexer")
        .tag(s"type=${nxv.Resolver.value.show}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[KgError](indexing.retry.retryStrategy)(kgErrorMonadError)
        .batch(indexing.batch, indexing.batchTimeout)
        .offset(Volatile)
        .mapping(mapper.apply)
        .index(_.traverse(resolverCache.put) >> F.unit)
        .build
    )
  }

  /**
    * Starts the index process for resolvers across all projects in the system.
    *
    * @param resolvers     the resolvers operations
    * @param resolverCache the distributed cache
    */
  final def delay[F[_]: Timer: Effect](resolvers: Resolvers[F], resolverCache: ResolverCache[F])(
      implicit
      projectCache: ProjectCache[F],
      projectInitializer: ProjectInitializer[F],
      adminClient: AdminClient[F],
      as: ActorSystem,
      config: AppConfig
  ): F[StreamSupervisor[F, ProjectionProgress]] =
    Effect[F].delay(start(resolvers, resolverCache))

  // $COVERAGE-ON$
}
