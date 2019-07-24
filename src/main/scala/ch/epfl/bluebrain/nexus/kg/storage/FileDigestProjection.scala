package ch.epfl.bluebrain.nexus.kg.storage

import akka.actor.ActorSystem
import cats.MonadError
import cats.effect.{Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError.InternalError
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.instances.kgErrorMonadError
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.FetchDigest
import ch.epfl.bluebrain.nexus.sourcing.projections._
import kamon.Kamon
import monix.execution.atomic.AtomicLong

private class FileDigestProjectionMapping[F[_]: FetchDigest](files: Files[F])(implicit F: MonadError[F, KgError]) {

  /**
    * When an event is received, a file digest is attempted to be calculated if the file does not currently have a digest.
    *
    * @param event event to be mapped to a Elastic Search insert query
    */
  final def apply(event: Event): F[Option[Resource]] = {
    implicit val subject = event.subject
    files.updateDigestIfEmpty(event.id).value.flatMap[Option[Resource]] {
      case Left(FileDigestNotComputed(_)) =>
        F.raiseError(InternalError(s"Resource '${event.id.ref.show}' does not have a computed digest."): KgError)
      case Left(UnexpectedState(_)) =>
        F.raiseError(InternalError(s"Storage for resource '${event.id.ref.show}' is not still on the cache."): KgError)
      case Left(_) =>
        F.pure(None)
      case Right(resource) =>
        F.pure(Some(resource))
    }
  }
}

object FileDigestProjection {

  // $COVERAGE-OFF$
  /**
    * Starts the projection process to compute the digest of the missing files
    *
    * @param files         the files bundle operations
    * @param project       the project to which the resource belongs
    */
  final def start[F[_]: Timer](
      files: Files[F],
      project: Project,
      restartOffset: Boolean
  )(implicit fetchDigest: FetchDigest[F],
    P: Projections[F, Event],
    as: ActorSystem,
    config: AppConfig,
    F: Effect[F]): StreamSupervisor[F, ProjectionProgress] = {

    val mapper = new FileDigestProjectionMapping(files)(fetchDigest, kgErrorMonadError)

    val ignoreIndex: List[Resource] => F[Unit] = _ => F.unit

    val processedEventsGauge = Kamon
      .gauge("digest_computation_gauge")
      .withTag("type", "digest")
      .withTag("project", project.projectLabel.show)
      .withTag("organization", project.organizationLabel)
    val processedEventsCounter = Kamon
      .counter("digest_computation_counter")
      .withTag("type", "digest")
      .withTag("project", project.projectLabel.show)
      .withTag("organization", project.organizationLabel)
    val processedEventsCount = AtomicLong(0L)

    TagProjection.start(
      ProjectionConfig
        .builder[F]
        .name(s"digest-computation-${project.uuid}")
        .tag(s"project=${project.uuid}")
        .plugin(config.persistence.queryJournalPlugin)
        .retry[KgError](config.storage.digestRetry.retryStrategy)(kgErrorMonadError)
        .restart(restartOffset)
        .mapping(mapper.apply)
        .index(ignoreIndex)
        .mapInitialProgress { p =>
          processedEventsCount.set(p.processedCount)
          processedEventsGauge.update(p.processedCount.toDouble)
          F.unit
        }
        .mapProgress { p =>
          val previousCount = processedEventsCount.get()
          processedEventsGauge.update(p.processedCount.toDouble)
          processedEventsCounter.increment(p.processedCount - previousCount)
          processedEventsCount.set(p.processedCount)
          F.unit
        }
        .build)
  }
}
