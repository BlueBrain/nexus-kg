package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.Timeout
import cats.effect.{Async, Effect, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Subject
import ch.epfl.bluebrain.nexus.kg.async.ProjectAttributesCoordinator._
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ArchivesConfig
import ch.epfl.bluebrain.nexus.kg.indexing.{cassandraSource, Statistics}
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Files, OrganizationRef, Rejection, Resource}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.FileDigestAlreadyExists
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.FetchAttributes
import ch.epfl.bluebrain.nexus.sourcing.StateMachine
import ch.epfl.bluebrain.nexus.sourcing.akka.StopStrategy
import ch.epfl.bluebrain.nexus.sourcing.akka.statemachine.AkkaStateMachine
import ch.epfl.bluebrain.nexus.sourcing.projections.ProgressFlow.ProgressFlowElem
import ch.epfl.bluebrain.nexus.sourcing.projections.{IndexingConfig, ProjectionProgress, Projections, StreamSupervisor}
import ch.epfl.bluebrain.nexus.sourcing.syntax._
import com.typesafe.scalalogging.Logger
import retry.{RetryDetails, RetryPolicy}
import retry.CatsEffect._
import retry._
import retry.syntax.all._

import scala.concurrent.ExecutionContext

/**
  * Coordinates the [[ProgressStreamSupervisor]] for each project
  */
class ProjectAttributesCoordinator[F[_]](ref: StateMachine[F, String, State[F], Command, Unit])(
    implicit projectCache: ProjectCache[F],
    F: Async[F]
) {

  /**
    * Fetches the statistics of the running file digest attributes computation for the passed project.
    */
  def statistics(project: Project): F[Statistics] =
    ref.currentState(project.uuid.toString).flatMap {
      case Some((_, supervisor)) =>
        supervisor.value.state().map(_.fold(Statistics.empty)(statistics(_, supervisor.progressId)))
      case None =>
        F.pure(Statistics.empty)
    }

  /**
    * Creates a [[ProgressStreamSupervisor]] that will call to compute the missing file attributes (digest and size).
    */
  def start(project: Project): F[Unit] =
    ref.evaluate(project.uuid.toString, Start(project)) >> F.unit

  /**
    * Stops the [[ProgressStreamSupervisor]] for all the projects that belong to the passed organization.
    */
  def stop(orgRef: OrganizationRef): F[Unit] =
    projectCache.list(orgRef).flatMap(_.map(project => stop(project.ref)).sequence) >> F.unit

  /**
    * Stops the [[ProgressStreamSupervisor]] for the passed project.
    */
  def stop(projectRef: ProjectRef): F[Unit] =
    ref.evaluate(projectRef.id.toString, Stop) >> F.unit

  private def statistics(progress: ProjectionProgress, progressId: String): Statistics = {
    val p = progress.progress(progressId)
    Statistics(p.processed, p.discarded, p.failed, p.processed, p.offset.asInstant, p.offset.asInstant)
  }
}

object ProjectAttributesCoordinator {

  private val log: Logger = Logger[ProjectAttributesCoordinator.type]

  private[async] type State[F[_]] = Option[(Project, ProgressStreamSupervisor[F])]
  private[async] type Command     = Protocol

  private[async] sealed trait Protocol                    extends Product with Serializable
  private[async] final case class Start(project: Project) extends Protocol
  private[async] final case object Stop                   extends Protocol

  trait SupervisorFactory[F[_]] {
    def apply(project: Project): ProgressStreamSupervisor[F]
  }

  object SupervisorFactory {
    // $COVERAGE-OFF$
    final def apply[F[_]: Timer](files: Files[F])(
        implicit
        config: AppConfig,
        fetchDigest: FetchAttributes[F],
        F: Effect[F],
        as: ActorSystem,
        projections: Projections[F, String]
    ): SupervisorFactory[F] = new SupervisorFactory[F] {
      private implicit val ec: ExecutionContext     = as.dispatcher
      private implicit val indexing: IndexingConfig = config.storage.indexing
      private implicit val policy: RetryPolicy[F]   = config.storage.fileAttrRetry.retryPolicy[F]
      private implicit val tm: Timeout              = Timeout(config.storage.askTimeout)
      private implicit val logErrors: (Either[Rejection, Resource], RetryDetails) => F[Unit] =
        (err, d) => F.pure(log.warn("Retrying on resource creation with retry details '{}' and error: '{}'", err, d))

      def apply(project: Project): ProgressStreamSupervisor[F] = {
        val progressId = s"attributes-computation-${project.uuid}"
        val sourceF: F[Source[ProjectionProgress, _]] = projections.progress(progressId).map { initial =>
          val flow = ProgressFlowElem[F, Any]
            .collectCast[Event]
            .collect {
              case ev: Event.FileCreated => ev: Event
              case ev: Event.FileUpdated => ev: Event
            }
            .mapAsync { event =>
              implicit val subject: Subject = event.subject
              val update                    = files.updateFileAttrEmpty(event.id).value
              update.retryingM(v => v.isRight || v.left.exists(_.isInstanceOf[FileDigestAlreadyExists])).map(_.toOption)
            }
            .collectSome[Resource]
            .toPersistedProgress(progressId, initial)

          cassandraSource(s"project=${project.uuid}", progressId, initial.minProgress.offset).via(flow)
        }
        ProgressStreamSupervisor(progressId, StreamSupervisor.start(sourceF, progressId))
      }
    }
    // $COVERAGE-ON$
  }

  final def apply[F[_]: Timer](supervisorFactory: SupervisorFactory[F])(
      implicit as: ActorSystem,
      projectCache: ProjectCache[F],
      cfg: ArchivesConfig,
      F: Effect[F]
  ): F[ProjectAttributesCoordinator[F]] = {
    implicit val retryPolicy: RetryPolicy[F] = cfg.cache.retry.retryPolicy[F]
    val neverStop                            = StopStrategy.never[State[F], Command]

    val evaluate: (State[F], Command) => F[State[F]] = {
      case (None, Start(project))        => F.pure(Some(project -> supervisorFactory(project)))
      case (Some((_, supervisor)), Stop) => supervisor.value.stop() >> F.pure(None)
      case (st, _)                       => F.pure(st)
    }

    AkkaStateMachine
      .sharded[F]("attr-coord", None, evaluate.toEither, neverStop, cfg.cache.akkaStateMachineConfig, cfg.cache.shards)
      .map(new ProjectAttributesCoordinator[F](_))
  }
}
