package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.{ask, AskTimeoutException}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import cats.effect.{Async, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{untyped, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.cache.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.Statistics
import ch.epfl.bluebrain.nexus.kg.indexing.View.SingleView
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, OrganizationRef, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.sourcing.projections.Projections
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

/**
  * ProjectViewCoordinator backed by [[ProjectViewCoordinatorActor]] that sends messages to the underlying actor
  *
  * @param cache the cache
  * @param ref   the underlying actor reference
  * @tparam F the effect type
  */
class ProjectViewCoordinator[F[_]](cache: Caches[F], ref: ActorRef)(
    implicit config: AppConfig,
    F: Async[F],
    ec: ExecutionContext
) {

  private implicit val timeout: Timeout = config.sourcing.askTimeout
  private val log                       = Logger[this.type]
  private implicit val contextShift     = IO.contextShift(ec)

  /**
    * Fetches view statistics for a given view.
    *
    * @param project  project to which the view belongs.
    * @param view     the view to fetch the statistics for.
    * @return [[Statistics]] wrapped in [[F]]
    */
  def statistics(project: Project, view: SingleView): F[Statistics] = {
    lazy val label = project.projectLabel.show
    IO.fromFuture(IO(ref ? FetchProgress(project.uuid, view)))
      .to[F]
      .flatMap[Statistics] {
        case s: Statistics => F.pure(s)
        case other =>
          val msg = s"Received unexpected reply from the project view coordinator actor: '$other' for project '$label'."
          log.error(msg)
          F.raiseError(KgError.InternalError(msg))
      }
      .recoverWith {
        case _: AskTimeoutException =>
          F.raiseError(
            KgError
              .OperationTimedOut(s"Timeout when asking for statistics to project view coordinator for project '$label'")
          )
        case NonFatal(th) =>
          val msg = s"Exception caught while exchanging messages with the project view coordinator for project '$label'"
          log.error(msg, th)
          F.raiseError(KgError.InternalError(msg))
      }
  }

  /**
    * Starts the project view coordinator for the provided project sending a Start message to the
    * underlying coordinator actor.
    * The coordinator actor will attempt to fetch the views linked to the current project and start them
    * while start listening to messages coming from the view cache and the coordinator itself
    *
    * @param project the project for which the view coordinator is triggered
    */
  def start(project: Project): F[Unit] =
    cache.view.getBy[SingleView](project.ref).map { views =>
      ref ! Start(project.uuid, project, views)
    }

  /**
    * Stops the coordinator children views actors and indices related to all the projects
    * that belong to the provided organization.
    *
    * @param orgRef the organization unique identifier
    */
  def stop(orgRef: OrganizationRef): F[Unit] =
    cache.project.list(orgRef).flatMap(projects => projects.map(project => stop(project.ref)).sequence) *> F.unit

  /**
    * Stops the coordinator children view actors and indices that belong to the provided organization.
    *
    * @param projectRef the project unique identifier
    */
  def stop(projectRef: ProjectRef): F[Unit] = {
    ref ! Stop(projectRef.id)
    F.unit
  }

  /**
    * Notifies the underlying coordinator actor about a change ocurring to the Project
    * whenever this change is relevant to the coordinator
    *
    * @param newProject the new uncoming project
    * @param project    the previous state of the project
    */
  def change(newProject: Project, project: Project): F[Unit] = {
    if (newProject.label != project.label || newProject.organizationLabel != project.organizationLabel)
      ref ! ProjectChanges(newProject.uuid, newProject)
    F.unit
  }
}

object ProjectViewCoordinator {
  def apply(resources: Resources[Task], cache: Caches[Task])(
      implicit config: AppConfig,
      as: ActorSystem,
      ucl: HttpClient[Task, SparqlResults],
      P: Projections[Task, Event]
  ): ProjectViewCoordinator[Task] = {
    implicit val mt: ActorMaterializer                          = ActorMaterializer()
    implicit val ul: UntypedHttpClient[Task]                    = untyped[Task]
    implicit val elasticSearchClient: ElasticSearchClient[Task] = ElasticSearchClient[Task](config.elasticSearch.base)
    val coordinatorRef                                          = ProjectViewCoordinatorActor.start(resources, cache.view, None, config.cluster.shards)
    new ProjectViewCoordinator[Task](cache, coordinatorRef)
  }
}
