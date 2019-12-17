package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.ActorSystem
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types._
import ch.epfl.bluebrain.nexus.admin.client.types.events.Event._
import ch.epfl.bluebrain.nexus.admin.client.types.events.{Event => AdminEvent}
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.cache.Caches
import ch.epfl.bluebrain.nexus.kg.cache.Caches._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.resources._
import com.typesafe.scalalogging.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

// $COVERAGE-OFF$
private class Indexing(
    storages: Storages[Task],
    views: Views[Task],
    resolvers: Resolvers[Task],
    viewCoordinator: ProjectViewCoordinator[Task],
    fileAttributesCoordinator: ProjectAttributesCoordinator[Task]
)(
    implicit cache: Caches[Task],
    adminClient: AdminClient[Task],
    projectInitializer: ProjectInitializer[Task],
    as: ActorSystem,
    config: AppConfig
) {

  private val logger = Logger[this.type]

  def startAdminStream(): Unit = {

    def handle(event: AdminEvent): Task[Unit] = {

      logger.debug(s"Handling admin event: '$event'")

      event match {
        case OrganizationDeprecated(uuid, _, _, _) =>
          viewCoordinator.stop(OrganizationRef(uuid))
          fileAttributesCoordinator.stop(OrganizationRef(uuid))

        case ProjectCreated(uuid, label, orgUuid, orgLabel, desc, am, base, vocab, instant, subject) =>
          // format: off
          implicit val project: Project = Project(config.http.projectsIri + label, label, orgLabel, desc, base, vocab, am, uuid, orgUuid, 1L, deprecated = false, instant, subject.id, instant, subject.id)
          // format: on
          projectInitializer(project, subject)

        case ProjectUpdated(uuid, label, desc, am, base, vocab, rev, instant, subject) =>
          cache.project.get(ProjectRef(uuid)).flatMap {
            case Some(project) =>
              // format: off
              val newProject = Project(config.http.projectsIri + label, label, project.organizationLabel, desc, base, vocab, am, uuid, project.organizationUuid, rev, deprecated = false, instant, subject.id, instant, subject.id)
              // format: on
              cache.project.replace(newProject).flatMap(_ => viewCoordinator.change(newProject, project))
            case None => Task.unit
          }
        case ProjectDeprecated(uuid, rev, _, _) =>
          val deprecated = cache.project.deprecate(ProjectRef(uuid), rev)
          deprecated >> List(viewCoordinator.stop(ProjectRef(uuid)), fileAttributesCoordinator.stop(ProjectRef(uuid))).sequence >> Task.unit

        case _ => Task.unit
      }
    }
    adminClient.events(handle)(config.iam.serviceAccountToken)
  }

  def startResolverStream(): Unit = {
    ResolverIndexer.start(resolvers, cache.resolver)
    ()
  }

  def startViewStream(): Unit = {
    ViewIndexer.start(views, cache.view)
    ()
  }

  def startStorageStream(): Unit = {
    StorageIndexer.start(storages, cache.storage)
    ()
  }
}

object Indexing {

  /**
    * Starts all indexing streams:
    * <ul>
    * <li>Views</li>
    * <li>Projects</li>
    * <li>Accounts</li>
    * <li>Resolvers</li>
    * </ul>
    *
    * @param storages  the storages operations
    * @param views     the views operations
    * @param resolvers the resolvers operations
    * @param cache     the distributed cache
    */
  def start(
      storages: Storages[Task],
      views: Views[Task],
      resolvers: Resolvers[Task],
      viewCoordinator: ProjectViewCoordinator[Task],
      fileAttributesCoordinator: ProjectAttributesCoordinator[Task]
  )(
      implicit cache: Caches[Task],
      adminClient: AdminClient[Task],
      projectInitializer: ProjectInitializer[Task],
      config: AppConfig,
      as: ActorSystem
  ): Unit = {
    val indexing = new Indexing(storages, views, resolvers, viewCoordinator, fileAttributesCoordinator)
    indexing.startAdminStream()
    indexing.startResolverStream()
    indexing.startViewStream()
    indexing.startStorageStream()
  }

}
// $COVERAGE-ON$
