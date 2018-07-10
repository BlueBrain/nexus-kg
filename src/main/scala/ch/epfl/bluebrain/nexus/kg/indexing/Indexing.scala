package ch.epfl.bluebrain.nexus.kg.indexing

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerSettings
import ch.epfl.bluebrain.nexus.admin.client.types.KafkaEvent._
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, KafkaEvent, Project}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.IllegalEventType
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator.Msg
import ch.epfl.bluebrain.nexus.kg.async.{ProjectViewCoordinator, Projects}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.Indexing.ProxyResolution
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.service.kafka.KafkaConsumer
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import org.apache.kafka.common.serialization.StringDeserializer

import scala.concurrent.Future

// $COVERAGE-OFF$
private class Indexing(projects: Projects[Task], coordinator: ActorRef)(implicit as: ActorSystem,
                                                                        repo: Repo[Task],
                                                                        config: AppConfig) {

  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)

  def startAccountStream(): Unit = {

    def index(event: KafkaEvent): Future[Unit] = {
      val update = event match {
        case OrganizationCreated(id, uuid, rev, _, org) =>
          projects.addAccount(AccountRef(uuid), Account(org.name, rev, id, deprecated = false, uuid), updateRev = false)
        case OrganizationUpdated(id, uuid, rev, _, org) =>
          projects.addAccount(AccountRef(uuid), Account(org.name, rev, id, deprecated = false, uuid), updateRev = true)
        case OrganizationDeprecated(_, uuid, rev, _) =>
          projects.deprecateAccount(AccountRef(uuid), rev)
        case _: ProjectCreated    => throw IllegalEventType("ProjectCreated", "Organization")
        case _: ProjectUpdated    => throw IllegalEventType("ProjectUpdated", "Organization")
        case _: ProjectDeprecated => throw IllegalEventType("ProjectDeprecated", "Organization")
      }
      update.flatMap { updated =>
        if (updated) Task.unit
        else Task.raiseError(new RetriableErr(s"Failed to update account '${event.id}'"))
      }.runAsync
    }

    KafkaConsumer.start(consumerSettings, index, config.kafka.accountTopic, "account-events", committable = false, None)
    ()
  }

  def startProjectStream(): Unit = {

    def processResult(updated: Boolean, accountRef: AccountRef, projectRef: ProjectRef): Task[Unit] = {
      if (updated) {
        coordinator ! Msg(accountRef, projectRef)
        Task.unit
      } else {
        Task.raiseError(new RetriableErr(s"Failed to update project '${projectRef.id}'"))
      }
    }

    def index(event: KafkaEvent): Future[Unit] = {
      val update = event match {
        case ProjectCreated(_, _, uuid, orgUUid, rev, _, proj) =>
          projects
            .addProject(ProjectRef(uuid),
                        Project(proj.name, proj.prefixMappings, proj.base, rev, deprecated = false, uuid),
                        updateRev = false)
            .flatMap(updated => processResult(updated, AccountRef(orgUUid), ProjectRef(uuid)))
        case ProjectUpdated(_, uuid, orgUUid, rev, _, proj) =>
          projects
            .addProject(ProjectRef(uuid),
                        Project(proj.name, proj.prefixMappings, proj.base, rev, deprecated = false, uuid),
                        updateRev = true)
            .flatMap(updated => processResult(updated, AccountRef(orgUUid), ProjectRef(uuid)))
        case ProjectDeprecated(_, uuid, orgUUid, rev, _) =>
          projects
            .deprecateProject(ProjectRef(uuid), rev)
            .flatMap(updated => processResult(updated, AccountRef(orgUUid), ProjectRef(uuid)))
        case _: OrganizationCreated    => throw IllegalEventType("OrganizationCreated", "Project")
        case _: OrganizationUpdated    => throw IllegalEventType("OrganizationUpdated", "Project")
        case _: OrganizationDeprecated => throw IllegalEventType("OrganizationDeprecated", "Project")
      }
      update.runAsync
    }

    KafkaConsumer.start(consumerSettings, index, config.kafka.projectTopic, "project-events", committable = false, None)
    ()
  }

  def startResolverStream(): Unit = {
    def resolution(projectRef: ProjectRef): Resolution[Task] = {
      val resolutionTask = projects.resolvers(projectRef).map { resolvers =>
        val sorted = resolvers.toList.sortBy(_.priority).map {
          case r: InProjectResolver    => InProjectResolution[Task](r.ref)
          case r: CrossProjectResolver => CrossProjectResolution[Task](r.ref, projects)
          case _                       => ??? // TODO: other kinds of resolver
        }
        CompositeResolution(sorted)
      }
      new ProxyResolution(resolutionTask)
    }

    ResolverIndexer.start(projects, resolution)
    ()
  }
}

object Indexing {

  /**
    * Starts all indexing streams:
    * <ul>
    *   <li>Views</li>
    *   <li>Projects</li>
    *   <li>Accounts</li>
    *   <li>Resolvers</li>
    * </ul>
    *
    * @param projects the project operations
    */
  def start(projects: Projects[Task])(implicit as: ActorSystem,
                                      repo: Repo[Task],
                                      ucl: HttpClient[Task, ResultSet],
                                      config: AppConfig): Unit = {

    def selector(view: View): ActorRef = view match {
      case _: ElasticView => ElasticIndexer.start(view)
      case _: SparqlView  => SparqlIndexer.start(view)
    }

    val coordinator = ProjectViewCoordinator.start(projects, selector, None, config.cluster.shards)
    val indexing    = new Indexing(projects, coordinator)
    indexing.startAccountStream()
    indexing.startProjectStream()
    indexing.startResolverStream()
  }

  /**
    * Constructs a [[Resolution]] instance using another existing instance in a Task context.
    */
  private class ProxyResolution(resolution: Task[Resolution[Task]]) extends Resolution[Task] {
    override def resolve(ref: Ref): Task[Option[Resource]] =
      resolution.flatMap(_.resolve(ref))

    override def resolveAll(ref: Ref): Task[List[Resource]] =
      resolution.flatMap(_.resolveAll(ref))
  }

}
// $COVERAGE-ON$
