package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{Changed, Deleted, Subscribe}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.pipe
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache._
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator.Start
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View.SingleView
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import ch.epfl.bluebrain.nexus.service.indexer.retryer.syntax._
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.duration._

/**
  * Manages the indices that are configured to be run for the selected project. It monitors changes in the organization,
  * project and view resources stored in the distributed data registers.
  *
  * It expects its name to use ''{ORGANIZATION_UUID}_{PROJECT_UUID}'' format. Attempting to create the actor without using
  * the expected format will result in an error.
  *
  * @param cache    the distributed cache
  * @param selector a function that selects the child actor index runner specific for the view type
  * @param onStop   a function that is called whenever a view is stopped
  */
//noinspection ActorMutableStateInspection
class ProjectViewCoordinator(cache: DistributedCache[Task],
                             selector: (SingleView, LabeledProject) => ActorRef,
                             onStop: SingleView => Task[Boolean])
    extends Actor
    with Stash {

  private val replicator = DistributedData(context.system).replicator

  private val Array(organizationUuid, projectUuid) = self.path.name.split('_').map(UUID.fromString)
  private val organizationRef                      = OrganizationRef(organizationUuid)
  private val projectRef                           = ProjectRef(projectUuid)
  private val organization                         = organizationKey(organizationRef)
  private val project                              = projectKey(projectRef)
  private val view                                 = projectViewsKey(projectRef)

  private val log = Logger(s"${getClass.getSimpleName} ($projectUuid)")

  private implicit val strategy: RetryStrategy = Backoff(1 minute, 0.2)

  init()

  def init(): Unit = {
    log.debug("Initializing")

    val start = for {
      org  <- cache.organization(organizationRef).retryWhenNot { case Some(ac) => ac }
      proj <- cache.project(projectRef).retryWhenNot { case Some(ac) => ac }
      vs   <- cache.views(projectRef).map(_.collect { case v: SingleView => v })
    } yield Start(org, LabeledProject(ProjectLabel(org.label, proj.label), proj, organizationRef), vs)
    val _ = start.map { msg =>
      replicator ! Subscribe(organization, self)
      replicator ! Subscribe(project, self)
      replicator ! Subscribe(view, self)
      msg
    }.runToFuture pipeTo self
  }

  def receive: Receive = {
    case s @ Start(ac, wrapped, views) =>
      log.debug(s"Started with state '$s'")
      context.become(initialized(ac, wrapped, views, Map.empty))
      unstashAll()
    case other =>
      log.debug(s"Received non Start message '$other', stashing until the actor is initialized")
      stash()
  }

  def initialized(ac: Organization,
                  wrapped: LabeledProject,
                  views: Set[SingleView],
                  childMapping: Map[SingleView, ActorRef])(): Receive = {

    def updateWrappedAc(organization: Organization): LabeledProject =
      wrapped.copy(label = wrapped.label.copy(organization = organization.label))
    def updateWrappedProj(project: Project): LabeledProject =
      wrapped.copy(label = wrapped.label.copy(value = project.label), project = project)

    // stop children if the organization or project is deprecated
    val stopChildren = ac.deprecated || wrapped.project.deprecated
    val nextMapping = if (stopChildren) {
      log.debug("Organization and/or project are deprecated, stopping any running children")
      childMapping.values.foreach { ref =>
        ref ! Stop
        context.stop(ref)
      }
      Task.sequence(childMapping.keys.map(onStop)).runToFuture
      Map.empty[SingleView, ActorRef]
    } else {
      val added   = views -- childMapping.keySet
      val removed = childMapping.keySet -- views
      // stop actors that don't have a corresponding view anymore
      if (removed.nonEmpty) log.debug(s"Stopping view coordinators for $removed")
      removed.foreach { name =>
        val ref = childMapping(name)
        ref ! Stop
        context.stop(ref)
      }
      Task.sequence(removed.map(onStop)).runToFuture

      // construct actors for the new view updates
      if (added.nonEmpty) log.debug(s"Creating view coordinators for $added")
      val newActorsMapping = views.intersect(added).map(v => v -> selector(v, wrapped))

      childMapping -- removed ++ newActorsMapping
    }

    {
      case c @ Changed(`organization`) =>
        val (newWrapped, newOrganization) =
          c.get(organization).value.value.map(a => updateWrappedAc(a) -> a).getOrElse(wrapped -> ac)
        log.debug(s"Organization deprecation changed (${ac.deprecated} -> ${newOrganization.deprecated})")
        context.become(initialized(newOrganization, newWrapped, views, nextMapping))

      case Deleted(`organization`) => // should not happen, maintain previous state
        log.warn("Received organization data entry deleted notification, discarding")

      case c @ Changed(`project`) =>
        val newWrapped = c.get(project).value.value.map(updateWrappedProj).getOrElse(wrapped)
        log.debug(s"Project deprecation changed (${wrapped.project.deprecated} -> ${newWrapped.project.deprecated})")
        context.become(initialized(ac, newWrapped, views, nextMapping))

      case Deleted(`project`) => // should not happen, maintain previous state
        log.warn("Received project data entry deleted notification, discarding")

      case c @ Changed(`view`) =>
        log.debug("View collection changed, updating state")
        context.become(
          initialized(ac, wrapped, c.get(view).value.value.collect { case v: SingleView => v }, nextMapping))

      case Deleted(`view`) =>
        log.debug("View collection removed, updating state")
        context.become(initialized(ac, wrapped, Set.empty, nextMapping))

      case _ => // drop
    }
  }

}

object ProjectViewCoordinator {
  private final case class Start(orgState: Organization, wrapped: LabeledProject, views: Set[SingleView])

  final case class Msg(orgRef: OrganizationRef, projectRef: ProjectRef)

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case Msg(OrganizationRef(acc), ProjectRef(proj)) => math.abs(s"${acc}_$proj".hashCode) % shards toString
    case ShardRegion.StartEntity(id)                 => (id.hashCode                       % shards) toString
  }

  private[async] val entityExtractor: ExtractEntityId = {
    case msg @ Msg(OrganizationRef(acc), ProjectRef(proj)) => (s"${acc}_$proj", msg)
  }

  private[async] def props(cache: DistributedCache[Task],
                           selector: (SingleView, LabeledProject) => ActorRef,
                           onStop: SingleView => Task[Boolean]): Props =
    Props(new ProjectViewCoordinator(cache, selector, onStop))

  /**
    * Starts the ProjectViewCoordinator shard with the provided configuration options.
    *
    * @param cache            the distributed cache
    * @param selector         a function that selects the child actor index runner specific for the view type
    * @param onStop           a function that is called whenever a view is stopped
    * @param shardingSettings the sharding settings
    * @param shards           the number of shards to use
    * @param as               the underlying actor system
    */
  final def start(
      cache: DistributedCache[Task],
      selector: (SingleView, LabeledProject) => ActorRef,
      onStop: SingleView => Task[Boolean],
      shardingSettings: Option[ClusterShardingSettings],
      shards: Int
  )(implicit as: ActorSystem): ActorRef = {
    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as)
      .start("project-view-coordinator",
             props(cache, selector, onStop),
             settings,
             entityExtractor,
             shardExtractor(shards))
  }
}
