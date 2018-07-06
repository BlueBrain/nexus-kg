package ch.epfl.bluebrain.nexus.kg.async

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.ddata.DistributedData
import akka.cluster.ddata.Replicator.{Changed, Deleted, Subscribe}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings}
import akka.pattern.pipe
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator.Start
import ch.epfl.bluebrain.nexus.kg.async.Projects._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.resources.{AccountRef, ProjectRef}
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

/**
  * Manages the indices that are configured to be run for the selected project. It monitors changes in the account,
  * project and view resources stored in the distributed data registers.
  *
  * It expects its name to use ''{ACCOUNT_UUID}_{PROJECT_UUID}'' format. Attempting to create the actor without using
  * the expected format will result in an error.
  *
  * @param projects the project operations
  * @param selector a function that selects the child actor index runner specific for the view type
  */
//noinspection ActorMutableStateInspection
class ProjectViewCoordinator(projects: Projects[Task], selector: View => ActorRef) extends Actor with Stash {

  private val replicator = DistributedData(context.system).replicator

  private val Array(accountUuid, projectUuid) = self.path.name.split('_')
  private val accountRef                      = AccountRef(accountUuid)
  private val projectRef                      = ProjectRef(projectUuid)
  private val account                         = accountKey(accountRef)
  private val project                         = projectKey(projectRef)
  private val view                            = viewKey(projectRef)

  private val log = Logger(s"${getClass.getSimpleName} ($projectUuid)")

  init()

  def init(): Unit = {
    log.debug("Initializing")

    replicator ! Subscribe(account, self)
    replicator ! Subscribe(project, self)
    replicator ! Subscribe(view, self)

    val start = for {
      as <- projects.account(accountRef)
      ps <- projects.project(projectRef)
      vs <- projects.views(projectRef)
    } yield Start(as, ps, vs)
    val _ = start.runAsync pipeTo self
  }

  def receive: Receive = {
    case s @ Start(accountStateOpt, projectStateOpt, views) =>
      log.debug(s"Started with state '$s'")
      // for missing values assume not deprecated
      val accountDeprecated = accountStateOpt.exists(_.deprecated)
      val projectDeprecated = projectStateOpt.exists(_.deprecated)
      context.become(initialized(accountDeprecated, projectDeprecated, views, Map.empty))
      unstashAll()
    case other =>
      log.debug(s"Received non Start message '$other', stashing until the actor is initialized")
      stash()
  }

  def initialized(accountState: Boolean,
                  projectState: Boolean,
                  views: Set[View],
                  childMapping: Map[String, ActorRef]): Receive = {
    // stop children if the account or project is deprecated
    val stopChildren = !accountState || !projectState
    val nextMapping = if (stopChildren) {
      log.debug(s"Account and/or project are deprecated, stopping any running children")
      childMapping.values.foreach { ref =>
        ref ! Stop
        context.stop(ref)
      }
      Map.empty[String, ActorRef]
    } else {
      val withNames = views.map(v => v.name -> v).toMap
      val added     = withNames.keySet -- childMapping.keySet
      val removed   = childMapping.keySet -- withNames.keySet

      // stop actors that don't have a corresponding view anymore
      if (removed.nonEmpty) log.debug(s"Stopping view coordinators for $removed")
      removed.foreach { name =>
        val ref = childMapping(name)
        ref ! Stop
        context.stop(ref)
      }

      // construct actors for the new view updates
      if (added.nonEmpty) log.debug(s"Creating view coordinators for $added")
      val newActorsMapping = withNames
        .filter { case (name, _) => added.contains(name) }
        .mapValues(selector)

      childMapping -- removed ++ newActorsMapping
    }

    {
      case c @ Changed(`account`) =>
        val deprecated = c.get(account).value.value.exists(_.deprecated)
        log.debug(s"Account deprecation changed ($accountState -> $deprecated)")
        context.become(initialized(deprecated, projectState, views, nextMapping))

      case Deleted(`account`) => // should not happen, maintain previous state
        log.warn("Received account data entry deleted notification, discarding")

      case c @ Changed(`project`) =>
        val deprecated = c.get(project).value.value.exists(_.deprecated)
        log.debug(s"Project deprecation changed ($projectState -> $deprecated)")
        context.become(initialized(accountState, deprecated, views, nextMapping))

      case Deleted(`project`) => // should not happen, maintain previous state
        log.warn("Received project data entry deleted notification, discarding")

      case c @ Changed(`view`) =>
        log.debug("View collection changed, updating state")
        context.become(initialized(accountState, projectState, c.get(view).value.value, nextMapping))

      case Deleted(`view`) =>
        log.debug("View collection removed, updating state")
        context.become(initialized(accountState, projectState, Set.empty, nextMapping))

      case _ => // drop
    }
  }

}

object ProjectViewCoordinator {
  private final case class Start(accountState: Option[Account], projectState: Option[Project], views: Set[View])

  final case class Msg(accountRef: AccountRef, projectRef: ProjectRef)

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case Msg(AccountRef(acc), ProjectRef(proj)) => math.abs(s"${acc}_$proj".hashCode) % shards toString
  }

  private[async] val entityExtractor: ExtractEntityId = {
    case msg @ Msg(AccountRef(acc), ProjectRef(proj)) => (s"${acc}_$proj", msg)
  }

  private[async] def props(projects: Projects[Task], selector: View => ActorRef): Props =
    Props(new ProjectViewCoordinator(projects, selector))

  /**
    * Starts the ProjectViewCoordinator shard with the provided configuration options.
    *
    * @param projects         the project operations
    * @param selector        a function that selects the child actor index runner specific for the view type
    * @param shardingSettings the sharding settings
    * @param shards           the number of shards to use
    * @param as               the underlying actor system
    */
  final def start(
      projects: Projects[Task],
      selector: View => ActorRef,
      shardingSettings: Option[ClusterShardingSettings],
      shards: Int
  )(implicit as: ActorSystem): ActorRef = {
    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as)
      .start("project-view-coordinator", props(projects, selector), settings, entityExtractor, shardExtractor(shards))
  }
}
