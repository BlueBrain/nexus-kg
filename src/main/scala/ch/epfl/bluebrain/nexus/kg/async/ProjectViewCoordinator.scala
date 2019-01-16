package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{withUnmarshaller, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinator._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexer, SparqlIndexer, View}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.service.indexer.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy
import ch.epfl.bluebrain.nexus.service.indexer.retryer.RetryStrategy.Backoff
import ch.epfl.bluebrain.nexus.service.indexer.retryer.syntax._
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.Stop
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import shapeless.TypeCase

import scala.collection.immutable.Set
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration._

/**
  * Coordinator for the running views' streams inside the provided project
  * Manages the start and stop of [[SparqlIndexer]] and [[ElasticIndexer]]
  *
  * @param resources the resources operations
  * @param cache     the cache bundle
  */
class ProjectViewCoordinator private (resources: Resources[Task], cache: Caches[Task])(implicit
                                                                                       esClient: ElasticClient[Task],
                                                                                       config: AppConfig,
                                                                                       mt: ActorMaterializer,
                                                                                       ul: UntypedHttpClient[Task],
                                                                                       ucl: HttpClient[Task, ResultSet],
                                                                                       as: ActorSystem)
    extends Actor
    with Stash
    with ActorLogging {

  private implicit val strategy: RetryStrategy = Backoff(1 minute, 0.2)
  private val sparql                           = config.sparql
  private implicit val jsonClient              = withUnmarshaller[Task, Json]
  private val ListSet                          = TypeCase[Set[View]]
  private val projectRef                       = ProjectRef(UUID.fromString(self.path.name))

  init()

  def init(): Unit = {
    log.debug("Initializing coordinator for project UUID {}", projectRef.id.toString)

    val start = for {
      project <- cache.project.get(projectRef).retryWhenNot { case Some(ac) => ac }
      views   <- cache.view.get(projectRef).map(_.collect { case v: SingleView => v })
    } yield Start(project, views)
    val _ = start.runToFuture pipeTo self
  }

  def receive: Receive = {
    case Start(project, views) =>
      log.debug("Started coordinator for project '{}' with initial views '{}'", project.projectLabel.show, views)
      context.become(initialized(project, views))
      unstashAll()
    case other =>
      log.debug("Received non Start message '{}', stashing until the actor is initialized", other)
      stash()
  }

  def initialized(project: Project, existingViews: Set[SingleView]): Receive = {

    val children = mutable.Map.empty[SingleView, ActorRef]

    def onStart(view: SingleView) = view match {
      case v: ElasticView => children += (view -> ElasticIndexer.start(v, resources, project))
      case v: SparqlView  => children += (view -> SparqlIndexer.start(v, resources, project))
    }

    existingViews.map(onStart)

    val viewEventChanges: OnKeyValueStoreChange[UUID, Set[View]] = new OnKeyValueStoreChange[UUID, Set[View]] {

      private def onStop(view: SingleView): Task[Unit] = view match {
        case v: ElasticView =>
          log.info("ElasticView index '{}' is removed from project '{}'", v.index, project.projectLabel.show)
          esClient.deleteIndex(v.index) *> Task.pure(children -= view) *> Task.unit
        case _: SparqlView =>
          log.info("Blazegraph keyspace '{}' is removed from project '{}'", view.name, project.projectLabel.show)
          BlazegraphClient[Task](sparql.base, view.name, sparql.akkaCredentials).deleteNamespace *> Task.pure(
            children.remove(view)) *> Task.unit
      }

      private def singleViews(values: Set[View]): Set[SingleView] =
        values.collect { case v: SingleView => v }

      override def apply(onChange: KeyValueStoreChanges[UUID, Set[View]]): Unit = {
        val addedOrModified = onChange.values.foldLeft(Set.empty[SingleView]) {
          case (acc, ValueAdded(uuid: UUID, ListSet(value))) if uuid == project.uuid    => acc ++ singleViews(value)
          case (acc, ValueModified(uuid: UUID, ListSet(value))) if uuid == project.uuid => acc ++ singleViews(value)
          case (acc, _)                                                                 => acc
        }
        val toRemove = (children.keySet -- addedOrModified).map(v => v -> children.get(v))
        val removed: collection.Set[Future[Unit]] = toRemove.map {
          case (v, actorRef) => (onStop(v) *> Task.pure(actorRef.foreach(stopChild))).runToFuture
        }
        Future.sequence(removed.toList).map(_ => addedOrModified.map(onStart))
        log.debug("Change on view. Added views '{}' to project '{}'", addedOrModified, project.projectLabel.show)
        log.debug("Change on view. Deleted views '{}' from project '{}'", toRemove.map(_._1), project.projectLabel.show)
      }
    }

    cache.view.subscribe(viewEventChanges)

    {
      case _: ProjectUpdated =>
      //TODO: After the project has been updated we also have to recreate the elasticSearch/blazegraph indices as if it were a view update.
      case _: ProjectDeprecated =>
        log.info("project '{}' is deprecated. Stopping actors that manage indices.", project.label)
        children.foreach { case (_, actor) => stopChild(actor) }
        children.clear()
    }

  }

  private def stopChild(ref: ActorRef): Unit = {
    ref ! Stop
    context.stop(ref)
  }

}

object ProjectViewCoordinator {

  private final case class Start(project: Project, views: Set[SingleView])

  sealed trait ProjectMsg {

    /**
      * @return the project unique identifier
      */
    def uuid: UUID
  }

  final case class ProjectCreated(uuid: UUID)    extends ProjectMsg
  final case class ProjectUpdated(uuid: UUID)    extends ProjectMsg
  final case class ProjectDeprecated(uuid: UUID) extends ProjectMsg

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case (msg: ProjectMsg)           => math.abs(msg.uuid.hashCode) % shards toString
    case ShardRegion.StartEntity(id) => (id.hashCode                % shards) toString
  }

  private[async] val entityExtractor: ExtractEntityId = {
    case (msg: ProjectMsg) => (msg.uuid.toString, msg)
  }

  /**
    * Starts the ProjectViewCoordinator shard that coordinates the running views' streams inside the provided project
    *
    * @param resources        the resources operations
    * @param cache            the cache bundle
    * @param shardingSettings the sharding settings
    * @param shards           the number of shards to use
    */
  final def start(resources: Resources[Task],
                  cache: Caches[Task],
                  shardingSettings: Option[ClusterShardingSettings],
                  shards: Int)(implicit esClient: ElasticClient[Task],
                               config: AppConfig,
                               mt: ActorMaterializer,
                               ul: UntypedHttpClient[Task],
                               ucl: HttpClient[Task, ResultSet],
                               as: ActorSystem): ActorRef = {

    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as)
      .start(
        "project-view-coordinator",
        Props(new ProjectViewCoordinator(resources, cache)),
        settings,
        entityExtractor,
        shardExtractor(shards)
      )
  }

}
