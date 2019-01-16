package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Stash}
import akka.cluster.sharding.ShardRegion.{ExtractEntityId, ExtractShardId}
import akka.cluster.sharding.{ClusterSharding, ClusterShardingSettings, ShardRegion}
import akka.stream.ActorMaterializer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{withUnmarshaller, UntypedHttpClient}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.kg.async.ProjectViewCoordinatorActor.Msg._
import ch.epfl.bluebrain.nexus.kg.async.ViewCache.RevisionedViews
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexer, SparqlIndexer, View}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{ProjectLabel, ProjectRef, Resources}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.service.indexer.cache.OnKeyValueStoreChange
import ch.epfl.bluebrain.nexus.service.indexer.stream.StreamCoordinator.{Stop => StreamCoordinatorStop}
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import shapeless.TypeCase

import scala.collection.immutable.Set
import scala.collection.mutable

/**
  * Coordinator backed by akka actor which runs the views' streams inside the provided project
  */
private abstract class ProjectViewCoordinatorActor(viewCache: ViewCache[Task])
    extends Actor
    with Stash
    with ActorLogging {

  private val children = mutable.Map.empty[SingleView, ActorRef]

  def receive: Receive = {
    case Start(_, project, views) =>
      log.debug("Started coordinator for project '{}' with initial views '{}'", project.projectLabel.show, views)
      context.become(initialized(project))
      viewCache.subscribe(onChange(project.ref))
      children ++= views.map(view => view -> startActor(view, project))
      unstashAll()
    case other =>
      log.debug("Received non Start message '{}', stashing until the actor is initialized", other)
      stash()
  }

  /**
    * Triggered in order to build an indexer actor for a provided view
    *
    * @param view    the view from where to create the indexer actor
    * @param project the project of the current coordinator
    * @return the actor reference
    */
  def startActor(view: SingleView, project: Project): ActorRef

  /**
    * Triggered once an indexer actor has been stopped
    *
    * @param view    the view linked to the indexer actor
    * @param project the project of the current coordinator
    */
  def onActorStopped(view: SingleView, project: Project): Task[Unit]

  /**
    * Triggered when a change to key value store occurs.
    *
    * @param projectRef the project unique reference
    */
  def onChange(projectRef: ProjectRef): OnKeyValueStoreChange[UUID, RevisionedViews]

  private def stopActor(ref: ActorRef): Unit = {
    ref ! StreamCoordinatorStop
    context.stop(ref)

  }

  def initialized(project: Project): Receive = {
    case ViewsChanges(_, views) =>
      //Start new actors
      views.map {
        case view if !children.contains(view) =>
          val ref = startActor(view, project)
          children += view -> ref
        case _ =>
      }
      log.debug("Change on view. Added views '{}' to project '{}'", views, project.projectLabel.show)

      //Stop removed actors
      val toRemove = (children.keySet -- views).map(v => v -> children(v))
      toRemove.map {
        case (v, ref) =>
          stopActor(ref)
          children -= v
          onActorStopped(v, project).runToFuture
      }
      log.debug("Change on view. Deleted views '{}' from project '{}'", toRemove.map(_._1), project.projectLabel.show)

    case _: ProjectChanges =>
    //TODO: After the project has been updated we also have to recreate the elasticSearch/blazegraph indices as if it were a view update.

    case Stop(_) =>
      children.foreach {
        case (view, ref) =>
          stopActor(ref)
          children -= view
      }
  }

}

object ProjectViewCoordinatorActor {

  private[async] sealed trait Msg {

    /**
      * @return the project unique identifier
      */
    def uuid: UUID
  }
  private[async] object Msg {

    final case class Start(uuid: UUID, project: Project, views: Set[SingleView]) extends Msg
    final case class Stop(uuid: UUID)                                            extends Msg
    final case class ViewsChanges(uuid: UUID, views: Set[SingleView])            extends Msg
    final case class ProjectChanges(uuid: UUID, base: AbsoluteIri, vocab: AbsoluteIri, projectLabel: ProjectLabel)
        extends Msg

  }

  private[async] def shardExtractor(shards: Int): ExtractShardId = {
    case (msg: Msg)                  => math.abs(msg.uuid.hashCode) % shards toString
    case ShardRegion.StartEntity(id) => (id.hashCode                % shards) toString
  }

  private[async] val entityExtractor: ExtractEntityId = {
    case (msg: Msg) => (msg.uuid.toString, msg)
  }

  /**
    * Starts the ProjectViewCoordinator shard that coordinates the running views' streams inside the provided project
    *
    * @param resources        the resources operations
    * @param viewCache        the view Cache
    * @param shardingSettings the sharding settings
    * @param shards           the number of shards to use
    */
  final def start(resources: Resources[Task],
                  viewCache: ViewCache[Task],
                  shardingSettings: Option[ClusterShardingSettings],
                  shards: Int)(implicit esClient: ElasticClient[Task],
                               config: AppConfig,
                               mt: ActorMaterializer,
                               ul: UntypedHttpClient[Task],
                               ucl: HttpClient[Task, ResultSet],
                               as: ActorSystem): ActorRef = {

    val props = {
      Props(
        new ProjectViewCoordinatorActor(viewCache) {

          private val sparql              = config.sparql
          private implicit val jsonClient = withUnmarshaller[Task, Json]

          override def startActor(view: SingleView, project: Project): ActorRef =
            view match {
              case v: ElasticView => ElasticIndexer.start(v, resources, project)
              case v: SparqlView  => SparqlIndexer.start(v, resources, project)
            }

          override def onActorStopped(view: SingleView, project: Project): Task[Unit] = view match {
            case v: ElasticView =>
              log.info("ElasticView index '{}' is removed from project '{}'", v.index, project.projectLabel.show)
              //TODO: Retry when delete fails
              esClient.deleteIndex(v.index) *> Task.unit
            case _: SparqlView =>
              log.info("Blazegraph keyspace '{}' is removed from project '{}'", view.name, project.projectLabel.show)
              //TODO: Retry when delete fails
              BlazegraphClient[Task](sparql.base, view.name, sparql.akkaCredentials).deleteNamespace *> Task.unit
          }

          override def onChange(projectRef: ProjectRef): OnKeyValueStoreChange[UUID, RevisionedViews] =
            onViewChange(projectRef, self)

        }
      )
    }
    start(props, shardingSettings, shards)
  }

  private[async] final def start(props: Props, shardingSettings: Option[ClusterShardingSettings], shards: Int)(
      implicit as: ActorSystem): ActorRef = {

    val settings = shardingSettings.getOrElse(ClusterShardingSettings(as)).withRememberEntities(true)
    ClusterSharding(as).start("project-view-coordinator", props, settings, entityExtractor, shardExtractor(shards))
  }

  private[async] def onViewChange(projectRef: ProjectRef,
                                  actorRef: ActorRef): OnKeyValueStoreChange[UUID, RevisionedViews] =
    new OnKeyValueStoreChange[UUID, RevisionedViews] {

      private def singleViews(values: Set[View]): Set[SingleView] = values.collect { case v: SingleView => v }
      private val SetView                                         = TypeCase[RevisionedViews]
      private val projectUuid                                     = projectRef.id

      override def apply(onChange: KeyValueStoreChanges[UUID, RevisionedViews]): Unit = {
        val views = onChange.values.foldLeft(Set.empty[SingleView]) {
          case (acc, ValueAdded(`projectUuid`, SetView(revValue)))    => acc ++ singleViews(revValue.value)
          case (acc, ValueModified(`projectUuid`, SetView(revValue))) => acc ++ singleViews(revValue.value)
          case (acc, _)                                               => acc
        }
        if (views.nonEmpty) actorRef ! ViewsChanges(projectUuid, views)

      }
    }
}
