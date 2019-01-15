package ch.epfl.bluebrain.nexus.kg.async

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import cats.implicits._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{untyped, withUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SingleView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexer, SparqlIndexer, View}
import ch.epfl.bluebrain.nexus.kg.resources.Resources
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValueStoreChange._
import ch.epfl.bluebrain.nexus.service.indexer.cache.KeyValueStoreSubscriber.KeyValueStoreChanges
import ch.epfl.bluebrain.nexus.service.indexer.cache.OnKeyValueStoreChange
import io.circe.Json
import journal.Logger
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.apache.jena.query.ResultSet
import shapeless.TypeCase

import scala.collection.JavaConverters._
import scala.collection._
import immutable.Set

/**
  * Coordinator for the running views' streams inside the provided project
  * Manages the start and stop of [[SparqlIndexer]] and [[ElasticIndexer]]
  *
  * @param resources the resources operations
  * @param project   the project for this coordinator
  * @param views     the already running views
  */
class ProjectViewCoordinator private (resources: Resources[Task],
                                      project: Project,
                                      views: ConcurrentHashMap[SingleView, ActorRef])(
    implicit viewCache: ViewCache[Task],
    esClient: ElasticClient[Task],
    config: AppConfig,
    ucl: HttpClient[Task, ResultSet],
    as: ActorSystem) {

  //TODO: After the project has been updated we also have to recreate the elasticSearch/blazegraph indices as if it were a view update.
  private val logger              = Logger[this.type]
  private val sparql              = config.sparql
  private implicit val mt         = ActorMaterializer()
  private implicit val ul         = untyped[Task]
  private implicit val jsonClient = withUnmarshaller[Task, Json]
  private val ListSet             = TypeCase[Set[View]]

  private val viewEventChanges: OnKeyValueStoreChange[UUID, Set[View]] = new OnKeyValueStoreChange[UUID, Set[View]] {

    private def onStart(view: SingleView): ActorRef = view match {
      case v: ElasticView => ElasticIndexer.start(v, resources, project)
      case v: SparqlView  => SparqlIndexer.start(v, resources, project)
    }

    private def onStop(view: SingleView): Task[Unit] = view match {
      case v: ElasticView =>
        logger.info(s"ElasticView index '${v.index}' is removed from project '${project.projectLabel.show}'")
        esClient.deleteIndex(v.index) *> Task.pure(views.remove(view)) *> Task.unit
      case _: SparqlView =>
        logger.info(s"Blazegraph keyspace '${view.name}' is removed from project '${project.projectLabel.show}'")
        BlazegraphClient[Task](sparql.base, view.name, sparql.akkaCredentials).deleteNamespace *> Task.pure(
          views.remove(view)) *> Task.unit
    }

    private def singleViews(values: Set[View]): Set[SingleView] =
      values.collect { case v: SingleView => v }

    override def apply(onChange: KeyValueStoreChanges[UUID, Set[View]]): Unit = {
      val viewsScala = views.asScala
      val addedOrModified = onChange.values.foldLeft(Set.empty[SingleView]) {
        case (acc, ValueAdded(uuid: UUID, ListSet(value))) if uuid == project.uuid    => acc ++ singleViews(value)
        case (acc, ValueModified(uuid: UUID, ListSet(value))) if uuid == project.uuid => acc ++ singleViews(value)
        case (acc, _)                                                                 => acc
      }
      val toRemove = (viewsScala.keySet -- addedOrModified).map(v => v -> viewsScala.get(v))
      toRemove.foreach {
        case (v, actorRef) =>
          (onStop(v) *> Task.pure(actorRef.foreach(as.stop))).runToFuture
      }
      val toAdd = addedOrModified.map(v => v -> onStart(v))
      views.clear()
      views.putAll(toAdd.toMap.asJava)
      logger.debug(s"Change on view. Added views '$addedOrModified' to project '${project.projectLabel.show}'")
      logger.debug(s"Change on view. Deleted views '${toRemove.map(_._1)}' from project '${project.projectLabel.show}'")
    }
  }

  private val subscription = viewCache.subscribe(viewEventChanges)

  /**
    * When the views on the project should be deprecated, all the actors that manage those views
    * (ElasticIndexer or SparqlIndexer) will be stopped
    */
  def deprecateProjectViews(): Unit = {
    logger.info(
      s"Organization '${project.organizationLabel}' or project '${project.label}' is deprecated. Stopping actors that manage indices.")
    views.asScala.map { case (_, actor) => as.stop(actor) }
    views.clear()
  }

  /**
    * When the current project gets updated, the current subscription to the view cache finishes
    * and a new ProjectViewCoordinator is created (with a new subscription), containing the new project
    *
    * @param proj the new project metadata
    * @return a new [[ProjectViewCoordinator]] containing the new project metadata wrapped in a [[Task]]
    */
  def updateProject(proj: Project): Task[ProjectViewCoordinator] =
    subscription.flatMap(viewCache.unsubscribe) *> Task.pure(new ProjectViewCoordinator(resources, proj, views))

}

object ProjectViewCoordinator {

  /**
    * Constructs a [[ProjectViewCoordinator]] that coordinates the running views' streams inside the provided project
    *
    * @param resources the resources operations
    * @param project   the project for this coordinator
    */
  final def apply(resources: Resources[Task], project: Project)(implicit viewCache: ViewCache[Task],
                                                                esClient: ElasticClient[Task],
                                                                config: AppConfig,
                                                                ucl: HttpClient[Task, ResultSet],
                                                                as: ActorSystem): ProjectViewCoordinator =
    new ProjectViewCoordinator(resources, project, new ConcurrentHashMap[SingleView, ActorRef]())
}
