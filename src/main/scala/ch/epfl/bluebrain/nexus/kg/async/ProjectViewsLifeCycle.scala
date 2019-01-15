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

class ProjectViewsLifeCycle private (resources: Resources[Task],
                                     project: Project,
                                     views: ConcurrentHashMap[SingleView, ActorRef])(
    implicit viewCache: ViewCache[Task],
    esClient: ElasticClient[Task],
    config: AppConfig,
    ucl: HttpClient[Task, ResultSet],
    as: ActorSystem) {

  val viewEventChanges: OnKeyValueStoreChange[UUID, Set[View]] = (onChange: KeyValueStoreChanges[UUID, Set[View]]) => {
    val viewsScala = views.asScala
    val addedOrModified = onChange.values.foldLeft(Set.empty[SingleView]) {
      case (acc, ValueAdded(uuid: UUID, ListSet(value))) if uuid == project.uuid    => acc ++ singleViews(value)
      case (acc, ValueModified(uuid: UUID, ListSet(value))) if uuid == project.uuid => acc ++ singleViews(value)
      case (acc, _)                                                                 => acc
    }
    val toRemove = (viewsScala.keySet -- addedOrModified).map(v => v -> viewsScala.get(v))
    toRemove.foreach {
      case (v, actorRef) =>
        (onStop(v) *> Task.pure(actorRef.map(as.stop))).runToFuture
    }
    val toAdd = addedOrModified.map(v => v -> onStart(v))
    views.clear()
    views.putAll(toAdd.toMap.asJava)
    logger.debug(s"Change on view. Added views '$addedOrModified' to project '${project.projectLabel.show}'")
    logger.debug(s"Change on view. Deleted views '${toRemove.map(_._1)}' from project '${project.projectLabel.show}'")
  }

  val subscription = viewCache.subscribe(viewEventChanges)

  private val sparql              = config.sparql
  private implicit val mt         = ActorMaterializer()
  private implicit val ul         = untyped[Task]
  private implicit val jsonClient = withUnmarshaller[Task, Json]
  private val ListSet             = TypeCase[Set[View]]

  private val logger = Logger[this.type]

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

  def deprecateViews: Task[Unit] = {
    logger.info(
      s"Organization '${project.organizationLabel}' or project '${project.label}' is deprecated. Removing indices.")

    val onStopSeq = views.asScala.map {
      case (view, actor) => onStop(view).map(_ => as.stop(actor))
    }
    Task.sequence(onStopSeq) *> Task.unit
  }

  def updateProject(proj: Project): Task[ProjectViewsLifeCycle] =
    subscription.flatMap(viewCache.unsubscribe) *> Task.pure(new ProjectViewsLifeCycle(resources, proj, views))

}

object ProjectViewsLifeCycle {
  final def apply(resources: Resources[Task], project: Project)(implicit viewCache: ViewCache[Task],
                                                                esClient: ElasticClient[Task],
                                                                config: AppConfig,
                                                                ucl: HttpClient[Task, ResultSet],
                                                                as: ActorSystem): ProjectViewsLifeCycle =
    new ProjectViewsLifeCycle(resources, project, new ConcurrentHashMap[SingleView, ActorRef]())
}
