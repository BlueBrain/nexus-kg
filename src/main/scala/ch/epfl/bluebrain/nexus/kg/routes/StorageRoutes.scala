package ch.epfl.bluebrain.nexus.kg.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.cache.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.IdSegment
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.RootedGraph
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class StorageRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit as: ActorSystem,
    project: Project,
    viewCache: ViewCache[Task],
    indexers: Clients[Task],
    config: AppConfig)
    extends CommonRoutes(resources, "storages", acls, caller) {

  def routes: Route =
    create(storageRef) ~ list(storageRef) ~
      pathPrefix(IdSegment) { id =>
        concat(
          create(id, storageRef),
          update(id, storageRef),
          tag(id, storageRef),
          deprecate(id, storageRef),
          fetch(id, storageRef),
          tags(id, storageRef)
        )
      }

  override implicit def additional: AdditionalValidation[Task] = AdditionalValidation.storage[Task]

  override def transform(json: Json): Json =
    json.addContext(storageCtxUri)

  override def transform(r: ResourceV): Task[ResourceV] = Task.pure {
    r.map { value =>
      val filter = Set[IriNode](nxv.accessKey, nxv.secretKey)
      val graph  = value.graph.remove(p = filter.contains)
      value.copy(graph = RootedGraph(value.graph.rootNode, graph))
    }
  }

}
