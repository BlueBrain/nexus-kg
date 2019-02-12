package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.Caches
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives.IdSegment
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.Json
import monix.eval.Task

class ResolverRoutes private[routes] (resources: Resources[Task], acls: AccessControlLists, caller: Caller)(
    implicit project: Project,
    cache: Caches[Task],
    indexers: Clients[Task],
    store: FileStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends CommonRoutes(resources, "resolvers", acls, caller, cache.view) {
  private val transformation: Transformation[Task, Resolver] = Transformation.resolver

  private implicit val projectCache = cache.project

  def routes: Route = {
    val resolverRefOpt = Some(resolverRef)
    create(resolverRef) ~ list(resolverRefOpt) ~
      pathPrefix(IdSegment) { id =>
        concat(
          create(id, resolverRef),
          update(id, resolverRefOpt),
          tag(id, resolverRefOpt),
          deprecate(id, resolverRefOpt),
          fetch(id, resolverRefOpt),
          tags(id, resolverRefOpt)
        )
      }
  }

  override def transform(json: Json) = json.addContext(resolverCtxUri)

  override implicit def additional: AdditionalValidation[Task] = AdditionalValidation.resolver(caller)

  override def transform(r: ResourceV): Task[ResourceV] = transformation(r)
}
