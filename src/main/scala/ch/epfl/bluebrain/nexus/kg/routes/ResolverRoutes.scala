package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import cats.data.EitherT
import cats.implicits._
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.tracing._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resolve.ResolverEncoder._
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.Schemed
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import com.github.ghik.silencer.silent
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

class ResolverRoutes private[routes] (resources: Resources[Task], acls: FullAccessControlList, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    store: AttachmentStore[Task, AkkaIn, AkkaOut],
    config: AppConfig)
    extends Schemed(resources, resolverSchemaUri, "resolvers", acls, caller) {

  override implicit def additional = AdditionalValidation.resolver(caller, wrapped.accountRef)

  override def list: Route =
    (get & parameter('deprecated.as[Boolean].?) & hasPermission(resourceRead) & pathEndOrSingleSlash) { deprecated =>
      trace("listResolvers") {
        val qr = filterDeprecated(cache.resolvers(wrapped.ref), deprecated)
          .flatMap(_.flatTraverse(_.labeled.value.map(_.toList)))
          .map(r => toQueryResults(r.sortBy(_.priority)))
        complete(qr.runAsync)
      }
    }

  override def transformCreate(j: Json) =
    j.addContext(resolverCtxUri)

  override def transformUpdate(@silent id: AbsoluteIri, j: Json) =
    EitherT.rightT(transformCreate(j))

  override def transformGet(resource: ResourceV) =
    Resolver(resource, wrapped.accountRef) match {
      case Some(r) =>
        val metadata  = resource.metadata ++ resource.typeTriples
        val resValueF = r.labeled.getOrElse(r).map(_.resourceValue(resource.id, resource.value.ctx))
        resValueF.map(v => resource.map(_ => v.copy(graph = v.graph ++ Graph(metadata))))
      case _ => Task.pure(resource)
    }

}
