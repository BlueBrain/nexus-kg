package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Identity}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.ResourceJsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.kg.resolve.InProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class ResourceRoutes(implicit repo: Repo[Task], adminClient: AdminClient[Future], iamClient: IamClient[Future]) {

  def routes: Route =
    token { implicit optToken =>
      resources ~ schemas
    }

  private def resources(implicit token: Option[AuthToken]): Route =
    (pathPrefix("resources") & project) { implicit proj =>
      // create resource with implicit or generated id
      (post & aliasOrCuriePath & entity(as[Json])) { (schema, source) =>
        callerIdentity.apply { implicit ident =>
          complete(createResource(schema, source))
        }
      } ~
        // create resource with explicit id
        (put & pathPrefix(aliasOrCurie / aliasOrCurie) & entity(as[Json]) & pathEndOrSingleSlash) {
          (schema, id, source) =>
            callerIdentity.apply { implicit ident =>
              complete(createResource(schema, source, Some(id)))
            }
        }
    }

  private def schemas(implicit token: Option[AuthToken]): Route =
    (pathPrefix("schemas") & project) { implicit proj =>
      // create schema with implicit or generated id
      (post & entity(as[Json]) & pathEndOrSingleSlash) { source =>
        callerIdentity.apply { implicit ident =>
          complete(createResource(nxv.ShaclSchema, source))
        }
      } ~
        // create schema with explicit id
        (put & aliasOrCuriePath & entity(as[Json])) { (id, source) =>
          callerIdentity.apply { implicit ident =>
            complete(createResource(nxv.ShaclSchema, source, Some(id)))
          }
        }
    }

  private def createResource(
      schema: AbsoluteIri,
      source: Json,
      optId: Option[AbsoluteIri] = None
  )(implicit project: Project, identity: Identity): Future[Either[Rejection, Resource]] = {
    val projectRef                                     = ProjectRef(project.uuid)
    implicit val resolution: InProjectResolution[Task] = InProjectResolution[Task](projectRef)
    optId match {
      case Some(id) => Resources.create[Task](Id(projectRef, id), Ref(schema), source).value.runAsync
      case None     => Resources.create[Task](projectRef, project.base, Ref(schema), source).value.runAsync
    }
  }

  implicit def resourceEncoder: Encoder[Resource] = ???

}
