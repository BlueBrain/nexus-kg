package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.resolve.InProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.marshallers.ResourceJsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.{Encoder, Json}
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global

import scala.concurrent.Future

class ResourceRoutes(implicit repo: Repo[Task], adminClient: AdminClient[Future], iamClient: IamClient[Future]) {

  def routes: Route =
    token { implicit optToken =>
      (pathPrefix("resources") & project) { implicit proj =>
        // create resource
        (post & pathPrefix(aliasOrCurie) & entity(as[Json]) & pathEndOrSingleSlash) { (schema, source) =>
          callerIdentity.apply { implicit ident =>
            complete(createResource(schema, source))
          }
        } ~
          (put & pathPrefix(aliasOrCurie) & pathPrefix(aliasOrCurie) & entity(as[Json]) & pathEndOrSingleSlash) {
            (schema, id, source) =>
              callerIdentity.apply { implicit ident =>
                complete(createResource(schema, id, source))
              }
          }
      } ~
        (pathPrefix("schemas") & project) { implicit proj =>
          (post & entity(as[Json]) & pathEndOrSingleSlash) { source =>
            callerIdentity.apply { implicit ident =>
              complete(createResource(Vocabulary.nxv.ShaclSchema, source))
            }
          } ~
            (put & pathPrefix(aliasOrCurie) & entity(as[Json]) & pathEndOrSingleSlash) { (id, source) =>
              callerIdentity.apply { implicit ident =>
                complete(createResource(Vocabulary.nxv.ShaclSchema, id, source))
              }
            }
        }
    }

  private def createResource(schema: AbsoluteIri, source: Json)(
      implicit identity: Identity,
      proj: Project,
      repo: Repo[Task]): Future[Either[Rejection, Resource]] = {
    val projectRef                                     = ProjectRef(proj.uuid)
    implicit val resolution: InProjectResolution[Task] = InProjectResolution[Task](projectRef)
    Resources.create[Task](projectRef, Ref(schema), source).value.runAsync
  }

  private def createResource(schema: AbsoluteIri, id: AbsoluteIri, source: Json)(
      implicit identity: Identity,
      proj: Project,
      repo: Repo[Task]): Future[Either[Rejection, Resource]] = {
    val projectRef                                     = ProjectRef(proj.uuid)
    implicit val resolution: InProjectResolution[Task] = InProjectResolution[Task](projectRef)
    Resources.create[Task](Id(projectRef, id), Ref(schema), source).value.runAsync
  }

  implicit def resourceEndcoer: Encoder[Resource] = ???

}
