package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.directives.AuthDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.PathDirectives._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.resolve.InProjectResolution
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import io.circe.Json
import monix.eval.Task

import scala.concurrent.Future

class ResourceRoutes(implicit repo: Repo[Task],
                     adminClient: AdminClient[Future],
                     iamClient: IamClient[Future],
                     shaclSchema: AbsoluteIri) {

  def routes: Route =
    token { implicit optToken =>
      pathPrefix("v1") {
        pathPrefix("resources") {
          project.apply { implicit proj =>
            // create resource
            (post & pathPrefix(aliasOrCurie) & pathEndOrSingleSlash) { schema =>
              callerIdentity.apply { implicit ident =>
                entity(as[Json]) { source =>
                  onSuccess(createResource(proj, schema, source)) {
                    case Left(rejection) => complete("fail: " + rejection.msg)
                    case Right(resource) => complete(resourceRepr(resource))
                  }
                }
              }
            }
          }
        } ~
          pathPrefix("schemas") {
            project(adminClient, optToken) { implicit proj =>
              (post & pathEndOrSingleSlash) {
                callerIdentity.apply { implicit ident =>
                  entity(as[Json]) { source =>
                    onSuccess(createResource(proj, shaclSchema, source)) {
                      case Left(rejection) => complete("fail: " + rejection.msg)
                      case Right(resource) => complete(resourceRepr(resource))
                    }
                  }
                }
              }
            }
          }
      }
    }

  def createResource(proj: Project, schema: AbsoluteIri, source: Json)(
      implicit identity: Identity,
      repo: Repo[Task]): Future[Either[Rejection, Resource]] = {
    val projectRef                                     = ProjectRef(proj.uuid)
    implicit val resolution: InProjectResolution[Task] = InProjectResolution[Task](projectRef)
    //TODO check what's the most appropriate scheduler to use here
    import monix.execution.Scheduler.Implicits.global
    Resources.create[Task](Id(projectRef, extractOrGenerateId(source)), Ref(schema), source).value.runAsync
  }

  def extractOrGenerateId(source: Json): AbsoluteIri = ???

  def resourceRepr(resource: Resource): Json = ???

}
