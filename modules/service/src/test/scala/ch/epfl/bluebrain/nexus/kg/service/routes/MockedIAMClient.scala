package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.util.ByteString
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.iam.auth.{AuthenticatedUser, User}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.commons.iam.identity.IdentityId
import ch.epfl.bluebrain.nexus.commons.test.Resources

import scala.concurrent.Future

trait MockedIAMClient extends Resources {
  val ValidToken       = "validToken"
  val ValidCredentials = OAuth2BearerToken(ValidToken)

  val mockedUser: User = AuthenticatedUser(
    Set(
      GroupRef(IdentityId("localhost:8080/v0/realms/BBP/groups/group1")),
      GroupRef(IdentityId("localhost:8080/v0/realms/BBP/groups/group2")),
      UserRef(IdentityId("localhost:8080/v0/realms/realm/users/f:someUUID:username"))
    ))

  private val mockedAclsJson     = jsonContentOf("/acl/mock-acl.json").noSpaces
  private val mockedUserJson     = jsonContentOf("/acl/mock-user.json").noSpaces
  private val mockedAuthUserJson = jsonContentOf("/acl/mock-auth.json").noSpaces

  implicit def fixedClient(implicit as: ActorSystem, mt: Materializer): UntypedHttpClient[Future] =
    new UntypedHttpClient[Future] {
      import as.dispatcher

      override def apply(req: HttpRequest): Future[HttpResponse] =
        req
          .header[Authorization]
          .collect {
            case Authorization(ValidCredentials) =>
              if (req.uri.toString().contains("/acls/"))
                Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mockedAclsJson)))
              else if (req.uri.path.toString.endsWith("/oauth2/user") & req.uri
                         .query()
                         .toString
                         .equals("filterGroups=true"))
                Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mockedUserJson)))
              else
                Future.successful(HttpResponse(status = StatusCodes.NotFound))

            case Authorization(OAuth2BearerToken(_)) =>
              Future.successful(
                HttpResponse(
                  entity = HttpEntity(
                    ContentTypes.`application/json`,
                    """{"code" : "UnauthorizedCaller", "description" : "The caller is not permitted to perform this request"}"""),
                  status = StatusCodes.Unauthorized
                ))
          }
          .getOrElse {
            if (req.uri.toString().contains("/acls/"))
              Future.successful(HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mockedAuthUserJson)))
            else
              Http().singleRequest(req)
          }

      override def discardBytes(entity: HttpEntity): Future[DiscardedEntity] =
        Future.successful(entity.discardBytes())

      override def toString(entity: HttpEntity): Future[String] =
        entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
    }
}
