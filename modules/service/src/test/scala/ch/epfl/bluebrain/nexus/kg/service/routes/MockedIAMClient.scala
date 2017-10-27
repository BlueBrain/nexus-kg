package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.util.ByteString
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.iam.acls._
import ch.epfl.bluebrain.nexus.commons.iam.auth.{AuthenticatedUser, User}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.{AuthenticatedRef, GroupRef, UserRef}
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.syntax._

import scala.concurrent.Future

trait MockedIAMClient {
  val ValidToken       = "validToken"
  val ValidCredentials = OAuth2BearerToken(ValidToken)

  val mockedUser: User = AuthenticatedUser(
    Set(GroupRef("BBP", "group1"), GroupRef("BBP", "group2"), UserRef("realm", "f:someUUID:username")))
  val mockedAcls = AccessControlList(
    Set(AccessControl(GroupRef("BBP", "group1"), Permissions(Own, Read, Write, Permission("publish")))))
  val mockedAnonAcls = AccessControlList(Set(AccessControl(AuthenticatedRef(None), Permissions(Read))))

  private implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  implicit def fixedClient(implicit as: ActorSystem, mt: Materializer): UntypedHttpClient[Future] =
    new UntypedHttpClient[Future] {
      import as.dispatcher

      override def apply(req: HttpRequest): Future[HttpResponse] =
        req
          .header[Authorization]
          .collect {
            case Authorization(ValidCredentials) =>
              if (req.uri.toString().contains("/acls/"))
                Future.successful(
                  HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mockedAcls.asJson.noSpaces)))
              else if (req.uri.path.tail.toString().endsWith("/user"))
                Future.successful(
                  HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mockedUser.asJson.noSpaces)))
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
              Future.successful(
                HttpResponse(entity = HttpEntity(ContentTypes.`application/json`, mockedAnonAcls.asJson.noSpaces)))
            else
              Http().singleRequest(req)
          }

      override def discardBytes(entity: HttpEntity): Future[DiscardedEntity] =
        Future.successful(entity.discardBytes())

      override def toString(entity: HttpEntity): Future[String] =
        entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
    }
}
