package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMessage.DiscardedEntity
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.stream.Materializer
import akka.util.ByteString
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.UntypedHttpClient
import ch.epfl.bluebrain.nexus.kg.auth.types.{AccessControl, AccessControlList, Permission}
import ch.epfl.bluebrain.nexus.kg.auth.types.Permission.{Own, Read, Write}
import ch.epfl.bluebrain.nexus.kg.auth.types.identity.Identity.GroupRef
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.auto._
import io.circe.syntax._

import scala.concurrent.Future

trait MockedIAMClient {
  val ValidToken       = "validToken"
  val ValidCredentials = OAuth2BearerToken(ValidToken)

  private val permissions                    = Set(Own, Read, Write, Permission("publish"))
  private implicit val config: Configuration = Configuration.default.withDiscriminator("type")

  implicit def fixedClient(implicit as: ActorSystem, mt: Materializer): UntypedHttpClient[Future] =
    new UntypedHttpClient[Future] {
      import as.dispatcher

      override def apply(req: HttpRequest): Future[HttpResponse] =
        req
          .header[Authorization]
          .collect {
            case Authorization(ValidCredentials) =>
              Future.successful(
                HttpResponse(entity = HttpEntity(
                  ContentTypes.`application/json`,
                  AccessControlList(
                    Set(AccessControl(GroupRef("https://bbpteam.epfl.ch/auth/realms/BBP", "/bbp-ou-nexus"),
                                      permissions))).asJson.noSpaces
                )))
            case Authorization(OAuth2BearerToken(_)) =>
              Future.successful(
                HttpResponse(
                  entity = HttpEntity(
                    ContentTypes.`application/json`,
                    """{"code":"Something bad happened, can't tell you exactly what; want to try again?"}"""),
                  status = StatusCodes.InternalServerError
                ))
          }
          .getOrElse(Http().singleRequest(req))

      override def discardBytes(entity: HttpEntity): Future[DiscardedEntity] =
        Future.successful(entity.discardBytes())

      override def toString(entity: HttpEntity): Future[String] =
        entity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String)
    }
}
