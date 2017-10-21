package ch.epfl.bluebrain.nexus.kg.service.routes

import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.{HttpClient, UnexpectedUnsuccessfulHttpResponse}
import ch.epfl.bluebrain.nexus.kg.auth.types.{AccessControlList, Permission}
import journal.Logger

import scala.concurrent.{ExecutionContext, Future}

object ResourceAccess {
  private val log = Logger[this.type]

  /**
    * Wrapper for IAM uri ''value''
    *
    * @param value the IAM uri
    */
  final case class IamUri(value: Uri)

  /**
    * Check that ''perm'' for a user with token identified by ''cred'' is present in the response to the IAM service.
    *
    * @param cred the bearer token from the HTTP request
    * @param id   the resource id which the requester wants to access
    * @param perm the list of permissions to check against the IAM response.
    *             Only one of them is required to be present.
    */
  def check(cred: OAuth2BearerToken, id: String, perm: Permission)(implicit
                                                                   cl: HttpClient[Future, AccessControlList],
                                                                   iamUri: IamUri,
                                                                   ec: ExecutionContext): Future[Boolean] = {
    cl(Get(Uri(s"${iamUri.value}/acls/kg/$id").withQuery(Query("all" -> "false"))).addCredentials(cred))
      .map(_.toMap.values.find(!_.contains(perm)).isEmpty)
      .recoverWith {
        case ur: UnexpectedUnsuccessfulHttpResponse =>
          log.warn(s"Received an unexpected response status code '${ur.response.status}' from IAM")
          Future.successful(false)
        case err =>
          log.error(s"IAM returned an exception when attempting to retrieve permission for resource '$id'", err)
          Future.successful(false)
      }
  }

  /**
    * Check that ''perm'' for a user with token identified by ''cred'' is present in the response to the IAM service.
    *
    * @param cred the bearer token from the HTTP request
    * @param id   the resource id which the requester wants to access
    * @param perm the list of permissions to check against the IAM response.
    *             Only one of them is required to be present.
    * @tparam Id the generic type of the resource ''id''
    */
  def check[Id](cred: OAuth2BearerToken, id: Id, perm: Permission)(implicit
                                                                   cl: HttpClient[Future, AccessControlList],
                                                                   iamUri: IamUri,
                                                                   ec: ExecutionContext,
                                                                   S: Show[Id]): Future[Boolean] =
    check(cred, id.show, perm)

}
