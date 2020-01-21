package ch.epfl.bluebrain.nexus

import java.net.URLEncoder
import java.util.UUID

import cats.Show
import cats.implicits._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, Authenticated, Group, User}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.{Rejection, ResId}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary.rdf
import ch.epfl.bluebrain.nexus.rdf.cursor.GraphCursor
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._

import scala.util.Try

package object kg {

  /**
    * @param a the value to encode
    * @return attempts to url encode the provided ''a''. It returns the provided ''s'' when encoding fails
    */
  def urlEncode[A: Show](a: A): String =
    urlEncode(a.show)

  /**
    * @param s the value to encode
    * @return attempts to url encode the provided ''s''. It returns the provided ''s'' when encoding fails
    */
  def urlEncode(s: String): String =
    urlEncodeOrElse(s)(s)

  /**
    * @param s       the value to encode
    * @param default the value when encoding ''s'' fails
    * @return attempts to url encode the provided ''s''. It returns the provided ''default'' when encoding fails
    */
  def urlEncodeOrElse(s: String)(default: => String): String =
    Try(URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20")).getOrElse(default)

  def uuid(): String =
    UUID.randomUUID().toString.toLowerCase

  def identities(resId: ResId, iter: Iterable[GraphCursor]): Either[Rejection, List[Identity]] =
    iter.toList.foldM(List.empty[Identity]) { (acc, innerCursor) =>
      identity(resId, innerCursor).map(_ :: acc)
    }

  def identity(resId: ResId, c: GraphCursor): Either[Rejection, Identity] = {
    lazy val anonymous =
      c.downField(rdf.tpe).focus.as[AbsoluteIri].toOption.collectFirst { case nxv.Anonymous.value => Anonymous }
    lazy val realm         = c.downField(nxv.realm).focus.as[String]
    lazy val user          = (c.downField(nxv.subject).focus.as[String], realm).mapN(User.apply).toOption
    lazy val group         = (c.downField(nxv.group).focus.as[String], realm).mapN(Group.apply).toOption
    lazy val authenticated = realm.map(Authenticated.apply).toOption
    (anonymous orElse user orElse group orElse authenticated).toRight(
      InvalidResourceFormat(
        resId.ref,
        "The provided payload could not be mapped to a resolver because the identity format is wrong"
      )
    )
  }
}
