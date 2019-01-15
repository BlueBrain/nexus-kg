package ch.epfl.bluebrain.nexus

import java.net.URLEncoder
import java.util.UUID

import cats.Show
import cats.syntax.show._

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
    Try(URLEncoder.encode(s, "UTF-8")).getOrElse(default)

  def uuid(): String =
    UUID.randomUUID().toString.toLowerCase

  /**
    * Converts a map where the values are Option into a Left(values) with the None values or a Right(map) with the Some values
    *
    * @param map the map
    * @tparam A the map key type
    * @tparam B the map value type
    */
  def resultOrFailures[A, B](map: Map[A, Option[B]]): Either[Set[A], Map[A, B]] = {
    val failed = map.collect { case (v, None) => v }
    if (failed.nonEmpty) Left(failed.toSet)
    else Right(map.collect { case (k, Some(v)) => k -> v })
  }
}
