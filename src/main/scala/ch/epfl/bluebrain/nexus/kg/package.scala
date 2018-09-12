package ch.epfl.bluebrain.nexus

import java.net.URLEncoder

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
}
