package ch.epfl.bluebrain.nexus

import java.net.URLEncoder

import cats.Show
import cats.syntax.show._

package object kg {

  def urlEncoded[A: Show](a: A): String =
    urlEncoded(a.show)

  def urlEncoded(s: String): String =
    URLEncoder.encode(s, "UTF-8").toLowerCase

}
