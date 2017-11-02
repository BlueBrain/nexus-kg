package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.Properties

import ch.epfl.bluebrain.nexus.commons.test.Randomness
import io.circe.Json

import scala.collection.JavaConverters._

trait IndexerFixture extends Randomness {

  lazy val localhost = "127.0.0.1"

  lazy val properties: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  protected def genId(): String =
    genString(length = 4, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  protected def genJson(): Json =
    Json.obj("key" -> Json.fromString(genString()))

  protected def genName(): String =
    genString(length = 8, Vector.range('a', 'z') ++ Vector.range('0', '9'))
}
