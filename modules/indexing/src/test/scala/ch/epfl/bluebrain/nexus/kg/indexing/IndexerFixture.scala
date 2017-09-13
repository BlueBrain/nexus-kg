package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.Properties
import scala.collection.JavaConverters._

trait IndexerFixture {

  lazy val localhost = "127.0.0.1"

  lazy val properties: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }
}
