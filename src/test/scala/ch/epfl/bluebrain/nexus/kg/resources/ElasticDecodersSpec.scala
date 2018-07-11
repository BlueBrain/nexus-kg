package ch.epfl.bluebrain.nexus.kg.resources

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.{Resources => TestResources}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class ElasticDecodersSpec extends WordSpecLike with Matchers with TestResources with EitherValues {

  "ElasticDecoder" should {

    val elasticPayload = jsonContentOf("/resources/elastic-resource.json")
    "decode representation IDs correctly when schema ID and resource ID are in prefix mappings" in {
      implicit val project = Project(
        "testproject",
        Map("test"          -> url"http://schemas.nexus.example.com/test/",
            "test-resource" -> url"http://resources.nexus.com/test-resource/"),
        url"http://unused.com",
        0L,
        false,
        "20fdc0fc-841a-11e8-adc0-fa7ae01bbebc"
      )

      val decoder = ElasticDecoders.resourceIdDecoder(url"http://resources.nexus.com/resources/bbp/testproject".value)

      decoder
        .decodeJson(elasticPayload)
        .right
        .value
        .show shouldEqual "http://resources.nexus.com/resources/bbp/testproject/test:v0.1.0/test-resource:306e8f68-8419-11e8-adc0-fa7ae01bbebc"

    }
  }

  implicit def toAbsoluteUri(iriNode: IriNode): AbsoluteIri = iriNode.value
}
