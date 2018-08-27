package ch.epfl.bluebrain.nexus.kg.resources

import java.util.UUID

import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.{Resources => TestResources}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.resources.ElasticDecoders._
class ElasticDecodersSpec extends WordSpecLike with Matchers with TestResources with EitherValues {

  "ElasticDecoder" should {
    implicit val http = HttpConfig("http://resources.nexus.com", 80, "v1", "http://resources.nexus.com")
    implicit def toLabel(implicit project: Project): LabeledProject =
      LabeledProject(ProjectLabel("bbp", "testproject"), project, AccountRef(UUID.randomUUID().toString))

    val elasticPayload = jsonContentOf("/resources/elastic-resource.json")
    "decode representation IDs correctly when schema ID and resource ID are in prefix mappings" in {
      implicit val project = Project(
        "testproject",
        "test",
        Map("test-schema"   -> url"http://schemas.nexus.example.com/test/",
            "test-resource" -> url"http://resources.nexus.com/test-resource/"),
        url"http://unused.com",
        0L,
        false,
        "20fdc0fc-841a-11e8-adc0-fa7ae01bbebc"
      )

      resourceIdDecoder
        .decodeJson(elasticPayload)
        .right
        .value
        .show shouldEqual "http://resources.nexus.com/v1/resources/bbp/testproject/test-schema:v0.1.0/test-resource:306e8f68-8419-11e8-adc0-fa7ae01bbebc"

    }

    "decode representation IDs correctly when resource ID is in prefix mappings and schema is an alias" in {
      implicit val project = Project(
        "testproject",
        "test",
        Map("test-schema"   -> url"http://schemas.nexus.example.com/test/v0.1.0",
            "test-resource" -> url"http://resources.nexus.com/test-resource/"),
        url"http://unused.com",
        0L,
        false,
        "20fdc0fc-841a-11e8-adc0-fa7ae01bbebc"
      )

      resourceIdDecoder
        .decodeJson(elasticPayload)
        .right
        .value
        .show shouldEqual "http://resources.nexus.com/v1/resources/bbp/testproject/test-schema/test-resource:306e8f68-8419-11e8-adc0-fa7ae01bbebc"

    }
  }

  implicit def toAbsoluteUri(iriNode: IriNode): AbsoluteIri = iriNode.value
}
