package ch.epfl.bluebrain.nexus.kg.resources

import java.time.Instant
import java.util.UUID

import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.Contexts
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.{urlEncode, TestHelper}

class AccessIdSpec extends WordSpecLike with Matchers with Inspectors with TestHelper {

  "An AccessId" should {
    implicit val http = HttpConfig("http://resources.nexus.com", 80, "v1", "http://resources.nexus.com")
    val defaultPrefixMapping: Map[String, AbsoluteIri] = Map(
      "nxv"           -> nxv.base,
      "nxs"           -> base,
      "nxc"           -> Contexts.base,
      "resource"      -> unconstrainedSchemaUri,
      "elasticsearch" -> nxv.defaultElasticSearchIndex,
      "sparql"        -> nxv.defaultSparqlIndex
    )
    val mappings = Map("test-schema" -> url"http://schemas.nexus.example.com/test/v0.1.0/".value) ++ defaultPrefixMapping
    val uuid     = UUID.fromString("20fdc0fc-841a-11e8-adc0-fa7ae01bbebc")
    implicit val project = Project(genIri,
                                   "core",
                                   "bbp",
                                   None,
                                   url"http://unused.com",
                                   genIri,
                                   mappings,
                                   uuid,
                                   genUUID,
                                   0L,
                                   false,
                                   Instant.EPOCH,
                                   genIri,
                                   Instant.EPOCH,
                                   genIri)

    "generate the access id" in {
      val list = List(
        (url"http://example.com/a".value,
         shaclSchemaUri,
         s"http://resources.nexus.com/v1/schemas/bbp/core/${urlEncode("http://example.com/a")}"),
        (url"http://example.com/a".value,
         fileSchemaUri,
         s"http://resources.nexus.com/v1/files/bbp/core/${urlEncode("http://example.com/a")}"),
        (url"http://example.com/a".value,
         storageSchemaUri,
         s"http://resources.nexus.com/v1/storages/bbp/core/${urlEncode("http://example.com/a")}"),
        (url"http://schemas.nexus.example.com/test/v0.1.0/a".value,
         unconstrainedSchemaUri,
         s"http://resources.nexus.com/v1/resources/bbp/core/_/test-schema:a"),
        (url"${base.asString}b".value,
         url"http://example.com/a".value,
         s"http://resources.nexus.com/v1/resources/bbp/core/${urlEncode("http://example.com/a")}/nxs:b"),
        (url"https://bluebrain.github.io/nexus/schemas/some/other".value,
         url"http://example.com/a".value,
         s"http://resources.nexus.com/v1/resources/bbp/core/${urlEncode("http://example.com/a")}/nxs:some%2Fother")
      )
      forAll(list) {
        case (id, schemaId, result) => AccessId(id, schemaId).asString shouldEqual result
      }
    }
  }

}
