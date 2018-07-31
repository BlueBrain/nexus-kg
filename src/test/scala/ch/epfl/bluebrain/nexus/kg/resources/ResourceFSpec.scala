package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant, ZoneId, ZoneOffset}

import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.IamConfig
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.{Iri, Node}
import io.circe.Json
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class ResourceFSpec extends WordSpecLike with Matchers with EitherValues with TestHelper {

  private implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)

  "A ResourceF" should {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    implicit val iamConfig    = IamConfig("http://iam.example.com", None)
    val identity: Identity    = UserRef("someRealm", "dmontero")
    val userIri               = Iri.absolute(s"${iamConfig.baseUri}/realms/someRealm/users/dmontero").right.value
    val anonIri               = Iri.absolute(s"${iamConfig.baseUri}/anonymous").right.value

    val projectRef = ProjectRef(uuid)
    val id         = Iri.absolute(s"http://example.com/$uuid").right.value
    val resId      = Id(projectRef, id)
    val json       = Json.obj("key" -> Json.fromString("value"))
    val schema     = Ref(shaclSchemaUri)

    "compute the metadata graph for a resource" in {
      val resource = ResourceF
        .simpleF(resId, json, 2L, schema = schema, types = Set(nxv.Schema))
        .copy(createdBy = identity, updatedBy = Anonymous)
      resource.metadata.triples should contain allElementsOf Set[Triple](
        (IriNode(id), nxv.rev, 2L),
        (IriNode(id), nxv.deprecated, false),
        (IriNode(id), nxv.updatedAt, clock.instant()),
        (IriNode(id), nxv.createdAt, clock.instant()),
        (IriNode(id), nxv.createdBy, IriNode(userIri)),
        (IriNode(id), nxv.updatedBy, IriNode(anonIri)),
        (IriNode(id), nxv.constrainedBy, IriNode(schema.iri))
      )
    }
  }

}
