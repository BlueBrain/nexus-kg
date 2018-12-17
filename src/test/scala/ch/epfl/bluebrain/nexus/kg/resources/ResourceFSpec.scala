package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant, ZoneId, ZoneOffset}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{Schemas, Settings}
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.node._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.rdf.{Iri, Node}
import io.circe.Json
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class ResourceFSpec
    extends TestKit(ActorSystem("ResourceFSpec"))
    with WordSpecLike
    with Matchers
    with EitherValues
    with TestHelper {

  private implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)
  private implicit val appConfig = Settings(system).appConfig

  "A ResourceF" should {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

    val identity: Identity = UserRef("someRealm", "dmontero")
    val userIri            = Iri.absolute(s"${appConfig.iam.baseUri}/realms/someRealm/users/dmontero").right.value
    val anonIri            = Iri.absolute(s"${appConfig.iam.baseUri}/anonymous").right.value

    val projectRef = ProjectRef(uuid)
    val genUuid    = uuid
    val id         = Iri.absolute(s"http://example.com/$genUuid").right.value
    val resId      = Id(projectRef, id)
    val json       = Json.obj("key" -> Json.fromString("value"))
    val schema     = Ref(shaclSchemaUri)
    val label      = ProjectLabel("bbp", "core")
    val prefixMappings = Map[String, AbsoluteIri](
      "nxv"           -> nxv.base,
      "ex"            -> url"http://example.com/",
      "resource"      -> Schemas.resourceSchemaUri,
      "elasticsearch" -> nxv.defaultElasticIndex,
      "graph"         -> nxv.defaultSparqlIndex
    )
    val projectMeta = Project("name", "core", prefixMappings, nxv.projects, 1L, false, "uuid")

    "compute the metadata graph for a resource" in {
      implicit val labeledProject = LabeledProject(label, projectMeta, AccountRef("uuid"))
      val resource = ResourceF
        .simpleF(resId, json, 2L, schema = schema, types = Set(nxv.Schema))
        .copy(createdBy = identity, updatedBy = Anonymous)
      resource.metadata should contain allElementsOf Set[Triple](
        (IriNode(id), nxv.rev, 2L),
        (IriNode(id), nxv.deprecated, false),
        (IriNode(id), nxv.updatedAt, clock.instant()),
        (IriNode(id), nxv.createdAt, clock.instant()),
        (IriNode(id), nxv.createdBy, IriNode(userIri)),
        (IriNode(id), nxv.updatedBy, IriNode(anonIri)),
        (IriNode(id), nxv.self, url"http://127.0.0.1:8080/v1/schemas/bbp/core/ex:$genUuid"),
        (IriNode(id), nxv.project, url"http://localhost:8080/admin/projects/bbp/core"),
        (IriNode(id), nxv.constrainedBy, IriNode(schema.iri))
      )
    }

    "remove the metadata from a resource" in {
      val jsonMeta = json deepMerge Json.obj("@id" -> Json.fromString(id.value.asString)) deepMerge Json.obj(
        nxv.rev.value.asString -> Json.fromLong(10L))
      simpleV(resId, jsonMeta, 2L, schema = schema, types = Set(nxv.Schema)).value.graph
        .removeMetadata(resId.value)
        .triples shouldEqual Set.empty[Triple]
    }
  }

}
