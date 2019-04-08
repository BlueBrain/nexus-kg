package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant, ZoneId, ZoneOffset}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, User}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.{IriNode, Literal}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.{Iri, Node}
import io.circe.Json
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

//noinspection NameBooleanParameters
class ResourceFSpec
    extends TestKit(ActorSystem("ResourceFSpec"))
    with WordSpecLike
    with Matchers
    with EitherValues
    with TestHelper {

  private implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)
  private implicit val appConfig: AppConfig = Settings(system).appConfig

  "A ResourceF" should {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

    val identity: Identity = User("dmontero", "someRealm")
    val userIri            = Iri.absolute(s"${appConfig.iam.publicIri.asUri}/realms/someRealm/users/dmontero").right.value
    val anonIri            = Iri.absolute(s"${appConfig.iam.publicIri.asUri}/anonymous").right.value

    val projectRef = ProjectRef(genUUID)
    val id         = Iri.absolute(s"http://example.com/${projectRef.id}").right.value
    val resId      = Id(projectRef, id)
    val json       = Json.obj("key" -> Json.fromString("value"))
    val schema     = Ref(shaclSchemaUri)
    val apiMappings = Map[String, AbsoluteIri](
      "nxv"           -> nxv.base,
      "ex"            -> url"http://example.com/",
      "resource"      -> unconstrainedSchemaUri,
      "elasticsearch" -> nxv.defaultElasticSearchIndex,
      "graph"         -> nxv.defaultSparqlIndex
    )
    implicit val projectMeta: Project = Project(id,
                                                "core",
                                                "bbp",
                                                None,
                                                nxv.projects,
                                                genIri,
                                                apiMappings,
                                                projectRef.id,
                                                genUUID,
                                                1L,
                                                false,
                                                Instant.EPOCH,
                                                userIri,
                                                Instant.EPOCH,
                                                anonIri)

    "compute the metadata graph for a resource" in {
      val resource = ResourceF
        .simpleF(resId, json, 2L, schema = schema, types = Set(nxv.Schema))
        .copy(createdBy = identity, updatedBy = Anonymous)
      resource.metadata() should contain allElementsOf Set[Triple](
        (IriNode(id), nxv.rev, 2L),
        (IriNode(id), nxv.deprecated, false),
        (IriNode(id), nxv.updatedAt, clock.instant()),
        (IriNode(id), nxv.createdAt, clock.instant()),
        (IriNode(id), nxv.createdBy, IriNode(userIri)),
        (IriNode(id), nxv.updatedBy, IriNode(anonIri)),
        (IriNode(id), nxv.self, s"http://127.0.0.1:8080/v1/schemas/bbp/core/ex:${projectRef.id}"),
        (IriNode(id), nxv.project, url"http://localhost:8080/v1/projects/bbp/core"),
        (IriNode(id), nxv.constrainedBy, IriNode(schema.iri))
      )
    }

    "compute the metadata graph for a resource when self is an iri" in {
      val resource = ResourceF
        .simpleF(resId, json, 2L, schema = schema, types = Set(nxv.Schema))
        .copy(createdBy = identity, updatedBy = Anonymous)
      resource.metadata(asIri = true) should contain allElementsOf Set[Triple](
        (IriNode(id), nxv.rev, 2L),
        (IriNode(id), nxv.deprecated, false),
        (IriNode(id), nxv.updatedAt, clock.instant()),
        (IriNode(id), nxv.createdAt, clock.instant()),
        (IriNode(id), nxv.createdBy, IriNode(userIri)),
        (IriNode(id), nxv.updatedBy, IriNode(anonIri)),
        (IriNode(id), nxv.self, url"http://127.0.0.1:8080/v1/schemas/bbp/core/ex:${projectRef.id}"),
        (IriNode(id), nxv.project, url"http://localhost:8080/v1/projects/bbp/core"),
        (IriNode(id), nxv.constrainedBy, IriNode(schema.iri))
      )
    }

    "remove the metadata from a resource" in {
      val jsonMeta = json deepMerge Json.obj("@id" -> Json.fromString(id.value.asString)) deepMerge Json.obj(
        nxv.rev.value.asString -> Json.fromLong(10L))
      simpleV(resId, jsonMeta, 2L, schema = schema, types = Set(nxv.Schema)).value.graph.removeMetadata.triples shouldEqual Set
        .empty[Triple]
    }
  }

}
