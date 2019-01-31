package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef, ResourceF}
import ch.epfl.bluebrain.nexus.kg.routes.OutputFormat.{Compacted, Expanded}
import ch.epfl.bluebrain.nexus.kg.routes.ResourceEncoder._
import ch.epfl.bluebrain.nexus.rdf._
import io.circe.Json
import io.circe.syntax._
import io.circe.parser.parse
import org.scalatest.{Matchers, WordSpecLike}

class ResourceEncoderSpec
    extends TestKit(ActorSystem("ResourceEncoderSpec"))
    with WordSpecLike
    with Matchers
    with TestHelper {
  private implicit val appConfig: AppConfig = Settings(system).appConfig
  private implicit val icc: IamClientConfig = appConfig.iam.iamClient

  private implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault)

  private val base       = Iri.absolute("http://example.com/").right.value
  private val voc        = Iri.absolute("http://example.com/voc/").right.value
  private val iri        = base + "id"
  private val subject    = Anonymous
  private val projectRef = ProjectRef(genUUID)
  private val projectId  = Id(projectRef, iri)
  private val resId      = Id(projectRef, base + "foobar")

  private implicit val project: Project = Project(
    projectId.value,
    "proj",
    "org",
    None,
    base,
    voc,
    Map.empty,
    projectRef.id,
    genUUID,
    1L,
    deprecated = false,
    Instant.now(clock),
    subject.id,
    Instant.now(clock),
    subject.id
  )

  "ResourceEncoder" should {
    val json     = Json.obj("@id" -> Json.fromString("foobar"), "foo" -> Json.fromString("bar"))
    val context  = Json.obj("@base" -> Json.fromString(base.asString), "@vocab" -> Json.fromString(voc.asString))
    val resource = ResourceF.simpleF(resId, json)

    "encode resource metadata" in {
      val expected =
        """
          |{
          |  "@id" : "http://example.com/foobar",
          |  "_constrainedBy" : "https://bluebrain.github.io/nexus/schemas/unconstrained.json",
          |  "_createdAt" : "1970-01-01T01:00:00Z",
          |  "_createdBy" : "http://localhost:8080/v1/anonymous",
          |  "_deprecated" : false,
          |  "_project" : "http://localhost:8080/v1/projects/org/proj",
          |  "_rev" : 1,
          |  "_self" : "http://127.0.0.1:8080/v1/resources/org/proj/_/foobar",
          |  "_updatedAt" : "1970-01-01T01:00:00Z",
          |  "_updatedBy" : "http://localhost:8080/v1/anonymous",
          |  "@context" : "https://bluebrain.github.io/nexus/contexts/resource.json"
          |}
        """.stripMargin
      resource.asJson shouldEqual parse(expected).right.value
    }

    "encode resource value in compacted form" in {
      implicit val output: OutputFormat = Compacted
      val triples                       = (Node.iri(base + "foobar"), Node.iri(voc + "foo"), Node.literal("bar"))
      val resourceV                     = resource.map(_ => Value(json, context, Graph(triples)))
      val expected =
        """
          |{
          |  "@context" : [
          |    {
          |      "@base" : "http://example.com/",
          |      "@vocab" : "http://example.com/voc/"
          |    },
          |    "https://bluebrain.github.io/nexus/contexts/resource.json"
          |  ],
          |  "@id" : "foobar",
          |  "foo" : "bar"
          |}
        """.stripMargin
      resourceV.asJson shouldEqual parse(expected).right.value
    }

    "encode resource value in expanded form" in {
      implicit val output: OutputFormat = Expanded
      val triples                       = (Node.iri(base + "foobar"), Node.iri(voc + "foo"), Node.literal("bar"))
      val resourceV                     = resource.map(_ => Value(json, context, Graph(triples)))
      val expected =
        """
          |{
          |  "@id" : "http://example.com/foobar",
          |  "http://example.com/voc/foo" : "bar"
          |}
        """.stripMargin
      resourceV.asJson shouldEqual parse(expected).right.value
    }
  }
}
