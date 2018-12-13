package ch.epfl.bluebrain.nexus.kg.indexing.v0

import java.nio.file.Paths
import java.time.{Clock, Instant}

import akka.actor.ActorSystem
import cats.data.{EitherT, OptionT}
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.binary.Binary
import ch.epfl.bluebrain.nexus.kg.resources.binary.Binary.BinaryAttributes
import ch.epfl.bluebrain.nexus.kg.resources.v0.Event
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import io.circe.parser.parse
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.mockito.Mockito
import org.mockito.ArgumentMatcher
import org.mockito.ArgumentMatchers.{argThat, eq => is}
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

class MigrationIndexerSpec
    extends WordSpecLike
    with MockitoSugar
    with Matchers
    with Resources
    with BeforeAndAfter
    with EitherValues
    with ScalaFutures
    with Inspectors {

  private val as             = ActorSystem("MigrationIndexerSpec")
  private val repo           = mock[Repo[Task]]
  private val resourceSchema = Ref(resourceSchemaUri)
  private val shaclSchema    = Ref(shaclSchemaUri)
  private val projectRef     = ProjectRef("org/v0")
  private val base           = url"https://nexus.example.com".value
  private val resource       = ResourceF.simpleF(Id(projectRef, base + "some-id"), Json.obj())(Clock.systemUTC)
  private val success        = EitherT.right[Rejection](Task(resource))
  private val indexer        = new MigrationIndexer(repo, "topic", base, projectRef, ".*".r)(as)

  before {
    Mockito.reset(repo)
  }

  "A MigrationIndexer" should {

    "convert v0 ids" in {
      indexer
        .toId("some/stuff/v0.1.1/2e838302-81df-48be-a348-ef45cbdb5ad0")
        .ref
        .iri
        .asUri shouldEqual "https://nexus.example.com/some/stuff/v0.1.1/2e838302-81df-48be-a348-ef45cbdb5ad0"
      indexer.toId("some/").ref.iri.asUri shouldEqual "https://nexus.example.com/some"
      indexer.toId("").ref.iri.asUri shouldEqual "https://nexus.example.com"
    }

    "process 'instance created' events" in {
      val id     = Id(projectRef, base + "some" + "stuff" + "v0.1.1" + "2e838302-81df-48be-a348-ef45cbdb5ad0")
      val schema = Ref(base + "some" + "stuff" + "v0.1.1")
      val types = Set(url"https://bbp-nexus.epfl.ch/vocabs/bbp/neurosciencegraph/core/v0.1.0/Entity".value,
                      Iri.absolute("nsg:Trace").right.value)
      val source  = instancePayload
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-07-16T07:04:08.799Z")
      Mockito
        .when(repo.create(id, schema, types, source, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/instance-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success
    }

    "process 'context created'" in {
      val id      = Id(projectRef, base + "neurosciencegraph" + "core" + "data" + "v1.0.3")
      val schema  = resourceSchema
      val types   = Set(nxv.Resource.value)
      val source  = contextPayload
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      val instant = Instant.parse("2018-07-30T15:09:16.017Z")
      Mockito
        .when(repo.create(id, schema, types, source, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success
    }

    "process 'schema created'" in {
      val id      = Id(projectRef, base + "some" + "sim" + "tool" + "v0.1.2")
      val schema  = shaclSchema
      val types   = Set(nxv.Schema.value)
      val source  = schemaPayload
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-08-22T14:58:28.838Z")
      Mockito
        .when(repo.create(id, schema, types, source, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success

    }

    "process 'instance attachment created' events" in {
      val id =
        Id(projectRef, base + "some" + "project" + "sim" + "script" + "v0.1.0" + "14572e6b-3811-4b48-9673-194059d40981")
      val rev = 2L
      val attr = new ArgumentMatcher[BinaryAttributes] {
        override def matches(argument: BinaryAttributes): Boolean = {
          argument.filePath == Paths.get("1/4/5/7/2/e/6/b/14572e6b-3811-4b48-9673-194059d40981.1") &&
          argument.filename == "cACint_L23MC.hoc" &&
          argument.mediaType == "application/neuron-hoc" &&
          argument.byteSize == 11641L &&
          argument.digest == Binary.Digest("SHA-256",
                                           "37d393c115f239bdc1782d494125dcd3e6a6aa2766ed0e28e837780830563ce2")
        }
      }
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      val instant = Instant.parse("2018-08-16T15:11:59.924Z")
      Mockito
        .when(repo.attachFromMetadata(is(id), is(rev), argThat(attr), is(instant))(is(author)))
        .thenReturn(success)
      val event = jsonContentOf("/v0/attachment-created.json").as[Event].right.value
      indexer.process(event) shouldEqual success

    }

    "process 'instance attachment removed' events" in {
      val instanceId =
        Id(projectRef, base + "some" + "project" + "sim" + "script" + "v0.1.0" + "14572e6b-3811-4b48-9673-194059d40981")
      val rev      = 2L
      val fileName = "fileName"
      val author   = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      val instant  = Instant.parse("2018-08-16T15:11:59.924Z")
      Mockito
        .when(repo.get(instanceId))
        .thenReturn(
          asOptionT(
            resource.copy(
              id = instanceId,
              binary =
                Set(BinaryAttributes(Paths.get("null"), "fileName", "mediaType", 123L, Binary.Digest("algo", "value")))
            )))
      Mockito
        .when(repo.unattach(instanceId, rev, fileName, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/attachment-removed.json").as[Event].right.value
      indexer.process(event).value.runToFuture.futureValue shouldEqual Right(resource)

    }

    "process 'instance updated' events" in {
      val id  = Id(projectRef, base + "some" + "trace" + "v1.0.0" + "a5b09604-1301-452c-a1a4-4a580d4b24db")
      val rev = 5L
      val types = Set(url"https://bbp-nexus.epfl.ch/vocabs/bbp/neurosciencegraph/core/v0.1.0/Entity".value,
                      Iri.absolute("nsg:Trace").right.value)
      val source  = instancePayload
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-456789abcdef:alice")
      val instant = Instant.parse("2018-08-23T10:24:49.893Z")
      Mockito
        .when(repo.update(id, rev, types, source, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/instance-updated.json").as[Event].right.value
      indexer.process(event) shouldEqual success

    }

    "process 'context updated' events" in {
      val id      = Id(projectRef, base + "demo" + "core" + "data" + "v0.2.8")
      val rev     = 2L
      val types   = Set(nxv.Resource.value)
      val source  = contextPayload
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-05-09T06:15:51.888Z")
      Mockito
        .when(repo.update(id, rev, types, source, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-updated.json").as[Event].right.value
      indexer.process(event) shouldEqual success

    }

    "process 'schema updated' events" in {
      val id      = Id(projectRef, base + "some" + "sim" + "tool" + "v0.1.2")
      val rev     = 2L
      val types   = Set(nxv.Schema.value)
      val source  = schemaPayload
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-08-22T14:58:28.838Z")
      Mockito
        .when(repo.update(id, rev, types, source, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-updated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
    }

    "process 'instance deprecated' events" in {
      val id      = Id(projectRef, base + "some" + "other" + "stuff" + "v0.1.0" + "85b8bc79-8e25-4ae5-81fd-2e76559f6a60")
      val rev     = 2L
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-08-27T13:16:35.621Z")
      Mockito
        .when(repo.deprecate(id, rev, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/instance-deprecated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
    }

    "process 'context deprecated' events" in {
      val id      = Id(projectRef, base + "neurosciencegraph" + "core" + "data" + "v1.0.3")
      val rev     = 2L
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-08-27T13:16:35.621Z")
      Mockito
        .when(repo.deprecate(id, rev, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/context-deprecated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
    }

    "process 'schema deprecated' events" in {
      val id      = Id(projectRef, base + "some" + "sim" + "tool" + "v0.1.2")
      val rev     = 3L
      val author  = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val instant = Instant.parse("2018-08-24T11:29:21.316Z")
      Mockito
        .when(repo.deprecate(id, rev, instant)(author))
        .thenReturn(success)
      val event = jsonContentOf("/v0/schema-deprecated.json").as[Event].right.value
      indexer.process(event) shouldEqual success
    }

    "process 'context published' events" in {
      val contextId     = Id(projectRef, base + "nexus" + "core" + "resource" + "v0.1.0")
      val resourceTypes = Set(nxv.Resource.value)
      val instant       = Instant.parse("2018-01-18T23:52:49.563Z")
      val author        = Anonymous
      val expected      = resource.copy(id = contextId, rev = 3L, tags = Map("published" -> 3L))
      Mockito
        .when(repo.get(contextId, 2L))
        .thenReturn(asOptionT(resource.copy(id = contextId, rev = 2L, types = resourceTypes)))
      Mockito
        .when(repo.update(contextId, 2L, resourceTypes, Json.obj(), instant)(author))
        .thenReturn(asEitherT(resource.copy(id = contextId, rev = 3L)))
      Mockito.when(repo.tag(contextId, 3L, 3L, "published", instant)(author)).thenReturn(asEitherT(expected))
      val event = jsonContentOf("/v0/context-published.json").as[Event].right.value
      indexer.process(event).value.runToFuture.futureValue shouldEqual Right(expected)
    }

    "process 'schema published' events" in {
      val schemaId    = Id(projectRef, base + "some" + "sim" + "tool" + "v0.1.2")
      val schemaTypes = Set(nxv.Schema.value)
      val instant     = Instant.parse("2018-08-22T15:00:43.408Z")
      val author      = UserRef("BBP", "f:9d46ddd6-134e-44d6-aa74-0123456789ab:bob")
      val expected    = resource.copy(id = schemaId, rev = 3L, tags = Map("published" -> 3L))
      Mockito
        .when(repo.get(schemaId, 2L))
        .thenReturn(asOptionT(resource.copy(id = schemaId, rev = 2L, types = schemaTypes)))
      Mockito
        .when(repo.update(schemaId, 2L, schemaTypes, Json.obj(), instant)(author))
        .thenReturn(asEitherT(resource.copy(id = schemaId, rev = 3L)))
      Mockito.when(repo.tag(schemaId, 3L, 3L, "published", instant)(author)).thenReturn(asEitherT(expected))
      val event = jsonContentOf("/v0/schema-published.json").as[Event].right.value
      indexer.process(event).value.runToFuture.futureValue shouldEqual Right(expected)
    }
  }

  private def asOptionT(resource: Resource) = OptionT[Task, Resource](Task(Some(resource)))
  private def asEitherT(resource: Resource) = EitherT[Task, Rejection, Resource](Task(Right(resource)))

  private val instancePayload = parse("""
      |{
      |  "@context": "https://nexus.example.com/contexts/demo/core/data/v0.2.7",
      |  "@type": [
      |    "https://bbp-nexus.epfl.ch/vocabs/bbp/neurosciencegraph/core/v0.1.0/Entity",
      |    "nsg:Trace",
      |    "Concentration"
      |  ],
      |  "uniprotCanonLeadProtID": "E9QAQ5",
      |  "uniprotMajProtIDs": "E9QAQ5,Q9WV60,Q5KU03",
      |  "geneName": "Gsk3b",
      |  "mgiID": "MGI:1861437"
      |}
    """.stripMargin).right.value

  private val schemaPayload = parse(
    """
      |{
      |  "@context": [
      |    "https://nexus.example.com/contexts/neurosciencegraph/core/schema/v0.1.0",
      |    {
      |      "this": "https://nexus.example.com/schemas/some/sim/tool/v0.1.2/shapes/"
      |    },
      |    "https://nexus.example.com/contexts/nexus/core/resource/v0.3.0"
      |  ],
      |  "@id": "https://nexus.example.com/schemas/some/sim/tool/v0.1.2",
      |  "@type": "nxv:Schema",
      |  "nxv:deprecated": false,
      |  "nxv:published": true,
      |  "nxv:rev": 1,
      |  "shapes": [
      |    {
      |      "@id": "this:ToolOptRun",
      |      "@type": "sh:NodeShape",
      |      "and": [
      |        {
      |          "node": "https://nexus.example.com/schemas/neurosciencegraph/commons/entity/v0.1.0/shapes/EntityShape"
      |        },
      |        {
      |          "nodeKind": "sh:BlankNode",
      |          "path": "nsg:gitHash",
      |          "minCount": 1
      |        }
      |      ],
      |      "comment": "",
      |      "label": "run",
      |      "nodekind": "sh:BlankNodeOrIRI",
      |      "targetClass": "nsg:Configuration"
      |    }
      |  ]
      |}
    """.stripMargin
  ).right.value

  private val contextPayload = parse(
    """
      |{
      |  "@context": [
      |    "https://nexus.example.com/contexts/nexus/core/provo20130430/v1.0.0",
      |    "https://nexus.example.com/contexts/nexus/core/schemaorg/v3.3.0",
      |    "https://nexus.example.com/contexts/nexus/core/standards/v0.1.0",
      |    {
      |      "@vocab": "https://bbp-nexus.epfl.ch/vocabs/bbp/neurosciencegraph/core/v0.1.0/",
      |      "nsg": "https://bbp-nexus.epfl.ch/vocabs/bbp/neurosciencegraph/core/v0.1.0/",
      |      "schema": "http://schema.org/",
      |      "brainRegion": {
      |        "@id": "nsg:brainRegion",
      |        "type": "@id"
      |      }
      |    }
      |  ]
      |}
    """.stripMargin).right.value
}
