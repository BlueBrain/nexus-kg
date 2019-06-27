package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import java.util.UUID

import cats.data.EitherT
import cats.effect.IO
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient.BulkOp
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources.Event.{Created, Updated}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri, RootedGraph}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.Node._
import io.circe.Json
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.duration._

//noinspection NameBooleanParameters
class ElasticSearchIndexerMappingSpec
    extends ActorSystemFixture("ElasticSearchIndexerMappingSpec")
    with WordSpecLike
    with Matchers
    with IOEitherValues
    with IOOptionValues
    with BeforeAndAfter
    with test.Resources
    with IdiomaticMockito
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 0.3 seconds)

  private val resources                     = mock[Resources[IO]]
  private implicit val appConfig: AppConfig = Settings(system).appConfig
  private val tpe1                          = nxv.withSuffix("MyType").value
  private val tpe2                          = nxv.withSuffix("MyType2").value
  val label                                 = ProjectLabel("bbp", "core")
  val mappings: Map[String, Iri.AbsoluteIri] =
    Map("ex" -> url"https://bbp.epfl.ch/nexus/data/".value, "resource" -> nxv.Resource.value)
  implicit val projectMeta: Project =
    Project(genIri,
            label.value,
            label.organization,
            None,
            nxv.projects.value,
            genIri,
            mappings,
            genUUID,
            genUUID,
            1L,
            false,
            Instant.EPOCH,
            genIri,
            Instant.EPOCH,
            genIri)

  val orgRef = OrganizationRef(projectMeta.organizationUuid)

  before {
    Mockito.reset(resources)
  }

  "An ElasticSearch event mapping function" when {

    val id: ResId = Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
                       url"https://bbp.epfl.ch/nexus/data/resourceName".value)
    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json =
      Json.obj("@context" -> Json.obj("key" -> Json.fromString(s"${nxv.base.show}key")),
               "@id"      -> Json.fromString(id.value.show),
               "key"      -> Json.fromInt(2))
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val ev =
      Created(id, orgRef, schema, Set.empty, json, clock.instant(), Anonymous)
    val defaultEsMapping = jsonContentOf("/elasticsearch/mapping.json")

    val instantString = clock.instant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)

    "using a default view" should {
      val view = ElasticSearchView(
        defaultEsMapping,
        Set.empty,
        Set.empty,
        None,
        includeMetadata = true,
        includeDeprecated = true,
        sourceAsText = true,
        id.parent,
        nxv.defaultElasticSearchIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val index  = s"${appConfig.elasticSearch.indexPrefix}_${view.name}"
      val mapper = new ElasticSearchIndexerMapping(view, resources)

      "return none when the event resource is not found on the resources" in {
        resources.fetch(id, selfAsIri = false) shouldReturn EitherT.leftT[IO, ResourceV](NotFound(id.ref): Rejection)
        mapper(ev).ioValue shouldEqual None
      }

      "return a ElasticSearch BulkOp" in {
        val res =
          ResourceF.simpleV(id, Value(json, json.contextValue, RootedGraph(blank, Graph())), rev = 2L, schema = schema)
        resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[IO, Rejection](res)

        val elasticSearchJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString(schema.iri.show),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString((appConfig.iam.publicIri + "anonymous").toString()),
            "_deprecated"      -> Json.fromBoolean(false),
            "_rev"             -> Json.fromLong(2L),
            "_self"            -> Json.fromString("http://127.0.0.1:8080/v1/resources/bbp/core/ex:schemaName/ex:resourceName"),
            "_incoming" -> Json.fromString(
              "http://127.0.0.1:8080/v1/resources/bbp/core/ex:schemaName/ex:resourceName/incoming"),
            "_outgoing" -> Json.fromString(
              "http://127.0.0.1:8080/v1/resources/bbp/core/ex:schemaName/ex:resourceName/outgoing"),
            "_project"   -> Json.fromString("http://localhost:8080/v1/projects/bbp/core"),
            "_updatedAt" -> Json.fromString(instantString),
            "_updatedBy" -> Json.fromString((appConfig.iam.publicIri + "anonymous").toString())
          )
        mapper(ev).some shouldEqual res.id -> BulkOp.Index(index, id.value.asString, elasticSearchJson)
      }
    }

    "using a view for a specific schema and types" should {
      val view = ElasticSearchView(
        defaultEsMapping,
        Set(nxv.Resolver.value, nxv.Resource.value),
        Set(tpe1, tpe2),
        None,
        includeMetadata = true,
        includeDeprecated = true,
        sourceAsText = true,
        id.parent,
        nxv.defaultElasticSearchIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val index  = s"${appConfig.elasticSearch.indexPrefix}_${view.name}"
      val mapper = new ElasticSearchIndexerMapping(view, resources)

      "return none when the schema is not on the view" in {
        val res =
          ResourceF.simpleV(id, Value(json, json.contextValue, RootedGraph(blank, Graph())), rev = 2L, schema = schema)
        resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[IO, Rejection](res)
        mapper(ev).ioValue shouldEqual None
      }

      "return a ElasticSearch BulkOp Delete" in {
        val res = ResourceF.simpleV(id,
                                    Value(json, json.contextValue, RootedGraph(blank, Graph())),
                                    rev = 2L,
                                    schema = Ref(nxv.Resource.value))
        resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[IO, Rejection](res)

        mapper(ev.copy(schema = Ref(nxv.Resource.value))).some shouldEqual
          res.id -> BulkOp.Delete(index, id.value.asString)

      }

      "return a ElasticSearch BulkOp Index" in {
        val otherTpe = nxv.withSuffix("Other").value
        val res = ResourceF.simpleV(id,
                                    Value(json, json.contextValue, RootedGraph(blank, Graph())),
                                    rev = 2L,
                                    schema = Ref(nxv.Resource.value),
                                    types = Set(tpe1, otherTpe),
                                    deprecated = true)
        resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[IO, Rejection](res)

        val elasticSearchJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "@type"            -> Json.arr(Json.fromString(tpe1.asString), Json.fromString(otherTpe.asString)),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString(nxv.Resource.value.show),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString((appConfig.iam.publicIri + "anonymous").toString()),
            "_deprecated"      -> Json.fromBoolean(true),
            "_self"            -> Json.fromString("http://127.0.0.1:8080/v1/resources/bbp/core/resource/ex:resourceName"),
            "_incoming" -> Json.fromString(
              "http://127.0.0.1:8080/v1/resources/bbp/core/resource/ex:resourceName/incoming"),
            "_outgoing" -> Json.fromString(
              "http://127.0.0.1:8080/v1/resources/bbp/core/resource/ex:resourceName/outgoing"),
            "_project"   -> Json.fromString("http://localhost:8080/v1/projects/bbp/core"),
            "_rev"       -> Json.fromLong(2L),
            "_updatedAt" -> Json.fromString(instantString),
            "_updatedBy" -> Json.fromString((appConfig.iam.publicIri + "anonymous").toString())
          )
        mapper(ev.copy(schema = Ref(nxv.Resource.value))).some shouldEqual
          res.id -> BulkOp.Index(index, id.value.asString, elasticSearchJson)

      }
    }

    "using a view without metadata, without sourceAsText, deleting deprecated and targeting a specific tag" should {
      val view = ElasticSearchView(
        defaultEsMapping,
        Set.empty,
        Set.empty,
        Some("one"),
        includeMetadata = false,
        includeDeprecated = false,
        sourceAsText = false,
        id.parent,
        nxv.defaultElasticSearchIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val index  = s"${appConfig.elasticSearch.indexPrefix}_${view.name}"
      val mapper = new ElasticSearchIndexerMapping(view, resources)

      "return a ElasticSearch BulkOp Index" in {
        val res = ResourceF
          .simpleV(id, Value(json, json.contextValue, RootedGraph(blank, Graph())), rev = 2L, schema = schema)
          .copy(tags = Map("two" -> 1L, "one" -> 2L))
        resources.fetch(id, "one", selfAsIri = false) shouldReturn EitherT.rightT[IO, Rejection](res)

        val elasticSearchJson = Json.obj("@id" -> Json.fromString(id.value.show), "key" -> Json.fromInt(2))
        mapper(ev).some shouldEqual res.id -> BulkOp.Index(index, id.value.asString, elasticSearchJson)
      }

      "return a ElasticSearch BulkOp Delete" in {
        val res = ResourceF
          .simpleV(id,
                   Value(json, json.contextValue, RootedGraph(blank, Graph())),
                   rev = 2L,
                   schema = schema,
                   deprecated = true)
          .copy(tags = Map("two" -> 1L, "one" -> 2L))
        resources.fetch(id, "one", selfAsIri = false) shouldReturn EitherT.rightT[IO, Rejection](res)

        mapper(ev).some shouldEqual res.id -> BulkOp.Delete(index, id.value.asString)
      }

      "return None when it is not matching the tag defined on the view" in {
        resources.fetch(id, "one", selfAsIri = false) shouldReturn EitherT.leftT[IO, ResourceV](
          NotFound(id.ref, tagOpt = Some("one")): Rejection)
        mapper(ev).ioValue shouldEqual None
      }

      "skip previous events from the same id" in {
        val instant = clock.instant()
        val updated =
          Updated(id, orgRef, 3L, Set.empty, Json.obj("key" -> Json.fromString("updated")), instant, Anonymous)
        val ev2 =
          Created(id.copy(parent = ProjectRef(genUUID)), orgRef, schema, Set.empty, json, clock.instant(), Anonymous)

        val expected = List(ev2, updated)

        List(ev.id -> ev, updated.id -> updated, ev2.id -> ev2).removeDupIds should contain theSameElementsAs expected
      }
    }
  }

}
