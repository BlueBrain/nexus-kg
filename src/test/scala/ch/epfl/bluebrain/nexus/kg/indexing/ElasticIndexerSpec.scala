package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import java.util.UUID
import java.util.regex.Pattern.quote

import cats.data.OptionT
import cats.effect.IO
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient.BulkOp
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.KgError.NotFound
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resources.Event.{Created, Updated}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path
import ch.epfl.bluebrain.nexus.service.http.UriOps._
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import io.circe.Json
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.duration._

//noinspection NameBooleanParameters
class ElasticIndexerSpec
    extends ActorSystemFixture("ViewIndexerSpec")
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

  before {
    Mockito.reset(resources)
  }

  "An ElasticIndexer" when {
    val doc = appConfig.elastic.docType
    val id: ResId = Id(ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
                       url"https://bbp.epfl.ch/nexus/data/resourceName".value)
    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json =
      Json.obj("@context" -> Json.obj("key" -> Json.fromString(s"${nxv.base.show}key")),
               "@id"      -> Json.fromString(id.value.show),
               "key"      -> Json.fromInt(2))
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val ev                    = Created(id, schema, Set.empty, json, clock.instant(), Anonymous)
    val defaultEsMapping =
      jsonContentOf("/elastic/mapping.json", Map(quote("{{docType}}") -> appConfig.elastic.docType))

    val instantString = clock.instant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)

    "using a default view" should {
      val view = ElasticView(
        defaultEsMapping,
        Set.empty,
        None,
        includeMetadata = true,
        sourceAsText = true,
        id.parent,
        nxv.defaultElasticIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val index   = s"${appConfig.elastic.indexPrefix}_${view.name}"
      val indexer = new ElasticIndexer(view, resources)

      "throw when the event resource is not found on the resources" in {
        resources.fetch(id, None) shouldReturn OptionT.none[IO, Resource]
        indexer(ev).failed[NotFound]
      }

      "index a resource when it does not exist" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        resources.fetch(id, None) shouldReturn OptionT.some(res)

        val elasticJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString(schema.iri.show),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString()),
            "_deprecated"      -> Json.fromBoolean(false),
            "_rev"             -> Json.fromLong(2L),
            "_self"            -> Json.fromString("http://127.0.0.1:8080/v1/resources/bbp/core/ex:schemaName/ex:resourceName"),
            "_project"         -> Json.fromString("http://localhost:8080/admin/projects/bbp/core"),
            "_updatedAt"       -> Json.fromString(instantString),
            "_updatedBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString())
          )
        indexer(ev).some shouldEqual BulkOp.Index(index, doc, id.value.asString, elasticJson)
      }
    }

    "using a view for a specific schema" should {
      val view = ElasticView(
        defaultEsMapping,
        Set(nxv.Resolver.value, nxv.Resource.value),
        None,
        includeMetadata = true,
        sourceAsText = true,
        id.parent,
        nxv.defaultElasticIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val index   = s"${appConfig.elastic.indexPrefix}_${view.name}"
      val indexer = new ElasticIndexer(view, resources)

      "skip indexing a resource when the schema is not on the view" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        resources.fetch(id, None) shouldReturn OptionT.some(res)
        indexer(ev).ioValue shouldEqual None
      }

      "index a resource when it does not exist" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = Ref(nxv.Resource.value))
        resources.fetch(id, None) shouldReturn OptionT.some(res)

        val elasticJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString(nxv.Resource.value.show),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString()),
            "_deprecated"      -> Json.fromBoolean(false),
            "_self"            -> Json.fromString("http://127.0.0.1:8080/v1/resources/bbp/core/resource/ex:resourceName"),
            "_project"         -> Json.fromString("http://localhost:8080/admin/projects/bbp/core"),
            "_rev"             -> Json.fromLong(2L),
            "_updatedAt"       -> Json.fromString(instantString),
            "_updatedBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString())
          )
        indexer(ev.copy(schema = Ref(nxv.Resource.value))).some shouldEqual BulkOp.Index(index,
                                                                                         doc,
                                                                                         id.value.asString,
                                                                                         elasticJson)

      }
    }

    "using a view without metadata, without sourceAsText and targeting a specific tag" should {
      val view = ElasticView(
        defaultEsMapping,
        Set.empty,
        Some("one"),
        includeMetadata = false,
        sourceAsText = false,
        id.parent,
        nxv.defaultElasticIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val index   = s"${appConfig.elastic.indexPrefix}_${view.name}"
      val indexer = new ElasticIndexer(view, resources)

      "index a resource when it does not exist" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema).copy(tags = Map("two" -> 1L, "one" -> 2L))
        resources.fetch(id, "one", None) shouldReturn OptionT.some(res)

        val elasticJson = Json.obj("@id" -> Json.fromString(id.value.show), "key" -> Json.fromInt(2))
        indexer(ev).some shouldEqual BulkOp.Index(index, doc, id.value.asString, elasticJson)
      }

      "skip indexing a resource when it is not matching the tag defined on the view" in {
        resources.fetch(id, "one", None) shouldReturn OptionT.none[IO, Resource]
        indexer(ev).failed[NotFound]
      }

      "skip previous events from the same id" in {
        val instant = clock.instant()
        val updated = Updated(id, 3L, Set.empty, Json.obj("key" -> Json.fromString("updated")), instant, Anonymous)
        val ev2 =
          Created(id.copy(parent = ProjectRef(genUUID)), schema, Set.empty, json, clock.instant(), Anonymous)

        List(ev, updated, ev2).removeDupIds should contain theSameElementsAs List(ev2, updated)
      }
    }
  }

}
