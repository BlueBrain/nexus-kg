package ch.epfl.bluebrain.nexus.kg.indexing

import java.net.URLEncoder
import java.time.format.DateTimeFormatter
import java.time.{Clock, Instant, ZoneId, ZoneOffset}
import java.util.UUID
import java.util.regex.Pattern.quote

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.testkit.TestKit
import cats.Show
import cats.data.{EitherT, OptionT}
import cats.instances.future._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticClientError
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{NotFound, Unexpected}
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.service.http.Path
import ch.epfl.bluebrain.nexus.service.http.UriOps._
import com.typesafe.config.ConfigFactory
import io.circe.Json
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class ElasticIndexerSpec
    extends TestKit(ActorSystem("ElasticIndexerSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with BeforeAndAfter
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 0.3 seconds)

  import system.dispatcher

  private val resources          = mock[Resources[Future]]
  private val client             = mock[ElasticClient[Future]]
  private implicit val rs        = mock[HttpClient[Future, Json]]
  private implicit val appConfig = new Settings(ConfigFactory.parseResources("app.conf").resolve()).appConfig

  before {
    Mockito.reset(resources)
    Mockito.reset(client)
  }

  "An ElasticIndexer" when {
    val doc         = appConfig.elastic.docType
    val id: ResId   = Id(ProjectRef("org/projectName"), url"https://bbp.epfl.ch/nexus/data/resourceName".value)
    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json =
      Json.obj("@context" -> Json.obj("key" -> Json.fromString(s"${nxv.base.show}key")),
               "@id"      -> Json.fromString(id.value.show),
               "key"      -> Json.fromInt(2))
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val ev                    = Created(id, 2L, schema, Set.empty, json, clock.instant(), Anonymous)
    val defaultEsMapping =
      jsonContentOf("/elastic/mapping.json", Map(quote("{{docType}}") -> appConfig.elastic.docType))

    def urlEncoded[A: Show](a: A): String =
      URLEncoder.encode(a.show, "UTF-8").toLowerCase

    val instantString = clock.instant().atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)

    "using a default view" should {
      val view = ElasticView(
        defaultEsMapping,
        Set.empty,
        None,
        includeMetadata = true,
        sourceAsText = true,
        id.parent,
        nxv.default.value,
        UUID.randomUUID().toString,
        1L,
        deprecated = false
      )
      val index   = s"${appConfig.elastic.indexPrefix}_${view.name}"
      val indexer = new ElasticIndexer(client, view, resources)

      "skip indexing a resource when it exists a higher revision on the indexer" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        when(client.get[Json](index, doc, urlEncoded(id.value), include = Set("_rev")))
          .thenReturn(Future.successful(Json.obj("_rev" -> Json.fromLong(3L))))
        indexer(ev).futureValue shouldEqual (())
      }

      "throw when the event resource is not found on the resources" in {
        when(resources.fetch(id, None)).thenReturn(OptionT.none[Future, Resource])
        whenReady(indexer(ev).failed)(_ shouldEqual NotFound(id.ref))
      }

      "index a resource when it does not exist" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        when(client.get[Json](index, doc, urlEncoded(id.value), include = Set("_rev")))
          .thenReturn(Future.failed(new ElasticClientError(StatusCodes.NotFound, "something")))

        val elasticJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString(schema.iri.show),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString()),
            "_deprecated"      -> Json.fromBoolean(false),
            "_rev"             -> Json.fromLong(2L),
            "_updatedAt"       -> Json.fromString(instantString),
            "_updatedBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString())
          )
        when(client.create(index, doc, urlEncoded(id.value), elasticJson)).thenReturn(Future.successful(()))
        indexer(ev).futureValue shouldEqual (())
        verify(client, times(1)).create(index, doc, urlEncoded(id.value), elasticJson)

      }

      "index a resource when it exists a lower revision on the indexer" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        when(client.get[Json](index, doc, urlEncoded(id.value), include = Set("_rev")))
          .thenReturn(Future.successful(Json.obj("_rev" -> Json.fromLong(1L))))

        val elasticJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString(schema.iri.show),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString()),
            "_deprecated"      -> Json.fromBoolean(false),
            "_rev"             -> Json.fromLong(2L),
            "_updatedAt"       -> Json.fromString(instantString),
            "_updatedBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString())
          )

        when(client.create(index, doc, urlEncoded(id.value), elasticJson)).thenReturn(Future.successful(()))
        indexer(ev).futureValue shouldEqual (())
        verify(client, times(1)).create(index, doc, urlEncoded(id.value), elasticJson)

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
        nxv.default.value,
        UUID.randomUUID().toString,
        1L,
        deprecated = false
      )
      val index   = s"${appConfig.elastic.indexPrefix}_${view.name}"
      val indexer = new ElasticIndexer(client, view, resources)

      "skip indexing a resource when the schema is not on the view" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        indexer(ev).futureValue shouldEqual (())
      }

      "index a resource when it does not exist" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = Ref(nxv.Resource.value))
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        when(client.get[Json](index, doc, urlEncoded(id.value), include = Set("_rev")))
          .thenReturn(Future.failed(new ElasticClientError(StatusCodes.NotFound, "something")))

        val elasticJson = Json
          .obj(
            "@id"              -> Json.fromString(id.value.show),
            "_original_source" -> Json.fromString(json.noSpaces),
            "_constrainedBy"   -> Json.fromString("nxv:Resource"),
            "_createdAt"       -> Json.fromString(instantString),
            "_createdBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString()),
            "_deprecated"      -> Json.fromBoolean(false),
            "_rev"             -> Json.fromLong(2L),
            "_updatedAt"       -> Json.fromString(instantString),
            "_updatedBy"       -> Json.fromString(appConfig.iam.baseUri.append(Path("anonymous")).toString())
          )
        when(client.create(index, doc, urlEncoded(id.value), elasticJson)).thenReturn(Future.successful(()))
        indexer(ev.copy(schema = Ref(nxv.Resource.value))).futureValue shouldEqual (())
        verify(client, times(1)).create(index, doc, urlEncoded(id.value), elasticJson)

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
        nxv.default.value,
        UUID.randomUUID().toString,
        1L,
        deprecated = false
      )
      val index   = s"${appConfig.elastic.indexPrefix}_${view.name}"
      val indexer = new ElasticIndexer(client, view, resources)

      "index a resource when it does not exist" in {
        val res  = ResourceF.simpleF(id, json, rev = 2L, schema = schema).copy(tags = Map("two" -> 1L, "one" -> 2L))
        val resV = simpleV(id, json, 2L, res.types, schema = schema).copy(tags = res.tags)
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        when(client.get[Json](index, doc, urlEncoded(id.value), include = Set("_rev")))
          .thenReturn(Future.failed(new ElasticClientError(StatusCodes.NotFound, "something")))
        when(resources.materialize(res)).thenReturn(EitherT.rightT[Future, Rejection](resV))

        val elasticJson = Json.obj("@id" -> Json.fromString(id.value.show), "nxv:key" -> Json.fromInt(2))
        when(client.create(index, doc, urlEncoded(id.value), elasticJson)).thenReturn(Future.successful(()))
        indexer(ev).futureValue shouldEqual (())
        verify(client, times(1)).create(index, doc, urlEncoded(id.value), elasticJson)
        verify(resources, times(1)).materialize(res)
      }

      "skip indexing a resource when it is not matching the tag defined on the view" in {
        val res = ResourceF.simpleF(id, json, rev = 2L, schema = schema)
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        indexer(ev).futureValue shouldEqual (())
      }

      "throw when the event resource canot be materialized" in {
        val res                  = ResourceF.simpleF(id, json, rev = 2L, schema = schema).copy(tags = Map("two" -> 1L, "one" -> 2L))
        val rejection: Rejection = Unexpected("something")
        when(resources.fetch(id, None)).thenReturn(OptionT.some(res))
        when(client.get[Json](index, doc, urlEncoded(id.value), include = Set("_rev")))
          .thenReturn(Future.failed(new ElasticClientError(StatusCodes.NotFound, "something")))
        when(resources.materialize(res)).thenReturn(EitherT.leftT[Future, ResourceV](rejection))

        whenReady(indexer(ev).failed)(_ shouldEqual rejection)
        verify(resources, times(1)).materialize(res)
      }
    }
  }

}
