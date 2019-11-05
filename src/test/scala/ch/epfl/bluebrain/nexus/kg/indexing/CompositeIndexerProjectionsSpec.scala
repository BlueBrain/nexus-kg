package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers.HttpCredentials
import akka.stream.ActorMaterializer
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{untyped, withUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlResults.{Binding, Bindings, Head}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlWriteQuery.{replace, SparqlReplaceQuery}
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults, SparqlWriteQuery}
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.ActorSystemFixture
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.indexing.View.CompositeView.Projection.{ElasticSearchProjection, SparqlProjection}
import ch.epfl.bluebrain.nexus.kg.indexing.View.{CompositeView, ElasticSearchView, Filter, SparqlView}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.routes.Clients
import ch.epfl.bluebrain.nexus.rdf.Graph.Triple
import ch.epfl.bluebrain.nexus.rdf.Node._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri, RootedGraph}
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Inspectors, Matchers, WordSpecLike}

import scala.collection.mutable
import scala.concurrent.duration._

//noinspection NameBooleanParameters
class CompositeIndexerProjectionsSpec
    extends ActorSystemFixture("CompositeIndexerProjectionsSpec")
    with WordSpecLike
    with Inspectors
    with Matchers
    with IOEitherValues
    with IOOptionValues
    with BeforeAndAfter
    with test.Resources
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 0.3 seconds)

  private implicit val appConfig: AppConfig = Settings(system).appConfig
  private implicit val ec                   = system.dispatcher
  private implicit val mt                   = ActorMaterializer()
  private implicit val adminClient          = mock[AdminClient[Task]]
  private implicit val iamClient            = mock[IamClient[Task]]
  private implicit val utClient             = untyped[Task]
  private implicit val qrClient             = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient           = withUnmarshaller[Task, Json]
  private implicit val sparql               = mock[BlazegraphClient[Task]]
  private implicit val elasticSearch        = mock[ElasticSearchClient[Task]]
  private implicit val storageClient        = mock[StorageClient[Task]]
  private implicit val clients              = Clients()

  val label = ProjectLabel("bbp", "core")
  val mappings: Map[String, Iri.AbsoluteIri] =
    Map("ex" -> url"https://bbp.epfl.ch/nexus/data/".value, "resource" -> nxv.Resource.value)

  // format: off
  implicit val projectMeta: Project =
    Project(genIri, label.value, label.organization, None, nxv.projects.value, genIri, mappings, genUUID, genUUID, 1L, false, Instant.EPOCH, genIri, Instant.EPOCH, genIri)
  // format: on

  val orgRef = OrganizationRef(projectMeta.organizationUuid)

  before {
    Mockito.reset(elasticSearch, sparql)
  }

  val tempQuery = SparqlWriteQuery.replace("http://example.com", Graph())

  "An CompositeIndexer projection indexer function" when {

    val id: ResId = Id(
      ProjectRef(UUID.fromString("4947db1e-33d8-462b-9754-3e8ae74fcd4e")),
      url"https://bbp.epfl.ch/nexus/data/resourceName".value
    )
    val schema: Ref = Ref(url"https://bbp.epfl.ch/nexus/data/schemaName".value)
    val json =
      Json.obj(
        "@context" -> Json.obj("key" -> Json.fromString(s"${nxv.base.show}key")),
        "@id"      -> Json.fromString(id.value.show),
        "key"      -> Json.fromInt(2)
      )
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val defaultEsMapping      = jsonContentOf("/elasticsearch/mapping.json")

    "using an ElasticSearch projection" should {
      import monix.execution.Scheduler.Implicits.global

      val esView = ElasticSearchView(
        defaultEsMapping,
        Filter(),
        includeMetadata = false,
        sourceAsText = true,
        id.parent,
        nxv.defaultElasticSearchIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val query = "CONSTRUCT{...}WHEN{...}"
      val context =
        Json.obj("@base" -> Json.fromString(nxv.base.asString), "@vocab" -> Json.fromString(nxv.base.asString))
      val composite = CompositeView(
        CompositeView.Source(Filter(), includeMetadata = true),
        Set(ElasticSearchProjection(query, esView, context)),
        id.parent,
        genIri,
        genUUID,
        1L,
        deprecated = false
      )

      val projectionIndex = new CompositeIndexerProjections[Task](composite)

      "create document in ElasticSearch index" in {
        val jsonWithMeta = json deepMerge Json.obj(nxv.deprecated.prefix -> Json.fromBoolean(true))
        val res =
          ResourceF.simpleV(
            id,
            Value(jsonWithMeta, jsonWithMeta.contextValue, RootedGraph(blank, Graph())),
            rev = 2L,
            schema = schema
          )
        val results = SparqlResults(
          Head(List("subject", "predicate", "object", "context"), None),
          Bindings(
            List(
              Map(
                "subject"   -> Binding("uri", id.value.asString, None, None),
                "predicate" -> Binding("uri", nxv.deprecated.value.asString, None, None),
                "object"    -> Binding("literal", "true", None, Some("http://www.w3.org/2001/XMLSchema#boolean"))
              ),
              Map(
                "subject"   -> Binding("uri", id.value.asString, None, None),
                "predicate" -> Binding("uri", nxv.rev.value.asString, None, None),
                "object"    -> Binding("literal", "2", None, Some("http://www.w3.org/2001/XMLSchema#long"))
              )
            )
          )
        )

        val doc = Json.obj(
          "@id"             -> Json.fromString(id.value.show),
          "original_source" -> Json.fromString(json.noSpaces),
          "deprecated"      -> Json.fromBoolean(true),
          "rev"             -> Json.fromLong(2L)
        )
        val defaultIndex = composite.defaultSparqlView.index
        sparql.copy(any[Uri], defaultIndex, any[Option[HttpCredentials]]) shouldReturn sparql
        sparql.queryRaw(query) shouldReturn Task(results)
        elasticSearch.create(esView.index, res.id.value.asString, doc) shouldReturn Task.unit
        projectionIndex(res, tempQuery).runToFuture.futureValue shouldEqual List(())
        sparql.queryRaw(query) wasCalled once
        elasticSearch.create(esView.index, res.id.value.asString, doc) wasCalled once
      }

      "remove document id when the query does not return results" in {
        val jsonWithMeta = json deepMerge Json.obj(nxv.deprecated.prefix -> Json.fromBoolean(true))
        val res =
          ResourceF.simpleV(
            id,
            Value(jsonWithMeta, jsonWithMeta.contextValue, RootedGraph(blank, Graph())),
            rev = 2L,
            schema = schema
          )

        val defaultIndex = composite.defaultSparqlView.index
        sparql.copy(any[Uri], defaultIndex, any[Option[HttpCredentials]]) shouldReturn sparql
        sparql.queryRaw(query) shouldReturn Task(SparqlResults(Head(), Bindings()))
        elasticSearch.delete(esView.index, res.id.value.asString) shouldReturn Task(true)
        projectionIndex(res, tempQuery).runToFuture.futureValue shouldEqual List(())
        sparql.queryRaw(query) wasCalled once
        elasticSearch.delete(esView.index, res.id.value.asString) wasCalled once
      }
    }

    "using an Sparql projection" should {
      import monix.execution.Scheduler.Implicits.global

      val sparqlView = SparqlView(
        Filter(Set(resolverSchemaUri), Set(nxv.Resolver)),
        includeMetadata = false,
        id.parent,
        nxv.defaultSparqlIndex.value,
        genUUID,
        1L,
        deprecated = false
      )
      val query = "CONSTRUCT{...}WHEN{...}"
      val composite = CompositeView(
        CompositeView.Source(Filter(), includeMetadata = true),
        Set(SparqlProjection(query, sparqlView)),
        id.parent,
        genIri,
        genUUID,
        1L,
        deprecated = false
      )

      val projectionIndex = new CompositeIndexerProjections[Task](composite)

      // Work arround for mockito inability to resolve signature with def something[A](value: A*)
      def matchesQuery(replace: SparqlReplaceQuery): mutable.WrappedArray[SparqlWriteQuery] => Boolean =
        _.array(0) == replace

      "create triples in Sparql namedgraph" in {
        val res =
          ResourceF.simpleV(
            id,
            Value(Json.obj(), Json.obj(), RootedGraph(blank, Graph())),
            types = Set(nxv.Resolver),
            rev = 2L,
            schema = resolverRef
          )
        val results = SparqlResults(
          Head(List("subject", "predicate", "object", "context"), None),
          Bindings(
            List(
              Map(
                "subject"   -> Binding("uri", id.value.asString, None, None),
                "predicate" -> Binding("uri", nxv.deprecated.value.asString, None, None),
                "object"    -> Binding("literal", "true", None, Some("http://www.w3.org/2001/XMLSchema#boolean"))
              ),
              Map(
                "subject"   -> Binding("uri", id.value.asString, None, None),
                "predicate" -> Binding("uri", nxv.rev.value.asString, None, None),
                "object"    -> Binding("literal", "2", None, Some("http://www.w3.org/2001/XMLSchema#long"))
              )
            )
          )
        )
        val graph        = Graph(Set[Triple]((id.value, nxv.deprecated.value, true), (id.value, nxv.rev.value, 2L)))
        val defaultIndex = composite.defaultSparqlView.index
        sparql.copy(any[Uri], defaultIndex, any[Option[HttpCredentials]]) shouldReturn sparql
        sparql.copy(any[Uri], sparqlView.index, any[Option[HttpCredentials]]) shouldReturn sparql
        sparql.queryRaw(query) shouldReturn Task(results)
        val replaceQuery = replace(res.id.toGraphUri, graph)
        sparql.bulk(argThat(matchesQuery(replaceQuery), "")) shouldReturn Task.unit
        projectionIndex(res, tempQuery).runToFuture.futureValue shouldEqual List(())
        sparql.queryRaw(query) wasCalled once
      }

      "do nothing when resource does not match filters" in {
        val list =
          List(
            ResourceF
              .simpleV(id, Value(Json.obj(), Json.obj(), RootedGraph(blank, Graph())), rev = 2L, schema = resolverRef),
            ResourceF.simpleV(
              id,
              Value(Json.obj(), Json.obj(), RootedGraph(blank, Graph())),
              rev = 2L,
              types = Set(nxv.Resolver)
            )
          )
        forAll(list) { res =>
          projectionIndex(res, tempQuery).runToFuture.futureValue shouldEqual List.empty
        }
      }

      "remove namedgraph when the query does not return results" in {
        val res =
          ResourceF.simpleV(
            id,
            Value(Json.obj(), Json.obj(), RootedGraph(blank, Graph())),
            types = Set(nxv.Resolver),
            rev = 2L,
            schema = resolverRef
          )

        val defaultIndex = composite.defaultSparqlView.index
        sparql.copy(any[Uri], defaultIndex, any[Option[HttpCredentials]]) shouldReturn sparql
        sparql.copy(any[Uri], sparqlView.index, any[Option[HttpCredentials]]) shouldReturn sparql
        sparql.queryRaw(query) shouldReturn Task(SparqlResults(Head(), Bindings()))
        sparql.drop(res.id.toGraphUri) shouldReturn Task.unit
        projectionIndex(res, tempQuery).runToFuture.futureValue shouldEqual List(())
        sparql.queryRaw(query) wasCalled once
        sparql.drop(res.id.toGraphUri) wasCalled once
      }
    }

  }
}
