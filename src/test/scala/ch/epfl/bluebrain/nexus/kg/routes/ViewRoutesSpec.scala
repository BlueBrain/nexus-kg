package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import cats.data.EitherT
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.{ElasticSearchClientError, ElasticUnexpectedError}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.{HttpClient, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlClientError
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults}
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.CirceEq
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.{Error, KgError, TestHelper}
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.NotFound
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.instances._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.parser.parse
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.Mockito.when
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.ArgumentMatchers.{eq => Eq}
import shapeless.Typeable

import scala.concurrent.duration._

//noinspection TypeAnnotation
class ViewRoutesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with BeforeAndAfter
    with ScalatestRouteTest
    with test.Resources
    with ScalaFutures
    with IdiomaticMockito
    with MacroBasedMatchers
    with TestHelper
    with Inspectors
    with CirceEq
    with Eventually {

  // required to be able to spin up the routes (CassandraClusterHealth depends on a cassandra session)
  override def testConfig: Config =
    ConfigFactory.load("test-no-inmemory.conf").withFallback(ConfigFactory.load()).resolve()

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3 second, 15 milliseconds)

  private implicit val appConfig = Settings(system).appConfig
  private implicit val clock     = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault())

  private implicit val adminClient   = mock[AdminClient[Task]]
  private implicit val iamClient     = mock[IamClient[Task]]
  private implicit val projectCache  = mock[ProjectCache[Task]]
  private implicit val viewCache     = mock[ViewCache[Task]]
  private implicit val resolverCache = mock[ResolverCache[Task]]
  private implicit val storageCache  = mock[StorageCache[Task]]
  private implicit val resources     = mock[Resources[Task]]
  private implicit val views         = mock[Views[Task]]
  private implicit val tagsRes       = mock[Tags[Task]]
  private implicit val initializer   = mock[ProjectInitializer[Task]]

  private implicit val cacheAgg = Caches(projectCache, viewCache, resolverCache, storageCache)

  private implicit val ec            = system.dispatcher
  private implicit val mt            = ActorMaterializer()
  private implicit val utClient      = untyped[Task]
  private implicit val qrClient      = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient    = withUnmarshaller[Task, Json]
  private implicit val sparql        = mock[BlazegraphClient[Task]]
  private implicit val elasticSearch = mock[ElasticSearchClient[Task]]
  private implicit val clients       = Clients()

  private val manageResolver =
    Set(Permission.unsafe("views/query"), Permission.unsafe("resources/read"), Permission.unsafe("views/write"))
  // format: off
  private val routes = Routes(resources, mock[Resolvers[Task]], views, mock[Storages[Task]], mock[Schemas[Task]], mock[Files[Task]], tagsRes, mock[ProjectViewCoordinator[Task]])
  // format: on

  //noinspection NameBooleanParameters
  abstract class Context(perms: Set[Permission] = manageResolver) extends RoutesFixtures {

    projectCache.getBy(label) shouldReturn Task.pure(Some(projectMeta))
    projectCache.getLabel(projectRef) shouldReturn Task.pure(Some(label))
    projectCache.get(projectRef) shouldReturn Task.pure(Some(projectMeta))

    iamClient.identities shouldReturn Task.pure(Caller(user, Set(Anonymous)))
    implicit val acls = AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> perms)))
    iamClient.acls(any[Path], any[Boolean], any[Boolean])(any[Option[AuthToken]]) shouldReturn Task.pure(acls)
    projectCache.getProjectLabels(Set(projectRef)) shouldReturn Task.pure(Map(projectRef -> Some(label)))

    val view = jsonContentOf("/view/elasticview.json")
      .removeKeys("_uuid")
      .deepMerge(Json.obj("@id" -> Json.fromString(id.value.show)))

    val types = Set[AbsoluteIri](nxv.View, nxv.ElasticSearchView)

    def viewResponse(): Json =
      response(viewRef) deepMerge Json.obj(
        "@type"     -> Json.arr(Json.fromString("View"), Json.fromString("ElasticSearchView")),
        "_self"     -> Json.fromString(s"http://127.0.0.1:8080/v1/views/$organization/$project/nxv:$genUuid"),
        "_incoming" -> Json.fromString(s"http://127.0.0.1:8080/v1/views/$organization/$project/nxv:$genUuid/incoming"),
        "_outgoing" -> Json.fromString(s"http://127.0.0.1:8080/v1/views/$organization/$project/nxv:$genUuid/outgoing")
      )

    val resource =
      ResourceF.simpleF(id, view, created = user, updated = user, schema = viewRef, types = types)

    // format: off
    val resourceValue = Value(view, viewCtx.contextValue, view.replaceContext(viewCtx).deepMerge(Json.obj("@id" -> Json.fromString(id.value.asString))).asGraph(id.value).right.value)
    // format: on

    val resourceV =
      ResourceF.simpleV(id, resourceValue, created = user, updated = user, schema = viewRef, types = types)

    resources.fetchSchema(id) shouldReturn EitherT.rightT[Task, Rejection](viewRef)

    def mappingToJson(json: Json): Json = {
      val mapping = json.hcursor.get[String]("mapping").right.value
      parse(mapping).map(mJsonValue => json deepMerge Json.obj("mapping" -> mJsonValue)).getOrElse(json)
    }

    // format: off
    val otherEsView = ElasticSearchView(Json.obj(), Set.empty, Set.empty, None, false, true, true, projectRef, nxv.withSuffix("otherEs").value, genUUID, 1L, false)
    val defaultSQLView = SparqlView(Set.empty, Set.empty, None, true, true, projectRef, nxv.defaultSparqlIndex.value, genUuid, 1L, false)
    val otherSQLView = SparqlView(Set.empty, Set.empty, None, true, true, projectRef, nxv.withSuffix("otherSparql").value, genUUID, 1L, false)
    val aggEsView = AggregateElasticSearchView(Set(ViewRef(projectRef, nxv.defaultElasticSearchIndex.value), ViewRef(projectRef, nxv.withSuffix("otherEs").value)), projectRef, genUUID, nxv.withSuffix("agg").value, 1L, false)
    val aggSparqlView = AggregateSparqlView(Set(ViewRef(projectRef, nxv.defaultSparqlIndex.value), ViewRef(projectRef, nxv.withSuffix("otherSparql").value)), projectRef, genUUID, nxv.withSuffix("aggSparql").value, 1L, false)
    // format: on

    def endpoints(rev: Option[Long] = None, tag: Option[String] = None): List[String] = {
      val queryParam = (rev, tag) match {
        case (Some(r), _) => s"?rev=$r"
        case (_, Some(t)) => s"?tag=$t"
        case _            => ""
      }
      List(
        s"/v1/views/$organization/$project/$urlEncodedId$queryParam",
        s"/v1/resources/$organization/$project/view/$urlEncodedId$queryParam",
        s"/v1/resources/$organization/$project/_/$urlEncodedId$queryParam"
      )
    }
  }

  "The view routes" should {

    "create a view without @id" in new Context {
      views.create(view) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/views/$organization/$project", view) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
      }
      Post(s"/v1/resources/$organization/$project/view", view) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
      }
    }

    "create a view with @id" in new Context {
      views.create(id, view) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Put(s"/v1/views/$organization/$project/$urlEncodedId", view) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
      }
      Put(s"/v1/resources/$organization/$project/view/$urlEncodedId", view) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
      }
    }

    "update a view" in new Context {
      views.update(id, 1L, view) shouldReturn EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Put(endpoint, view) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }
    }

    "deprecate a view" in new Context {
      views.deprecate(id, 1L)(caller.subject) shouldReturn EitherT.rightT[Task, Rejection](resource)

      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Delete(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }
    }

    "tag a view" in new Context {
      val json = tag(2L, "one")

      tagsRes.create(id, 1L, json, viewRef) shouldReturn EitherT.rightT[Task, Rejection](resource)

      forAll(endpoints()) { endpoint =>
        Post(s"$endpoint/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }
    }

    "fetch latest revision of a view" in new Context {
      views.fetch(id) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = mappingToJson(resourceValue.graph.as[Json](viewCtx).right.value.removeKeys("@context"))
      forAll(endpoints()) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific revision of a view" in new Context {
      views.fetch(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = mappingToJson(resourceValue.graph.as[Json](viewCtx).right.value.removeKeys("@context"))
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific tag of a view" in new Context {
      views.fetch(id, "some") shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = mappingToJson(resourceValue.graph.as[Json](viewCtx).right.value.removeKeys("@context"))
      forAll(endpoints(tag = Some("some"))) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch latest revision of a views' source" in new Context {
      val expected = resourceV.value.source
      views.fetchSource(id) shouldReturn EitherT.rightT[Task, Rejection](expected)
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/source") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific revision of a views' source" in new Context {
      val expected = resourceV.value.source
      views.fetchSource(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](expected)
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/source?rev=1") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific tag of a views' source" in new Context {
      val expected = resourceV.value.source
      views.fetchSource(id, "some") shouldReturn EitherT.rightT[Task, Rejection](expected)
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/source?tag=some") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "list views" in new Context {

      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)), Some(sort.noSpaces))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(viewSchemaUri), deprecated = Some(false))
      val pagination = Pagination(20)
      views.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/views/$organization/$project?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/views/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }

      Get(s"/v1/resources/$organization/$project/view?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resources/$organization/$project/view?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }
    }

    "list views with after" in new Context {

      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val after      = Json.arr(Json.fromString("one"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)), Some(sort.noSpaces))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(viewSchemaUri), deprecated = Some(false))
      val pagination = Pagination(after, 20)
      views.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/views/$organization/$project?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/views/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }

      Get(s"/v1/resources/$organization/$project/view?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(
        oauthToken
      ) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resources/$organization/$project/view?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }
    }

    "search for resources on a ElasticSearchView" in new Context {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultElasticSearchIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultEsView)))

      when(
        elasticSearch.searchRaw(
          Eq(query),
          Eq(Set(s"kg_${defaultEsView.name}")),
          Eq(Uri.Query(Map("other" -> "value")))
        )(any[HttpClient[Task, Json]])
      ).thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$organization/$project/documents/_search?other=value", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }

      Post(s"/v1/resources/$organization/$project/view/documents/_search?other=value", query) ~> addCredentials(
        oauthToken
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "search for resources on a AggElasticSearchView" in new Context {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.withSuffix("agg").value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(aggEsView)))

      when(
        viewCache.getBy[ElasticSearchView](Eq(projectRef), Eq(nxv.withSuffix("otherEs").value))(
          any[Typeable[ElasticSearchView]]
        )
      ).thenReturn(Task.pure(Some(otherEsView)))

      when(
        viewCache.getBy[ElasticSearchView](Eq(projectRef), Eq(nxv.defaultElasticSearchIndex.value))(
          any[Typeable[ElasticSearchView]]
        )
      ).thenReturn(Task.pure(Some(defaultEsView)))

      when(
        elasticSearch.searchRaw(
          Eq(query),
          Eq(Set(s"kg_${defaultEsView.name}", s"kg_${otherEsView.name}")),
          Eq(Query())
        )(any[HttpClient[Task, Json]])
      ).thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$organization/$project/nxv:agg/_search", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }

      Post(s"/v1/resources/$organization/$project/view/nxv:agg/_search", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from ElasticSearch search" in new Context {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-error-response.json")
      val qp         = Uri.Query(Map("other" -> "value"))

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultElasticSearchIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultEsView)))

      when(
        elasticSearch.searchRaw(Eq(query), Eq(Set(s"kg_${defaultEsView.name}")), Eq(qp))(any[HttpClient[Task, Json]])
      ).thenReturn(Task.raiseError(ElasticSearchClientError(StatusCodes.BadRequest, esResponse.noSpaces)))

      Post(s"/v1/views/$organization/$project/documents/_search?other=value", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Json] shouldEqual esResponse
      }

      Post(s"/v1/resources/$organization/$project/view/documents/_search?other=value", query) ~> addCredentials(
        oauthToken
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from ElasticSearch Search when response is not JSON" in new Context {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"
      val qp         = Uri.Query(Map("other" -> "value"))

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultElasticSearchIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultEsView)))

      when(
        elasticSearch.searchRaw(Eq(query), Eq(Set(s"kg_${defaultEsView.name}")), Eq(qp))(any[HttpClient[Task, Json]])
      ).thenReturn(Task.raiseError(ElasticSearchClientError(StatusCodes.BadRequest, esResponse)))

      Post(s"/v1/views/$organization/$project/documents/_search?other=value", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual esResponse
      }

      Post(s"/v1/resources/$organization/$project/view/documents/_search?other=value", query) ~> addCredentials(
        oauthToken
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual esResponse
      }
    }

    "return 502 Bad Gateway when received unexpected response from ES" in new Context {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"
      val qp         = Uri.Query(Map("other" -> "value"))

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultElasticSearchIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultEsView)))

      when(
        elasticSearch.searchRaw(Eq(query), Eq(Set(s"kg_${defaultEsView.name}")), Eq(qp))(any[HttpClient[Task, Json]])
      ).thenReturn(Task.raiseError(ElasticUnexpectedError(StatusCodes.ImATeapot, esResponse)))

      Post(s"/v1/views/$organization/$project/documents/_search?other=value", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.InternalServerError
        responseAs[Error].tpe shouldEqual classNameOf[KgError.InternalError]
      }
    }

    "search for resources on a custom SparqlView" in new Context {
      val query  = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      val result = jsonContentOf("/search/sparql-query-result.json")

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultSparqlIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultSQLView)))

      when(sparql.copy(namespace = s"kg_${defaultSQLView.name}")).thenReturn(sparql)

      when(sparql.queryRaw(query)).thenReturn(Task.pure(result.as[SparqlResults].right.value))

      val httpEntity = HttpEntity(RdfMediaTypes.`application/sparql-query`, query)

      Post(s"/v1/views/$organization/$project/graph/sparql", httpEntity) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual result
      }

      Post(s"/v1/resources/$organization/$project/view/graph/sparql", httpEntity) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual result
      }
    }

    "return sparql error when sparql search has a client error" in new Context {
      val query = "SELECT ?s where {?s ?p ?o} LIMIT 10"

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultSparqlIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultSQLView)))

      when(sparql.copy(namespace = s"kg_${defaultSQLView.name}")).thenReturn(sparql)

      when(sparql.queryRaw(query)).thenReturn(Task.raiseError(SparqlClientError(StatusCodes.BadRequest, "some error")))

      val httpEntity = HttpEntity(RdfMediaTypes.`application/sparql-query`, query)

      Post(s"/v1/views/$organization/$project/graph/sparql", httpEntity) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual "some error"
      }

      Post(s"/v1/resources/$organization/$project/view/graph/sparql", httpEntity) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual "some error"
      }
    }

    "search for resources on a AggSparqlView" in new Context {
      val query     = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      val response1 = jsonContentOf("/search/sparql-query-result.json")
      val response2 = jsonContentOf("/search/sparql-query-result2.json")

      val sparql1 = mock[BlazegraphClient[Task]]
      val sparql2 = mock[BlazegraphClient[Task]]

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.defaultSparqlIndex.value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(defaultSQLView)))

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.withSuffix("aggSparql").value))(any[Typeable[View]]))
        .thenReturn(Task.pure(Some(aggSparqlView)))

      when(
        viewCache.getBy[SparqlView](Eq(projectRef), Eq(nxv.withSuffix("otherSparql").value))(any[Typeable[SparqlView]])
      ).thenReturn(Task.pure(Some(otherSQLView)))

      when(viewCache.getBy[SparqlView](Eq(projectRef), Eq(nxv.defaultSparqlIndex.value))(any[Typeable[SparqlView]]))
        .thenReturn(Task.pure(Some(defaultSQLView)))

      when(sparql.copy(namespace = s"kg_${defaultSQLView.name}")).thenReturn(sparql1)
      when(sparql.copy(namespace = s"kg_${otherSQLView.name}")).thenReturn(sparql2)
      when(sparql1.queryRaw(query)).thenReturn(Task.pure(response1.as[SparqlResults].right.value))
      when(sparql2.queryRaw(query)).thenReturn(Task.pure(response2.as[SparqlResults].right.value))

      val httpEntity = HttpEntity(RdfMediaTypes.`application/sparql-query`, query)
      val expected   = jsonContentOf("/search/sparql-query-result-combined.json")

      Post(s"/v1/views/$organization/$project/nxv:aggSparql/sparql", httpEntity) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        eventually {
          responseAs[Json] should equalIgnoreArrayOrder(expected)
        }
      }

      Post(s"/v1/resources/$organization/$project/view/nxv:aggSparql/sparql", httpEntity) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        eventually {
          responseAs[Json] should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "reject searching on a view that does not exists" in new Context {

      when(viewCache.getBy[View](Eq(projectRef), Eq(nxv.withSuffix("some").value))(any[Typeable[View]]))
        .thenReturn(Task.pure(None))

      Post(s"/v1/views/$organization/$project/nxv:some/_search?size=23&other=value", Json.obj()) ~> addCredentials(
        oauthToken
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[NotFound]
      }

      Post(s"/v1/resources/$organization/$project/view/nxv:some/_search?size=23&other=value", Json.obj()) ~> addCredentials(
        oauthToken
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[NotFound]
      }
    }
  }
}
