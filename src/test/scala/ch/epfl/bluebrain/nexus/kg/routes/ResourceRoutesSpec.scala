package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.{CirceEq, Randomness}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.Path
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.instances._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.IdiomaticMockito
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._

//noinspection TypeAnnotation
class ResourceRoutesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with BeforeAndAfter
    with ScalatestRouteTest
    with test.Resources
    with ScalaFutures
    with Randomness
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
  private implicit val tagsRes       = mock[Tags[Task]]

  private implicit val cacheAgg = Caches(projectCache, viewCache, resolverCache, storageCache)

  private implicit val ec            = system.dispatcher
  private implicit val mt            = ActorMaterializer()
  private implicit val utClient      = untyped[Task]
  private implicit val qrClient      = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient    = withUnmarshaller[Task, Json]
  private val sparql                 = mock[BlazegraphClient[Task]]
  private implicit val elasticSearch = mock[ElasticSearchClient[Task]]
  private implicit val clients       = Clients(sparql)

  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("resources/write"))
  // format: off
  private val routes = Routes(resources, mock[Resolvers[Task]], mock[Views[Task]], mock[Storages[Task]], mock[Schemas[Task]], mock[Files[Task]], tagsRes, mock[ProjectViewCoordinator[Task]])
  // format: on

  //noinspection NameBooleanParameters
  abstract class Context(perms: Set[Permission] = manageResolver) extends RoutesFixtures {

    projectCache.getBy(label) shouldReturn Task.pure(Some(projectMeta))
    projectCache.get(OrganizationRef(projectMeta.organizationUuid), ProjectRef(projectMeta.uuid)) shouldReturn Task
      .pure(Some(projectMeta))
    projectCache.get(OrganizationRef(projectMeta.organizationUuid), ProjectRef(projectMeta.uuid)) shouldReturn Task
      .pure(Some(projectMeta))
    projectCache.getBy(ProjectLabel(projectMeta.organizationUuid.toString, projectMeta.uuid.toString)) shouldReturn Task
      .pure(None)
    projectCache.get(projectRef) shouldReturn Task.pure(Some(projectMeta))

    iamClient.identities shouldReturn Task.pure(Caller(user, Set(Anonymous)))
    val acls = AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> perms)))
    iamClient.acls(any[Path], any[Boolean], any[Boolean])(any[Option[AuthToken]]) shouldReturn Task.pure(acls)
    projectCache.getProjectLabels(Set(projectRef)) shouldReturn Task.pure(Map(projectRef -> Some(label)))

    val json = Json.obj("key" -> Json.fromString(genString()))

    val defaultCtxValue = Json.obj("@base" -> Json.fromString("http://example.com/base/"),
                                   "@vocab" -> Json.fromString("http://example.com/voc/"))

    val jsonWithCtx = json deepMerge Json.obj("@context" -> defaultCtxValue)

    def resourceResponse(): Json =
      response(unconstrainedRef) deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resources/$organization/$project/_/nxv:$genUuid")
      )

    val resource =
      ResourceF.simpleF(id, jsonWithCtx, created = user, updated = user, schema = unconstrainedRef)

    // format: off
    val resourceValue = Value(jsonWithCtx, defaultCtxValue, jsonWithCtx.deepMerge(Json.obj("@id" -> Json.fromString(id.value.asString))).asGraph(id.value).right.value)
    // format: on

    val resourceV =
      ResourceF.simpleV(id, resourceValue, created = user, updated = user, schema = unconstrainedRef)

    resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
  }

  "The resources routes" should {

    "create a resource without @id" in new Context {
      resources.create(unconstrainedRef, json) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/resources/$organization/$project/resource", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }

      Post(s"/v1/resources/$organization/$project/_", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }

      Post(s"/v1/resources/$organization/$project", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
    }

    "create a resource with @id" in new Context {
      resources.create(id, unconstrainedRef, json) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Put(s"/v1/resources/$organization/$project/resource/$urlEncodedId", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
      Put(s"/v1/resources/$organization/$project/_/$urlEncodedId", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
    }

    "update a resource" in new Context {
      resources.update(id, 1L, unconstrainedRef, json) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Put(s"/v1/resources/$organization/$project/resource/$urlEncodedId?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
      Put(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
    }

    "deprecate a resource" in new Context {
      resources.deprecate(id, 1L, unconstrainedRef) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Delete(s"/v1/resources/$organization/$project/resource/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
      Delete(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
    }

    "tag a resource" in new Context {
      val tagJson = tag(2L, "one")

      tagsRes.create(id, 1L, tagJson, unconstrainedRef) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/resources/$organization/$project/resource/$urlEncodedId/tags?rev=1", tagJson) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
      Post(s"/v1/resources/$organization/$project/_/$urlEncodedId/tags?rev=1", tagJson) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resourceResponse())
      }
    }

    "fetch latest revision of a resource" in new Context {
      resources.fetch(id, unconstrainedRef) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected =
        resourceValue.graph
          .as[Json](Json.obj("@context" -> defaultCtxValue).appendContextOf(resourceCtx))
          .right
          .value
          .removeKeys("@context")

      val endpoints = List(
        s"/v1/resources/${projectMeta.organizationUuid}/${projectMeta.uuid}/_/$urlEncodedId",
        s"/v1/resources/$organization/$project/resource/$urlEncodedId",
        s"/v1/resources/$organization/$project/_/$urlEncodedId"
      )
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific revision of a resource" in new Context {
      resources.fetch(id, 1L, unconstrainedRef) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected =
        resourceValue.graph
          .as[Json](Json.obj("@context" -> defaultCtxValue).appendContextOf(resourceCtx))
          .right
          .value
          .removeKeys("@context")

      Get(s"/v1/resources/$organization/$project/resource/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
      Get(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
    }

    "fetch specific tag of a resource" in new Context {
      resources.fetch(id, "some", unconstrainedRef) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected =
        resourceValue.graph
          .as[Json](Json.obj("@context" -> defaultCtxValue).appendContextOf(resourceCtx))
          .right
          .value
          .removeKeys("@context")

      Get(s"/v1/resources/$organization/$project/resource/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
      Get(s"/v1/resources/$organization/$project/_/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
    }

    "list resources of a schema" in new Context {
      val resultElem                = Json.obj("one" -> Json.fromString("two"))
      val expectedList: JsonResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(unconstrainedSchemaUri), deprecated = Some(false))
      val pagination = Pagination(20)
      resources.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/resources/$organization/$project/resource?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected
      }
    }

    "list resources" in new Context {
      val resultElem                = Json.obj("one" -> Json.fromString("two"))
      val sort                      = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem, Some(sort))))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(deprecated = Some(false))
      val pagination = Pagination(20)
      resources.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/resources/$organization/$project?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/resources/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }

      Get(s"/v1/resources/$organization/$project/_?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/resources/$organization/$project/_?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }
    }

    "list resources with after" in new Context {
      val resultElem                = Json.obj("one" -> Json.fromString("two"))
      val after                     = Json.arr(Json.fromString("one"))
      val sort                      = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem, Some(sort))))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(deprecated = Some(false))
      val pagination = Pagination(after, 20)
      resources.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/resources/$organization/$project?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/resources/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }

      Get(s"/v1/resources/$organization/$project/_?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(
        oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/resources/$organization/$project/_?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }
    }
  }
}
