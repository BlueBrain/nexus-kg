package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.data.EitherT
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.search.{Pagination, QueryResults, Sort, SortList}
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.{CirceEq, EitherValues}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.archives.ArchiveCache
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.implicits._
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Inspectors, OptionValues}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

//noinspection TypeAnnotation
class SchemaRoutesSpec
    extends AnyWordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with ScalatestRouteTest
    with test.Resources
    with ScalaFutures
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with BeforeAndAfter
    with TestHelper
    with Inspectors
    with CirceEq
    with Eventually {

  // required to be able to spin up the routes (CassandraClusterHealth depends on a cassandra session)
  override def testConfig: Config =
    ConfigFactory.load("test-no-inmemory.conf").withFallback(ConfigFactory.load()).resolve()

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.second, 15.milliseconds)

  private implicit val appConfig = Settings(system).appConfig
  private implicit val clock     = Clock.fixed(Instant.EPOCH, ZoneId.systemDefault())

  private implicit val adminClient   = mock[AdminClient[Task]]
  private implicit val iamClient     = mock[IamClient[Task]]
  private implicit val projectCache  = mock[ProjectCache[Task]]
  private implicit val viewCache     = mock[ViewCache[Task]]
  private implicit val resolverCache = mock[ResolverCache[Task]]
  private implicit val storageCache  = mock[StorageCache[Task]]
  private implicit val schemas       = mock[Schemas[Task]]
  private implicit val resources     = mock[Resources[Task]]
  private implicit val tagsRes       = mock[Tags[Task]]
  private implicit val initializer   = mock[ProjectInitializer[Task]]

  private implicit val cacheAgg =
    Caches(projectCache, viewCache, resolverCache, storageCache, mock[ArchiveCache[Task]])

  private implicit val ec            = system.dispatcher
  private implicit val utClient      = untyped[Task]
  private implicit val qrClient      = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient    = withUnmarshaller[Task, Json]
  private implicit val sparql        = mock[BlazegraphClient[Task]]
  private implicit val elasticSearch = mock[ElasticSearchClient[Task]]
  private implicit val storageClient = mock[StorageClient[Task]]
  private implicit val clients       = Clients()
  private val sortList               = SortList(List(Sort(nxv.createdAt.prefix), Sort("@id")))

  before {
    Mockito.reset(schemas)
  }

  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("schemas/write"))
  // format: off
  private val routes = Routes(resources, mock[Resolvers[Task]], mock[Views[Task]], mock[Storages[Task]], schemas, mock[Files[Task]], mock[Archives[Task]], tagsRes, mock[ProjectViewCoordinator[Task]])
  // format: on

  //noinspection NameBooleanParameters
  abstract class Context(perms: Set[Permission] = manageResolver) extends RoutesFixtures {

    projectCache.get(label) shouldReturn Task.pure(Some(projectMeta))
    projectCache.getLabel(projectRef) shouldReturn Task.pure(Some(label))
    projectCache.get(projectRef) shouldReturn Task.pure(Some(projectMeta))

    iamClient.identities shouldReturn Task.pure(Caller(user, Set(Anonymous)))
    val acls = AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> perms)))
    iamClient.acls(any[Path], any[Boolean], any[Boolean])(any[Option[AuthToken]]) shouldReturn Task.pure(acls)

    val schema = jsonContentOf("/schemas/simple.json") deepMerge Json
      .obj("@id" -> Json.fromString(id.value.show))
      .addContext(shaclCtxUri)
    val types = Set[AbsoluteIri](nxv.Schema)

    def schemaResponse(): Json =
      response(shaclRef) deepMerge Json.obj(
        "@type" -> Json.fromString("Schema"),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/schemas/$organization/$project/nxv:$genUuid"),
        "_incoming" -> Json.fromString(
          s"http://127.0.0.1:8080/v1/schemas/$organization/$project/nxv:$genUuid/incoming"
        ),
        "_outgoing" -> Json.fromString(
          s"http://127.0.0.1:8080/v1/schemas/$organization/$project/nxv:$genUuid/outgoing"
        )
      )

    val resource =
      ResourceF.simpleF(id, schema, created = user, updated = user, schema = shaclRef, types = types)

    // format: off
    val resourceValue = Value(schema, shaclCtx.contextValue, schema.replaceContext(shaclCtx).deepMerge(Json.obj("@id" -> Json.fromString(id.value.asString))).toGraph(id.value).rightValue)
    // format: on

    val resourceV =
      ResourceF.simpleV(id, resourceValue, created = user, updated = user, schema = shaclRef, types = types)

    resources.fetchSchema(id) shouldReturn EitherT.rightT[Task, Rejection](shaclRef)

    def endpoints(rev: Option[Long] = None, tag: Option[String] = None): List[String] = {
      val queryParam = (rev, tag) match {
        case (Some(r), _) => s"?rev=$r"
        case (_, Some(t)) => s"?tag=$t"
        case _            => ""
      }
      List(
        s"/v1/schemas/$organization/$project/$urlEncodedId$queryParam",
        s"/v1/resources/$organization/$project/schema/$urlEncodedId$queryParam",
        s"/v1/resources/$organization/$project/_/$urlEncodedId$queryParam"
      )
    }
  }

  "The schema routes" should {

    "create a schema without @id" in new Context {
      schemas.create(eqTo(schema))(eqTo(caller.subject), eqTo(finalProject)) shouldReturn
        EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/schemas/$organization/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
      }
      Post(s"/v1/resources/$organization/$project/schema", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
      }
    }

    "create a schema with @id" in new Context {
      schemas.create(eqTo(id), eqTo(schema))(eqTo(caller.subject), eqTo(finalProject)) shouldReturn
        EitherT.rightT[Task, Rejection](resource)

      Put(s"/v1/schemas/$organization/$project/$urlEncodedId", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
      }
      Put(s"/v1/resources/$organization/$project/schema/$urlEncodedId", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
      }
    }

    "update a schema" in new Context {
      schemas.update(eqTo(id), eqTo(1L), eqTo(schema))(eqTo(caller.subject), eqTo(finalProject)) shouldReturn
        EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Put(endpoint, schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
        }
      }
    }

    "deprecate a schema" in new Context {
      schemas.deprecate(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Delete(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
        }
      }
    }

    "tag a schema" in new Context {
      val json = tag(2L, "one")
      tagsRes.create(id, 1L, json, shaclRef) shouldReturn EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints()) { endpoint =>
        Post(s"$endpoint/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(schemaResponse())
        }
      }
    }

    "fetch latest revision of a schema" in new Context {
      schemas.fetch(id) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.toJson(shaclCtx).rightValue.removeNestedKeys("@context") deepMerge Json.obj(
        "@type" -> Json.fromString("Schema")
      )
      forAll(endpoints()) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeNestedKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific revision of a schema" in new Context {
      schemas.fetch(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.toJson(shaclCtx).rightValue.removeNestedKeys("@context") deepMerge Json.obj(
        "@type" -> Json.fromString("Schema")
      )
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeNestedKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific tag of a schema" in new Context {
      schemas.fetch(id, "some") shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.toJson(shaclCtx).rightValue.removeNestedKeys("@context") deepMerge Json.obj(
        "@type" -> Json.fromString("Schema")
      )
      forAll(endpoints(tag = Some("some"))) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeNestedKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch latest revision of a schemas' source" in new Context {
      val expected = resourceV.value.source
      schemas.fetchSource(id) shouldReturn EitherT.rightT[Task, Rejection](expected)
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/source") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific revision of a schemas' source" in new Context {
      val expected = resourceV.value.source
      schemas.fetchSource(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](expected)
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/source?rev=1") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific tag of a schemas' source" in new Context {
      val expected = resourceV.value.source
      schemas.fetchSource(id, "some") shouldReturn EitherT.rightT[Task, Rejection](expected)
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/source?tag=some") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "list schemas" in new Context {

      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)), Some(sort.noSpaces))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(shaclSchemaUri), deprecated = Some(false), sort = sortList)
      val pagination = Pagination(20)
      schemas.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/schemas/$organization/$project?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeNestedKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/schemas/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }

      Get(s"/v1/resources/$organization/$project/schema?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeNestedKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resources/$organization/$project/schema?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }
    }

    "list schemas with after" in new Context {

      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val after      = Json.arr(Json.fromString("one"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)), Some(sort.noSpaces))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(shaclSchemaUri), deprecated = Some(false), sort = sortList)
      val pagination = Pagination(after, 20)
      schemas.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/schemas/$organization/$project?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeNestedKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/schemas/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }

      Get(s"/v1/resources/$organization/$project/schema?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(
        oauthToken
      ) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeNestedKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resources/$organization/$project/schema?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }
    }
  }
}
