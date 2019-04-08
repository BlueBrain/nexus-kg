package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import cats.data.EitherT
import cats.syntax.show._
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
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.Verify
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.{IdiomaticMockito, Mockito}
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest._
import org.mockito.Mockito.when
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.mockito.ArgumentMatchers.{eq => Eq}

import scala.concurrent.duration._

//noinspection TypeAnnotation
class StorageRoutesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with ScalatestRouteTest
    with test.Resources
    with ScalaFutures
    with Randomness
    with IdiomaticMockito
    with MacroBasedMatchers
    with BeforeAndAfter
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
  private implicit val storages      = mock[Storages[Task]]
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

  before {
    Mockito.reset(storages)
  }

  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("storages/write"))
  // format: off
  private val routes = Routes(resources, mock[Resolvers[Task]], mock[Views[Task]], storages, mock[Schemas[Task]], mock[Files[Task]], tagsRes, mock[ProjectViewCoordinator[Task]])
  // format: on

  //noinspection NameBooleanParameters
  abstract class Context(perms: Set[Permission] = manageResolver) extends RoutesFixtures {

    projectCache.getBy(label) shouldReturn Task.pure(Some(projectMeta))
    projectCache.getLabel(projectRef) shouldReturn Task.pure(Some(label))
    projectCache.get(projectRef) shouldReturn Task.pure(Some(projectMeta))

    iamClient.identities shouldReturn Task.pure(Caller(user, Set(Anonymous)))
    val acls = AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> perms)))
    iamClient.acls(any[Path], any[Boolean], any[Boolean])(any[Option[AuthToken]]) shouldReturn Task.pure(acls)
    projectCache.getProjectLabels(Set(projectRef)) shouldReturn Task.pure(Map(projectRef -> Some(label)))

    val storage = jsonContentOf("/storage/s3.json") deepMerge Json
      .obj("@id" -> Json.fromString(id.value.show))
      .addContext(storageCtxUri)
    val types = Set[AbsoluteIri](nxv.Storage, nxv.S3Storage)

    def storageResponse(): Json =
      response(storageRef) deepMerge Json.obj(
        "@type" -> Json.arr(Json.fromString("S3Storage"), Json.fromString("Storage")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/storages/$organization/$project/nxv:$genUuid")
      )

    val resource =
      ResourceF.simpleF(id, storage, created = user, updated = user, schema = storageRef, types = types)

    // format: off
    val resourceValue = Value(storage, storageCtx.contextValue, storage.replaceContext(storageCtx).deepMerge(Json.obj("@id" -> Json.fromString(id.value.asString))).asGraph(id.value).right.value)
    // format: on

    val resourceV =
      ResourceF.simpleV(id, resourceValue, created = user, updated = user, schema = storageRef, types = types)

    resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
  }

  "The storage routes" should {

    "create a storage without @id" in new Context {
      when(storages.create(Eq(projectMeta.base), Eq(storage))(Eq(caller.subject), any[Verify[Task]], Eq(finalProject)))
        .thenReturn(EitherT.rightT[Task, Rejection](resource))

      Post(s"/v1/storages/$organization/$project", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Post(s"/v1/resources/$organization/$project/storage", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
    }

    "create a storage with @id" in new Context {
      when(storages.create(Eq(id), Eq(storage))(Eq(caller.subject), any[Verify[Task]]))
        .thenReturn(EitherT.rightT[Task, Rejection](resource))

      Put(s"/v1/storages/$organization/$project/$urlEncodedId", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Put(s"/v1/resources/$organization/$project/storage/$urlEncodedId", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
    }

    "update a storage" in new Context {
      when(storages.update(Eq(id), Eq(1L), Eq(storage))(Eq(caller.subject), any[Verify[Task]]))
        .thenReturn(EitherT.rightT[Task, Rejection](resource))

      Put(s"/v1/storages/$organization/$project/$urlEncodedId?rev=1", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Put(s"/v1/resources/$organization/$project/storage/$urlEncodedId?rev=1", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Put(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1", storage) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }

    }

    "deprecate a storage" in new Context {
      storages.deprecate(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Delete(s"/v1/storages/$organization/$project/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Delete(s"/v1/resources/$organization/$project/storage/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Delete(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
    }

    "tag a storage" in new Context {
      val json = tag(2L, "one")

      tagsRes.create(id, 1L, json, storageRef) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/storages/$organization/$project/$urlEncodedId/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Post(s"/v1/resources/$organization/$project/storage/$urlEncodedId/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
      Post(s"/v1/resources/$organization/$project/_/$urlEncodedId/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
      }
    }

    "fetch latest revision of a storage" in new Context {
      storages.fetch(id) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](storageCtx).right.value.removeKeys("@context")

      Get(s"/v1/storages/$organization/$project/$urlEncodedId") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
      Get(s"/v1/resources/$organization/$project/storage/$urlEncodedId") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
      Get(s"/v1/resources/$organization/$project/_/$urlEncodedId") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
    }

    "fetch specific revision of a storage" in new Context {
      storages.fetch(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](storageCtx).right.value.removeKeys("@context")
      Get(s"/v1/storages/$organization/$project/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
    }

    "fetch specific tag of a storage" in new Context {
      storages.fetch(id, "some") shouldReturn EitherT.rightT[Task, Rejection](resourceV)

      val expected = resourceValue.graph.as[Json](storageCtx).right.value.removeKeys("@context")

      Get(s"/v1/storages/$organization/$project/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }

      Get(s"/v1/resources/$organization/$project/storage/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
    }

    "list storages" in new Context {

      val resultElem                = Json.obj("one" -> Json.fromString("two"))
      val expectedList: JsonResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(storageSchemaUri), deprecated = Some(false))
      val pagination = Pagination(0L, 20)
      storages.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/storages/$organization/$project?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected
      }

      Get(s"/v1/resources/$organization/$project/storage?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected
      }
    }
  }
}
