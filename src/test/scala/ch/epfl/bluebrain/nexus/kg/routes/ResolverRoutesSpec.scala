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
import ch.epfl.bluebrain.nexus.commons.test.CirceEq
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
import ch.epfl.bluebrain.nexus.kg.urlEncode
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
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
class ResolverRoutesSpec
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
  private implicit val resolvers     = mock[Resolvers[Task]]
  private implicit val resources     = mock[Resources[Task]]
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

  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("resolvers/write"))
  // format: off
  private val routes = Routes(resources, resolvers, mock[Views[Task]], mock[Storages[Task]], mock[Schemas[Task]], mock[Files[Task]], tagsRes, mock[ProjectViewCoordinator[Task]])
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

    val resolver = jsonContentOf("/resolve/cross-project.json") deepMerge Json
      .obj("@id" -> Json.fromString(id.value.show))
      .addContext(resolverCtxUri)
    val types = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)

    def resolverResponse(): Json =
      response(resolverRef) deepMerge Json.obj(
        "@type" -> Json.arr(Json.fromString("CrossProject"), Json.fromString("Resolver")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resolvers/$organization/$project/nxv:$genUuid"),
        "_incoming" -> Json.fromString(
          s"http://127.0.0.1:8080/v1/resolvers/$organization/$project/nxv:$genUuid/incoming"
        ),
        "_outgoing" -> Json.fromString(
          s"http://127.0.0.1:8080/v1/resolvers/$organization/$project/nxv:$genUuid/outgoing"
        )
      )

    val resource =
      ResourceF.simpleF(id, resolver, created = user, updated = user, schema = resolverRef, types = types)

    // format: off
    val resourceValue = Value(resolver, resolverCtx.contextValue, resolver.replaceContext(resolverCtx).deepMerge(Json.obj("@id" -> Json.fromString(id.value.asString))).asGraph(id.value).right.value)
    // format: on

    val resourceV =
      ResourceF.simpleV(id, resourceValue, created = user, updated = user, schema = resolverRef, types = types)

    resources.fetchSchema(id) shouldReturn EitherT.rightT[Task, Rejection](resolverRef)

    def endpoints(rev: Option[Long] = None, tag: Option[String] = None): List[String] = {
      val queryParam = (rev, tag) match {
        case (Some(r), _) => s"?rev=$r"
        case (_, Some(t)) => s"?tag=$t"
        case _            => ""
      }
      List(
        s"/v1/resolvers/$organization/$project/$urlEncodedId$queryParam",
        s"/v1/resources/$organization/$project/resolver/$urlEncodedId$queryParam",
        s"/v1/resources/$organization/$project/_/$urlEncodedId$queryParam"
      )
    }
  }

  "The resolver routes" should {

    "create a resolver without @id" in new Context {
      resolvers.create(resolver) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/resolvers/$organization/$project", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
      }
      Post(s"/v1/resources/$organization/$project/resolver", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
      }
    }

    "create a resolver with @id" in new Context {
      resolvers.create(id, resolver) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Put(s"/v1/resolvers/$organization/$project/$urlEncodedId", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
      }
      Put(s"/v1/resources/$organization/$project/resolver/$urlEncodedId", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
      }
    }

    "update a resolver" in new Context {
      resolvers.update(id, 1L, resolver) shouldReturn EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Put(endpoint, resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
      }
    }

    "deprecate a resolver" in new Context {
      resolvers.deprecate(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Delete(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
      }
    }

    "tag a resolver" in new Context {
      val json = tag(2L, "one")
      tagsRes.create(id, 1L, json, resolverRef) shouldReturn EitherT.rightT[Task, Rejection](resource)
      forAll(endpoints()) { endpoint =>
        Post(s"$endpoint/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
      }
    }

    "fetch latest revision of a resolver" in new Context {
      resolvers.fetch(id) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](resolverCtx).right.value.removeKeys("@context")
      forAll(endpoints()) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific revision of a resolver" in new Context {
      resolvers.fetch(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](resolverCtx).right.value.removeKeys("@context")
      forAll(endpoints(rev = Some(1L))) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch specific tag of a resolver" in new Context {
      resolvers.fetch(id, "some") shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](resolverCtx).right.value.removeKeys("@context")
      forAll(endpoints(tag = Some("some"))) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch resolved resource for a concrete resolver" in new Context {
      val resourceId = genIri
      resolvers.resolve(id, resourceId) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](resolverCtx).right.value.removeKeys("@context")
      forAll(endpoints()) { endpoint =>
        Get(s"$endpoint/${urlEncode(resourceId)}") ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
        }
      }
    }

    "fetch resolved resource" in new Context {
      val resourceId = genIri
      resolvers.resolve(resourceId, 1L) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
      val expected = resourceValue.graph.as[Json](resolverCtx).right.value.removeKeys("@context")

      Get(s"/v1/resolvers/$organization/$project/_/${urlEncode(resourceId)}?rev=1") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(expected)
      }
    }

    "list resolvers" in new Context {
      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)), Some(sort.noSpaces))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(resolverSchemaUri), deprecated = Some(false))
      val pagination = Pagination(20)
      resolvers.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/resolvers/$organization/$project?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resolvers/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }

      Get(s"/v1/resources/$organization/$project/resolver?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resources/$organization/$project/resolver?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }
    }

    "list resolvers with after" in new Context {
      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val after      = Json.arr(Json.fromString("one"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem)), Some(sort.noSpaces))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(resolverSchemaUri), deprecated = Some(false))
      val pagination = Pagination(after, 20)
      resolvers.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/resolvers/$organization/$project?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`
      ) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resolvers/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }

      Get(s"/v1/resources/$organization/$project/resolver?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(
        oauthToken
      ) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://127.0.0.1:8080/v1/resources/$organization/$project/resolver?deprecated=false&after=%5B%22two%22%5D"
            )
          )
        )
      }
    }
  }
}
