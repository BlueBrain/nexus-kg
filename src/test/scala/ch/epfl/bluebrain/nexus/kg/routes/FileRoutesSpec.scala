package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.Multipart.FormData
import akka.http.scaladsl.model.Multipart.FormData.BodyPart
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.data.EitherT
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes.`application/ld+json`
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
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.DiskStorage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.{Fetch, Link, Save}
import ch.epfl.bluebrain.nexus.kg.storage.{AkkaSource, Storage}
import ch.epfl.bluebrain.nexus.rdf.Iri.Path
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, RootedGraph}
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.mockito.matchers.MacroBasedMatchers
import org.scalactic.Equality
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.duration._

//noinspection TypeAnnotation
class FileRoutesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with ScalatestRouteTest
    with test.Resources
    with ScalaFutures
    with Randomness
    with IdiomaticMockito
    with ArgumentMatchersSugar
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
  private implicit val files         = mock[Files[Task]]
  private implicit val resources     = mock[Resources[Task]]
  private implicit val tagsRes       = mock[Tags[Task]]

  private implicit val cacheAgg = Caches(projectCache, viewCache, resolverCache, storageCache)

  private implicit val ec            = system.dispatcher
  private implicit val mt            = ActorMaterializer()
  private implicit val utClient      = untyped[Task]
  private implicit val qrClient      = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient    = withUnmarshaller[Task, Json]
  private implicit val sparql        = mock[BlazegraphClient[Task]]
  private implicit val elasticSearch = mock[ElasticSearchClient[Task]]
  private implicit val clients       = Clients()

  before {
    Mockito.reset(files)
  }

  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("files/write"))
  // format: off
  private val routes = Routes(resources, mock[Resolvers[Task]], mock[Views[Task]], mock[Storages[Task]], mock[Schemas[Task]], files, tagsRes, mock[ProjectViewCoordinator[Task]])
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

    val metadataRanges = Seq(`application/json`, `application/ld+json`)
    val storage        = DiskStorage.default(projectRef)
    storageCache.getDefault(projectRef) shouldReturn Task(Some(storage))

    val path = getClass.getResource("/resources/file.txt")
    val uuid = UUID.randomUUID
    val at1 = FileAttributes(uuid,
                             Uri(path.toString),
                             Uri.Path("file.txt"),
                             "file.txt",
                             `text/plain(UTF-8)`,
                             1024,
                             Digest("SHA-256", "digest1"))
    val content = genString()
    val source: Source[ByteString, Any] =
      Source.single(ByteString(content)).mapMaterializedValue[Any](v => v)
    val entity: HttpEntity.Strict = HttpEntity(ContentTypes.`text/plain(UTF-8)`, content)
    val multipartForm             = FormData(BodyPart.Strict("file", entity, Map("filename" -> "myFile.txt"))).toEntity()

    def fileResponse(): Json =
      response(fileRef) deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/files/$organization/$project/nxv:$genUuid")
      )

    val fileLink = jsonContentOf("/resources/file-link.json")
    val fileDesc = FileDescription("myFile.txt", `text/plain(UTF-8)`)

    implicit val ignoreUuid: Equality[FileDescription] = (a: FileDescription, b: Any) =>
      b match {
        case FileDescription(_, filename, mediaType) => a.filename == filename && a.mediaType == mediaType
        case _                                       => false
    }

    val resource =
      ResourceF.simpleF(id, Json.obj(), created = user, updated = user, schema = fileRef)

    val resourceV =
      ResourceF.simpleV(id,
                        Value(Json.obj(), Json.obj(), RootedGraph(IriNode(id.value), Graph())),
                        created = user,
                        updated = user,
                        schema = fileRef)

    resources.fetch(id, selfAsIri = false) shouldReturn EitherT.rightT[Task, Rejection](resourceV)
  }

  "The file routes" should {

    "create a file without @id" in new Context {
      files
        .create(eqTo(storage), eqTo(fileDesc), any[AkkaSource])(eqTo(caller.subject),
                                                                eqTo(finalProject),
                                                                any[Save[Task, AkkaSource]])
        .shouldReturn(EitherT.rightT[Task, Rejection](resource))

      Post(s"/v1/files/$organization/$project", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Post(s"/v1/resources/$organization/$project/file", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "create a file with @id" in new Context {
      files
        .create(eqTo(id), eqTo(storage), eqTo(fileDesc), any[AkkaSource])(eqTo(caller.subject),
                                                                          eqTo(finalProject),
                                                                          any[Save[Task, AkkaSource]])
        .shouldReturn(EitherT.rightT[Task, Rejection](resource))

      Put(s"/v1/files/$organization/$project/$urlEncodedId", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Put(s"/v1/resources/$organization/$project/file/$urlEncodedId", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "update a file" in new Context {
      files
        .update(eqTo(id), eqTo(storage), eqTo(1L), eqTo(fileDesc), any[AkkaSource])(eqTo(caller.subject),
                                                                                    any[Save[Task, AkkaSource]])
        .shouldReturn(EitherT.rightT[Task, Rejection](resource))

      Put(s"/v1/files/$organization/$project/$urlEncodedId?rev=1", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Put(s"/v1/resources/$organization/$project/file/$urlEncodedId?rev=1", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Put(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1", multipartForm) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "create a link without @id" in new Context {
      files
        .createLink(eqTo(storage), eqTo(fileLink))(eqTo(caller.subject), eqTo(finalProject), any[Link[Task]])
        .shouldReturn(EitherT.rightT[Task, Rejection](resource))

      Post(s"/v1/files/$organization/$project", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Post(s"/v1/resources/$organization/$project/file", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "create a link with @id" in new Context {
      files
        .createLink(eqTo(id), eqTo(storage), eqTo(fileLink))(eqTo(caller.subject), eqTo(finalProject), any[Link[Task]])
        .shouldReturn(EitherT.rightT[Task, Rejection](resource))

      Put(s"/v1/files/$organization/$project/$urlEncodedId", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Put(s"/v1/resources/$organization/$project/file/$urlEncodedId", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "update a link" in new Context {
      files
        .updateLink(eqTo(id), eqTo(storage), eqTo(1L), eqTo(fileLink))(eqTo(caller.subject), any[Link[Task]])
        .shouldReturn(EitherT.rightT[Task, Rejection](resource))

      Put(s"/v1/files/$organization/$project/$urlEncodedId?rev=1", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Put(s"/v1/resources/$organization/$project/file/$urlEncodedId?rev=1", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Put(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1", fileLink) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "deprecate a file" in new Context {
      files.deprecate(id, 1L) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Delete(s"/v1/files/$organization/$project/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Delete(s"/v1/resources/$organization/$project/file/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Delete(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "tag a file" in new Context {
      val json = tag(2L, "one")

      tagsRes.create(id, 1L, json, fileRef) shouldReturn EitherT.rightT[Task, Rejection](resource)

      Post(s"/v1/files/$organization/$project/$urlEncodedId/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Post(s"/v1/resources/$organization/$project/file/$urlEncodedId/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
      Post(s"/v1/resources/$organization/$project/_/$urlEncodedId/tags?rev=1", json) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.Created
        responseAs[Json] should equalIgnoreArrayOrder(fileResponse())
      }
    }

    "fetch latest revision of a file" in new Context {
      files
        .fetch[AkkaSource](eqTo(id))(any[Fetch[Task, AkkaSource]])
        .shouldReturn(EitherT.rightT[Task, Rejection]((storage: Storage, at1, source)))

      val accepted =
        List(Accept(MediaRanges.`*/*`), Accept(MediaRanges.`text/*`), Accept(`text/plain(UTF-8)`.mediaType))

      forAll(accepted) { accept =>
        Get(s"/v1/files/$organization/$project/$urlEncodedId") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
        Get(s"/v1/resources/$organization/$project/file/$urlEncodedId") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
        Get(s"/v1/resources/$organization/$project/_/$urlEncodedId") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
    }

    "fetch specific revision of a file" in new Context {
      files
        .fetch[AkkaSource](eqTo(id), eqTo(1L))(any[Fetch[Task, AkkaSource]])
        .shouldReturn(EitherT.rightT[Task, Rejection]((storage: Storage, at1, source)))

      val accepted =
        List(Accept(MediaRanges.`*/*`), Accept(MediaRanges.`text/*`), Accept(`text/plain(UTF-8)`.mediaType))

      forAll(accepted) { accept =>
        Get(s"/v1/files/$organization/$project/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
        Get(s"/v1/resources/$organization/$project/file/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
        Get(s"/v1/resources/$organization/$project/_/$urlEncodedId?rev=1") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
    }

    "fetch specific tag of a file" in new Context {
      files
        .fetch[AkkaSource](eqTo(id), eqTo("some"))(any[Fetch[Task, AkkaSource]])
        .shouldReturn(EitherT.rightT[Task, Rejection]((storage: Storage, at1, source)))

      val accepted =
        List(Accept(MediaRanges.`*/*`), Accept(MediaRanges.`text/*`), Accept(`text/plain(UTF-8)`.mediaType))

      forAll(accepted) { accept =>
        Get(s"/v1/files/$organization/$project/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
        Get(s"/v1/resources/$organization/$project/file/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
        Get(s"/v1/resources/$organization/$project/_/$urlEncodedId?tag=some") ~> addCredentials(oauthToken) ~> accept ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual `text/plain(UTF-8)`.value
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
    }

    "list files" in new Context {
      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem, Some(sort))))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(fileSchemaUri), deprecated = Some(false))
      val pagination = Pagination(20)
      files.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/files/$organization/$project?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/files/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }

      Get(s"/v1/resources/$organization/$project/file?deprecated=false") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/resources/$organization/$project/file?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }
    }

    "list files with after" in new Context {
      val resultElem = Json.obj("one" -> Json.fromString("two"))
      val after      = Json.arr(Json.fromString("one"))
      val sort       = Json.arr(Json.fromString("two"))
      val expectedList: JsonResults =
        UnscoredQueryResults(1L, List(UnscoredQueryResult(resultElem, Some(sort))))
      viewCache.getDefaultElasticSearch(projectRef) shouldReturn Task(Some(defaultEsView))
      val params     = SearchParams(schema = Some(fileSchemaUri), deprecated = Some(false))
      val pagination = Pagination(after, 20)
      files.list(Some(defaultEsView), params, pagination) shouldReturn Task(expectedList)

      val expected = Json.obj("_total" -> Json.fromLong(1L), "_results" -> Json.arr(resultElem))

      Get(s"/v1/files/$organization/$project?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(oauthToken) ~> Accept(
        MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/files/$organization/$project?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }

      Get(s"/v1/resources/$organization/$project/file?deprecated=false&after=%5B%22one%22%5D") ~> addCredentials(
        oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") shouldEqual expected.deepMerge(
          Json.obj(
            "_next" -> Json.fromString(
              s"http://example.com/v1/resources/$organization/$project/file?deprecated=false&after=%5B%22two%22%5D"
            )
          ))
      }
    }
  }
}
