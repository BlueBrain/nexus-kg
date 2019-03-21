package ch.epfl.bluebrain.nexus.kg.routes

import java.nio.file.{Files, Paths}
import java.time.{Clock, Instant, ZoneId}
import java.util.UUID
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.MediaRanges._
import akka.http.scaladsl.model.headers.{Accept, OAuth2BearerToken}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Source
import akka.util.ByteString
import cats.data.EitherT
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticSearchFailure.{ElasticSearchClientError, ElasticUnexpectedError}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes._
import ch.epfl.bluebrain.nexus.commons.http.{HttpClient, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.commons.sparql.client.{BlazegraphClient, SparqlResults}
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlFailure.SparqlClientError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.{CirceEq, Randomness}
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.cache._
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{Schemas, Settings}
import ch.epfl.bluebrain.nexus.kg.indexing.View.{apply => _, _}
import ch.epfl.bluebrain.nexus.kg.indexing.{View => IndexingView}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes}
import ch.epfl.bluebrain.nexus.kg.storage.{AkkaSource, Storage}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.DiskStorage
import ch.epfl.bluebrain.nexus.kg.storage.Storage.StorageOperations.Fetch
import ch.epfl.bluebrain.nexus.kg.{Error, KgError, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri, RootedGraph}
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import io.circe.syntax._
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.ArgumentMatchers.{eq => mEq}
import org.mockito.IdiomaticMockito
import org.mockito.Mockito.when
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import shapeless.Typeable
import ch.epfl.bluebrain.nexus.kg.resources.{Tag => ResourceTag}

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
  private val iamUri             = appConfig.iam.publicIri.asUri
  private val adminUri           = appConfig.admin.publicIri.asUri
  private implicit val clock     = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  private implicit val adminClient      = mock[AdminClient[Task]]
  private implicit val iamClient        = mock[IamClient[Task]]
  private implicit val projectCache     = mock[ProjectCache[Task]]
  private implicit val viewCache        = mock[ViewCache[Task]]
  private implicit val resolverCache    = mock[ResolverCache[Task]]
  private implicit val storageCache     = mock[StorageCache[Task]]
  private implicit val resources        = mock[Resources[Task]]
  private implicit val projectViewCoord = mock[ProjectViewCoordinator[Task]]

  private implicit val cacheAgg = Caches(projectCache, viewCache, resolverCache, storageCache)

  private implicit val ec         = system.dispatcher
  private implicit val mt         = ActorMaterializer()
  private implicit val utClient   = untyped[Task]
  private implicit val qrClient   = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient = withUnmarshaller[Task, Json]
//  private implicit val sparqlResultsClient = withUnmarshaller[Task, SparqlResults]
  private val sparql                 = mock[BlazegraphClient[Task]]
  private implicit val elasticSearch = mock[ElasticSearchClient[Task]]
  private implicit val aclsCache     = mock[AclsCache[Task]]
  private implicit val clients       = Clients(sparql)

  private val user                              = User("dmontero", "realm")
  private implicit val subject: Subject         = user
  private implicit val token: Option[AuthToken] = Some(AuthToken("valid"))

  private val oauthToken     = OAuth2BearerToken("valid")
  private val manageRes      = Set(Permission.unsafe("resources/read"), Permission.unsafe("resources/write"))
  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("resolvers/write"))
  private val manageSchemas  = Set(Permission.unsafe("resources/read"), Permission.unsafe("schemas/write"))
  private val manageStorages = Set(Permission.unsafe("storages/read"), Permission.unsafe("storages/write"))
  private val manageFiles    = Set(Permission.unsafe("resources/read"), Permission.unsafe("files/write"))
  private val manageViews =
    Set(Permission.unsafe("resources/read"), Permission.unsafe("views/query"), Permission.unsafe("views/write"))
  private val routes = Routes(resources, projectViewCoord)

  //noinspection NameBooleanParameters
  abstract class Context(perms: Set[Permission]) {
    val organization = genString(length = 4)
    val project      = genString(length = 4)
    val defaultPrefixMapping: Map[String, AbsoluteIri] = Map(
      "resource"        -> Schemas.unconstrainedSchemaUri,
      "schema"          -> Schemas.shaclSchemaUri,
      "view"            -> Schemas.viewSchemaUri,
      "resolver"        -> Schemas.resolverSchemaUri,
      "file"            -> Schemas.fileSchemaUri,
      "storage"         -> Schemas.storageSchemaUri,
      "nxv"             -> nxv.base,
      "documents"       -> nxv.defaultElasticSearchIndex,
      "graph"           -> nxv.defaultSparqlIndex,
      "defaultResolver" -> nxv.defaultResolver,
      "defaultStorage"  -> nxv.defaultStorage
    )
    val mappings: Map[String, AbsoluteIri] =
      Map("nxv"      -> nxv.base,
          "resource" -> unconstrainedSchemaUri,
          "view"     -> viewSchemaUri,
          "resolver" -> resolverSchemaUri)
    val organizationMeta = Organization(genIri,
                                        organization,
                                        Some("description"),
                                        genUUID,
                                        1L,
                                        false,
                                        Instant.EPOCH,
                                        genIri,
                                        Instant.EPOCH,
                                        genIri)
    val organizationRef = OrganizationRef(organizationMeta.uuid)
    val genUuid         = genUUID
    val projectRef      = ProjectRef(genUUID)
    val id              = Id(projectRef, nxv.withSuffix(genUuid.toString))
    implicit val projectMeta =
      Project(
        id.value,
        project,
        organization,
        None,
        url"http://example.com/",
        nxv.base,
        mappings,
        projectRef.id,
        organizationRef.id,
        1L,
        false,
        Instant.EPOCH,
        genIri,
        Instant.EPOCH,
        genIri
      )

    val defaultEsView =
      ElasticSearchView(Json.obj(),
                        Set.empty,
                        None,
                        false,
                        true,
                        projectRef,
                        nxv.defaultElasticSearchIndex.value,
                        genUUID,
                        1L,
                        false)

    val otherEsView =
      ElasticSearchView(Json.obj(),
                        Set.empty,
                        None,
                        false,
                        true,
                        projectRef,
                        nxv.withSuffix("otherEs").value,
                        genUUID,
                        1L,
                        false)

    val defaultSQLView = SparqlView(projectRef, nxv.defaultSparqlIndex.value, genUuid, 1L, false)
    val otherSQLView   = SparqlView(projectRef, nxv.withSuffix("otherSparql").value, genUUID, 1L, false)

    val aggEsView = AggregateElasticSearchView(
      Set(ViewRef(projectRef, nxv.defaultElasticSearchIndex.value),
          ViewRef(projectRef, nxv.withSuffix("otherEs").value)),
      projectRef,
      genUUID,
      nxv.withSuffix("agg").value,
      1L,
      false
    )

    val aggSparqlView = AggregateSparqlView(
      Set(ViewRef(projectRef, nxv.defaultSparqlIndex.value), ViewRef(projectRef, nxv.withSuffix("otherSparql").value)),
      projectRef,
      genUUID,
      nxv.withSuffix("aggSparql").value,
      1L,
      false
    )

    private val label = ProjectLabel(organization, project)
    when(projectCache.getBy(label)).thenReturn(Task.pure(Some(projectMeta)))
    when(projectCache.getLabel(projectRef)).thenReturn(Task.pure(Some(label)))
    when(projectCache.get(projectRef)).thenReturn(Task.pure(Some(projectMeta)))
    when(viewCache.get(projectRef))
      .thenReturn(
        Task.pure(
          Set[IndexingView](defaultEsView, defaultSQLView, aggEsView, otherEsView, otherSQLView, aggSparqlView)))
    when(
      viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.defaultElasticSearchIndex.value))(
        any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(defaultEsView)))
    when(
      viewCache.getBy[ElasticSearchView](mEq(projectRef), mEq(nxv.defaultElasticSearchIndex.value))(
        any[Typeable[ElasticSearchView]]))
      .thenReturn(Task.pure(Some(defaultEsView)))
    when(
      viewCache.getBy[ElasticSearchView](mEq(projectRef), mEq(nxv.withSuffix("otherEs").value))(
        any[Typeable[ElasticSearchView]]))
      .thenReturn(Task.pure(Some(otherEsView)))
    when(
      viewCache.getBy[SparqlView](mEq(projectRef), mEq(nxv.withSuffix("otherSparql").value))(any[Typeable[SparqlView]]))
      .thenReturn(Task.pure(Some(otherSQLView)))
    when(viewCache.getBy[SparqlView](mEq(projectRef), mEq(nxv.defaultSparqlIndex.value))(any[Typeable[SparqlView]]))
      .thenReturn(Task.pure(Some(defaultSQLView)))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.withSuffix("agg").value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(aggEsView)))
    when(
      viewCache
        .getBy[IndexingView](mEq(projectRef), mEq(nxv.withSuffix("aggSparql").value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(aggSparqlView)))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.withSuffix("some").value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(None: Option[IndexingView]))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.defaultSparqlIndex.value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(defaultSQLView)))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(id.value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(None))

    when(iamClient.identities).thenReturn(Task.pure(Caller(user, Set(Anonymous))))
    val acls = AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> perms)))
    iamClient.acls(any[Path], any[Boolean], any[Boolean])(any[Option[AuthToken]]) shouldReturn Task.pure(acls)
    when(aclsCache.list).thenReturn(Task.pure(acls))
    when(projectCache.getProjectLabels(Set(projectRef))).thenReturn(Task.pure(Map(projectRef -> Some(label))))

    def schemaRef: Ref

    def response(deprecated: Boolean = false): Json =
      Json
        .obj(
          "@id"            -> Json.fromString(s"nxv:$genUuid"),
          "_constrainedBy" -> schemaRef.iri.asJson,
          "_createdAt"     -> Json.fromString(clock.instant().toString),
          "_createdBy"     -> Json.fromString(s"$iamUri/realms/${user.realm}/users/${user.subject}"),
          "_deprecated"    -> Json.fromBoolean(deprecated),
          "_rev"           -> Json.fromLong(1L),
          "_project"       -> Json.fromString(s"$adminUri/projects/$organization/$project"),
          "_updatedAt"     -> Json.fromString(clock.instant().toString),
          "_updatedBy"     -> Json.fromString(s"$iamUri/realms/${user.realm}/users/${user.subject}")
        )
        .addContext(resourceCtxUri)

    def listingResponse(): Json = Json.obj(
      "@context" -> Json.arr(
        Json.fromString("https://bluebrain.github.io/nexus/contexts/search.json"),
        Json.fromString("https://bluebrain.github.io/nexus/contexts/resource.json")
      ),
      "_total" -> Json.fromInt(5),
      "_results" -> Json.arr(
        (1 to 5).map(i => {
          val id = s"${appConfig.http.publicUri}/resources/$organization/$project/resource/resource:$i"
          jsonContentOf("/resources/es-metadata.json", Map(quote("{id}") -> id.toString))
        }): _*
      )
    )

    def projectMatcher = matches[Project] { argument =>
      argument == projectMeta.copy(apiMappings = projectMeta.apiMappings ++ defaultPrefixMapping)
    }
  }

  abstract class Ctx(perms: Set[Permission] = manageRes) extends Context(perms) {
    val ctx       = Json.obj("nxv" -> Json.fromString(nxv.base.show), "_rev" -> Json.fromString(nxv.rev.show))
    val schemaRef = Ref(unconstrainedSchemaUri)

    def ctxResponse: Json =
      response() deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resources/$organization/$project/_/nxv:$genUuid"))
  }

  abstract class File extends Context(manageFiles) {
    val ctx       = Json.obj("nxv" -> Json.fromString(nxv.base.show), "_rev" -> Json.fromString(nxv.rev.show))
    val schemaRef = Ref(fileSchemaUri)

    val metadataRanges: Seq[MediaRange] = List(`application/json`, `application/ld+json`)
    val storage                         = DiskStorage.default(projectRef)
    when(storageCache.getDefault(projectRef)).thenReturn(Task(Some(storage)))

  }

  abstract class Schema(perms: Set[Permission] = manageSchemas) extends Context(perms) {
    val schema    = jsonContentOf("/schemas/resolver.json")
    val schemaRef = Ref(shaclSchemaUri)

    def schemaResponse(deprecated: Boolean = false): Json =
      response(deprecated) deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/schemas/$organization/$project/nxv:$genUuid"))
  }

  abstract class StorageCtx(perms: Set[Permission] = manageStorages) extends Context(perms) {
    val storage = jsonContentOf("/storage/disk.json") deepMerge Json
      .obj("@id" -> Json.fromString(id.value.show))
    val schemaRef = Ref(storageSchemaUri)

    val types = Set[AbsoluteIri](nxv.Storage, nxv.DiskStorage)

    def storageResponse(deprecated: Boolean = false): Json =
      response(deprecated) deepMerge Json.obj(
        "@type" -> Json.arr(Json.fromString("DiskStorage"), Json.fromString("Storage")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/storages/$organization/$project/nxv:$genUuid")
      )
  }

  abstract class Resolver extends Context(manageResolver) {
    val resolver = jsonContentOf("/resolve/cross-project.json") deepMerge Json
      .obj("@id" -> Json.fromString(id.value.show))
      .addContext(resolverCtxUri)
    val types     = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)
    val schemaRef = Ref(resolverSchemaUri)

    def resolverResponse(): Json =
      response() deepMerge Json.obj(
        "@type" -> Json.arr(Json.fromString("CrossProject"), Json.fromString("Resolver")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resolvers/$organization/$project/nxv:$genUuid")
      )
  }

  abstract class Views(perms: Set[Permission] = manageViews) extends Context(perms) {
    val view = jsonContentOf("/view/elasticview.json")
      .removeKeys("_uuid")
      .deepMerge(Json.obj("@id" -> Json.fromString(id.value.show)))
      .addContext(viewCtxUri)

    val types     = Set[AbsoluteIri](nxv.View, nxv.ElasticSearchView, nxv.Alpha)
    val schemaRef = Ref(viewSchemaUri)

    def viewResponse(): Json =
      response() deepMerge Json.obj(
        "@type" -> Json
          .arr(Json.fromString("View"), Json.fromString("ElasticSearchView"), Json.fromString("Alpha")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/views/$organization/$project/nxv:$genUuid")
      )
  }

  "The routes" when {

    def metadata(account: String, project: String, i: Int): Json = {
      val id = url"${appConfig.http.publicUri.copy(
        path = appConfig.http.publicUri.path / "resources" / account / project / "resource" / s"resource:$i")}".value
      jsonContentOf("/resources/es-metadata.json", Map(quote("{id}") -> id.asString)) deepMerge Json.obj(
        "_original_source" -> Json.fromString(Json.obj("k" -> Json.fromInt(1)).noSpaces))
    }

    "performing operations on resolvers" should {

      "create a resolver without @id" in new Resolver {
        private val expected = ResourceF
          .simpleF(id, resolver, created = subject, updated = subject, schema = schemaRef, types = types)
        when(
          resources.create(mEq(projectMeta.base), mEq(schemaRef), mEq(resolver))(mEq(subject),
                                                                                 projectMatcher,
                                                                                 isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Post(s"/v1/resolvers/$organization/$project", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
        Post(s"/v1/resources/$organization/$project/resolver", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
      }
    }

    "performing operations on storages" should {

      "create a storage without @id" in new StorageCtx {
        private val expected = ResourceF
          .simpleF(id, storage, created = subject, updated = subject, schema = schemaRef, types = types)
        when(
          resources.create(mEq(projectMeta.base), mEq(schemaRef), mEq(storage.addContext(storageCtxUri)))(
            mEq(subject),
            projectMatcher,
            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        val endpoints = List(s"/v1/storages/$organization/$project", s"/v1/resources/$organization/$project/storage")

        forAll(endpoints) { endpoint =>
          Post(endpoint, storage) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.Created
            responseAs[Json] should equalIgnoreArrayOrder(storageResponse())
          }
        }
      }
    }

    "performing operations on views" should {

      "create a view without @id" in new Views {
        private val expected =
          ResourceF.simpleF(id, view, created = subject, updated = subject, schema = schemaRef, types = types)
        when(
          resources.create(mEq(projectMeta.base), mEq(schemaRef), matches[Json](_.removeKeys("_uuid") == view))(
            mEq(subject),
            projectMatcher,
            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Post(s"/v1/views/$organization/$project", view) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }

        Post(s"/v1/resources/$organization/$project/view", view) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }

      "reject when not enough permissions" in new Views(Set[Permission](Permission.unsafe("views/query"))) {
        val mapping = Json.obj("mapping" -> Json.obj("key" -> Json.fromString("value")))
        Put(s"/v1/views/$organization/$project/nxv:$genUuid", view deepMerge mapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Forbidden
          responseAs[Error].tpe shouldEqual "AuthorizationFailed"
        }
      }

      "create a view with @id" in new Views {
        val mappingValue           = Json.obj("key"                    -> Json.fromString("value"))
        val viewMapping            = view deepMerge Json.obj("mapping" -> mappingValue)
        val viewMappingTransformed = view deepMerge Json.obj("mapping" -> Json.fromString(mappingValue.noSpaces))
        private val expected =
          ResourceF.simpleF(id, viewMapping, created = subject, updated = subject, schema = schemaRef, types = types)
        when(
          resources.create(mEq(id), mEq(schemaRef), matches[Json](_.removeKeys("_uuid") == viewMappingTransformed))(
            mEq(subject),
            projectMatcher,
            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Put(s"/v1/views/$organization/$project/nxv:$genUuid", viewMapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }

        Put(s"/v1/resources/$organization/$project/view/nxv:$genUuid", viewMapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }

      "update a view" in new Views {
        val mappingValue           = Json.obj("key"                    -> Json.fromString("value"))
        val viewMapping            = view deepMerge Json.obj("mapping" -> mappingValue)
        val viewMappingTransformed = view deepMerge Json.obj("mapping" -> Json.fromString(mappingValue.noSpaces))
        private val expected =
          ResourceF.simpleF(id, viewMapping, created = subject, updated = subject, schema = schemaRef, types = types)
        when(
          resources.create(mEq(id), mEq(schemaRef), matches[Json](_.removeKeys("_uuid") == viewMappingTransformed))(
            mEq(subject),
            projectMatcher,
            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Put(s"/v1/views/$organization/$project/nxv:$genUuid", viewMapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          val uuid = responseAs[Json].hcursor.get[String]("_uuid").getOrElse("")

          val mappingValueUpdated = Json.obj("key2" -> Json.fromString("value2"))

          val uuidJson       = Json.obj("_uuid" -> Json.fromString(uuid))
          val expectedUpdate = expected.copy(value = view.deepMerge(uuidJson))
          val jsonUpdate     = view deepMerge Json.obj("mapping" -> mappingValueUpdated)
          val jsonUpdateTransformed = view deepMerge Json.obj(
            "mapping" -> Json.fromString(mappingValueUpdated.noSpaces))
          when(
            resources.update(mEq(id),
                             mEq(1L),
                             mEq(Latest(viewSchemaUri)),
                             matches[Json](_.removeKeys("_uuid") == jsonUpdateTransformed))(
              mEq(subject),
              projectMatcher,
              isA[AdditionalValidation[Task]]))
            .thenReturn(EitherT.rightT[Task, Rejection](expectedUpdate.copy(rev = 2L)))
          val resource = simpleV(expectedUpdate.map(_.appendContextOf(viewCtx)))
          when(resources.materializeWithMeta(expectedUpdate))
            .thenReturn(EitherT.rightT[Task, Rejection](resource))
          Put(s"/v1/views/$organization/$project/nxv:$genUuid?rev=1", jsonUpdate) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] should equalIgnoreArrayOrder(
              viewResponse() deepMerge Json.obj("_rev" -> Json.fromLong(2L)))
          }
        }
      }
    }

    "performing operations on resources" should {

      "create a resource without @id" in new Ctx {
        when(
          resources.create(mEq(projectMeta.base), mEq(schemaRef), mEq(ctx))(mEq(subject),
                                                                            projectMatcher,
                                                                            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, ctx, created = subject, updated = subject, schema = schemaRef)))
        val endpoints = List(s"/v1/resources/$organization/$project/resource",
                             s"/v1/resources/$organization/$project/_",
                             s"/v1/resources/$organization/$project")
        forAll(endpoints) { endpoint =>
          Post(endpoint, ctx) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.Created
            responseAs[Json] shouldEqual ctxResponse
          }
        }
      }

      "create a resource with @id" in new Ctx {
        when(
          resources.create(mEq(id), mEq(schemaRef), mEq(ctx))(mEq(subject),
                                                              projectMatcher,
                                                              isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, ctx, created = subject, updated = subject, schema = schemaRef)))

        Put(s"/v1/resources/$organization/$project/resource/nxv:$genUuid", ctx) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual ctxResponse
        }
      }

      "create a plain JSON resource without an @id" in new Ctx {
        val json = Json.obj("foo" -> Json.fromString("bar"))
        when(
          resources.create(mEq(projectMeta.base), mEq(schemaRef), mEq(json))(mEq(subject),
                                                                             projectMatcher,
                                                                             isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, json, created = subject, updated = subject, schema = schemaRef)))

        Post(s"/v1/resources/$organization/$project/resource", json) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual ctxResponse
        }
      }

      "create a plain JSON resource with an @id" in new Ctx {
        val json = Json.obj("@id" -> Json.fromString("foobar"), "foo" -> Json.fromString("bar"))
        // format: off
        when(resources.create(mEq(projectMeta.base), mEq(schemaRef), mEq(json))(mEq(subject), projectMatcher, isA[AdditionalValidation[Task]])).thenReturn(
          EitherT.rightT[Task, Rejection](ResourceF.simpleF(Id(projectRef, url"http://example.com/foobar"), json, created = subject, updated = subject, schema = schemaRef)))
        // format: on
        Post(s"/v1/resources/$organization/$project/resource", json) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual response().deepMerge(
            Json.obj(
              "@id"   -> Json.fromString("http://example.com/foobar"),
              "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resources/$organization/$project/_/foobar")
            ))
        }
      }

      "list resources constrained by a schema" in new Ctx {
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(schema = Some(unconstrainedSchemaUri))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticSearchClient[Task]]
          )
        ).thenReturn(Task.pure(
          UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(organization, project, i))))))
        Get(s"/v1/resources/$organization/$project/resource") ~> addCredentials(oauthToken) ~> addHeader(
          "Accept",
          "application/json") ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual listingResponse()
        }
      }

      "list views" in new Ctx(Set(read)) {
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(schema = Some(viewSchemaUri))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticSearchClient[Task]]
          )
        ).thenReturn(Task.pure(
          UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(organization, project, i))))))
        val endpoints = List(s"/v1/views/$organization/$project", s"/v1/resources/$organization/$project/view")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual listingResponse()
          }
        }
      }

      "list resolvers not deprecated" in new Ctx(Set(read)) {
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(deprecated = Some(false), schema = Some(resolverSchemaUri))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticSearchClient[Task]]
          )
        ).thenReturn(Task.pure(
          UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(organization, project, i))))))
        val endpoints = List(s"/v1/resolvers/$organization/$project?deprecated=false",
                             s"/v1/resources/$organization/$project/resolver?deprecated=false")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual listingResponse()
          }
        }
      }

      "list resources with types" in new Ctx {
        val listTypes =
          List[AbsoluteIri](nxv.withSuffix("other"), Iri.absolute(projectMeta.vocab.asString + "Some").right.value)
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(types = listTypes, createdBy = Some(url"http://example.com/user"))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticSearchClient[Task]]
          )
        ).thenReturn(Task.pure(
          UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(organization, project, i))))))

        Get(s"/v1/resources/$organization/$project?type=Some&type=nxv:other&createdBy=http%3A%2F%2Fexample.com%2Fuser") ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual listingResponse()
        }
      }
    }

    "list all resources" in new Ctx {
      when(
        resources.list(mEq(Some(defaultEsView)), mEq(SearchParams()), mEq(Pagination(0, 20)))(
          isA[HttpClient[Task, QueryResults[Json]]],
          isA[ElasticSearchClient[Task]]
        )
      ).thenReturn(Task.pure(
        UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(organization, project, i))))))

      val endpoints = List(s"/v1/resources/$organization/$project", s"/v1/resources/$organization/$project/_")
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual listingResponse()
        }
      }
    }

    "get a file resource" in new File {
      val resource = ResourceF.simpleF(id,
                                       Json.obj(),
                                       created = subject,
                                       updated = subject,
                                       schema = schemaRef,
                                       types = Set(nxv.File.value))
      val path    = getClass.getResource("/resources/file.txt")
      val uuid    = UUID.randomUUID
      val at1     = FileAttributes(uuid, Uri(path.toString), "file.txt", "text/plain", 1024, Digest("SHA-256", "digest1"))
      val content = new String(Files.readAllBytes(Paths.get(path.toURI)))
      val source: Source[ByteString, Any] =
        Source.single(ByteString(content)).mapMaterializedValue[Any](v => v)

      when(resources.fetch(mEq(id))).thenReturn(EitherT.rightT[Task, Rejection](resource))
      when(resources.fetchFile(mEq(id), any[Long], mEq(fileRef))(any[Fetch[Task, AkkaSource]]))
        .thenReturn(EitherT.rightT[Task, Rejection]((storage: Storage, at1, source)))

      val endpoints = List(
        s"/v1/files/$organization/$project/nxv:$genUuid?rev=1",
        s"/v1/resources/$organization/$project/file/nxv:$genUuid?rev=2",
        s"/v1/resources/$organization/$project/_/nxv:$genUuid?rev=3"
      )
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> Accept(`*/*`) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual "text/plain"
          header("Content-Disposition").value.value() shouldEqual """attachment; filename*=UTF-8''file.txt"""
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(`text/plain`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual "text/plain"
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`text/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual "text/plain"
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> Accept(MediaRanges.`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType.value shouldEqual "text/plain"
          responseEntity.dataBytes.runFold("")(_ ++ _.utf8String).futureValue shouldEqual content
        }
      }
    }

    "get the file metadata" in new File {
      val resource = ResourceF.simpleF(id, Json.obj(), created = subject, updated = subject, schema = schemaRef)
      val uuid     = UUID.randomUUID
      val at1 =
        FileAttributes(uuid, Uri("file:///some1"), "filename1.txt", "text/plain", 1024, Digest("SHA-256", "digest1"))
      val resourceV =
        simpleV(id, Json.obj(), created = subject, updated = subject, schema = schemaRef)
          .copy(file = Some(storage -> at1))
      when(resources.fetch(id, 1L, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](resource))
      when(resources.fetch(id, 1L)).thenReturn(EitherT.rightT[Task, Rejection](resource))
      when(resources.materializeWithMeta(mEq(resource), mEq(false))(projectMatcher)).thenReturn(
        EitherT.rightT[Task, Rejection](
          resourceV.copy(value = resourceV.value.copy(graph = RootedGraph(IriNode(id.value), resourceV.metadata())))))

      val json = jsonContentOf("/resources/file-metadata.json",
                               Map(quote("{account}") -> organizationMeta.label,
                                   quote("{proj}")    -> projectMeta.label,
                                   quote("{id}")      -> s"nxv:$genUuid"))

      forAll(metadataRanges) { range =>
        Get(s"/v1/files/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> Accept(range) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual json
        }
      }
      Get(s"/v1/files/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> Accept(
        metadataRanges: _*) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual json
      }
    }

    "reject getting a file with both tag and rev query params" in new File {
      Get(s"/v1/files/$organization/$project/nxv:$genUuid?rev=1&tag=2") ~> Accept(`*/*`) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].tpe shouldEqual "MalformedQueryParam"
      }
    }

    "reject getting a file metadata with both tag and rev query params" in new File {
      Get(s"/v1/files/$organization/$project/nxv:$genUuid?rev=1&tag=2") ~> addCredentials(oauthToken) ~> Accept(
        metadataRanges: _*) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].tpe shouldEqual "MalformedQueryParam"
      }
    }

    "reject getting a resource that does not exist" in new Ctx {
      when(resources.fetch(id)).thenReturn(EitherT.leftT[Task, Resource](NotFound(id.ref): Rejection))

      Get(s"/v1/resources/$organization/$project/_/nxv:$genUuid") ~> Accept(`application/json`) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[NotFound]
      }
    }

    "reject getting a revision that does not exist" in new Ctx {

      val resource = ResourceF.simpleF(id, Json.obj(), created = subject, updated = subject, schema = schemaRef)
      when(resources.fetch(id)).thenReturn(EitherT.rightT[Task, Rejection](resource))
      when(resources.fetch(id, 1L, schemaRef))
        .thenReturn(EitherT.leftT[Task, Resource](NotFound(id.ref, revOpt = Some(1L)): Rejection))

      forAll(
        List(s"/v1/resources/$organization/$project/_/nxv:$genUuid?rev=1",
             s"/v1/resources/$organization/$project/${schemaRef.iri}/nxv:$genUuid?rev=1")) { endpoint =>
        Get(endpoint) ~> Accept(`application/json`) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          responseAs[Error].tpe shouldEqual classNameOf[NotFound]
        }
      }

    }

    "reject getting a tag that does not exist" in new Ctx {

      val resource = ResourceF.simpleF(id, Json.obj(), created = subject, updated = subject, schema = schemaRef)
      when(resources.fetch(id)).thenReturn(EitherT.rightT[Task, Rejection](resource))
      when(resources.fetch(id, "one", schemaRef))
        .thenReturn(EitherT.leftT[Task, Resource](NotFound(id.ref, tagOpt = Some("one")): Rejection))

      forAll(
        List(s"/v1/resources/$organization/$project/_/nxv:$genUuid?tag=one",
             s"/v1/resources/$organization/$project/${schemaRef.iri}/nxv:$genUuid?tag=one")) { endpoint =>
        Get(endpoint) ~> Accept(`application/json`) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          responseAs[Error].tpe shouldEqual classNameOf[NotFound]
        }
      }
    }

    "reject getting tags of a resource that does not exist" in new Ctx {
      when(resources.fetchTags(id, schemaRef)).thenReturn(EitherT.leftT[Task, Tags](NotFound(id.ref): Rejection))

      Get(s"/v1/resources/$organization/$project/${schemaRef.iri}/nxv:$genUuid/tags") ~> Accept(`application/json`) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[NotFound]
      }
    }

    "search for resources on a ElasticSearchView" in new Views {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(
        elasticSearch.searchRaw(mEq(query),
                                mEq(Set(s"kg_${defaultEsView.name}")),
                                mEq(Uri.Query(Map("other" -> "value"))))(any[HttpClient[Task, Json]]))
        .thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticSearchIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "search for resources on a AggElasticSearchView" in new Views {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(
        elasticSearch.searchRaw(mEq(query),
                                mEq(Set(s"kg_${defaultEsView.name}", s"kg_${otherEsView.name}")),
                                mEq(Query()))(any[HttpClient[Task, Json]]))
        .thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$organization/$project/nxv:agg/_search", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from ElasticSearch Search " in new Views {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-error-response.json")

      when(
        elasticSearch.searchRaw(mEq(query),
                                mEq(Set(s"kg_${defaultEsView.name}")),
                                mEq(Uri.Query(Map("other" -> "value"))))(any[HttpClient[Task, Json]]))
        .thenReturn(Task.raiseError(ElasticSearchClientError(StatusCodes.BadRequest, esResponse.noSpaces)))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticSearchIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from ElasticSearch Search when response is not JSON" in new Views {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"

      when(
        elasticSearch.searchRaw(mEq(query),
                                mEq(Set(s"kg_${defaultEsView.name}")),
                                mEq(Uri.Query(Map("other" -> "value"))))(any[HttpClient[Task, Json]]))
        .thenReturn(Task.raiseError(ElasticSearchClientError(StatusCodes.BadRequest, esResponse)))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticSearchIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual esResponse
      }
    }

    "return 502 Bad Gateway when received unexpected response from ES" in new Views {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"

      when(
        elasticSearch.searchRaw(mEq(query),
                                mEq(Set(s"kg_${defaultEsView.name}")),
                                mEq(Uri.Query(Map("other" -> "value"))))(any[HttpClient[Task, Json]]))
        .thenReturn(Task.raiseError(ElasticUnexpectedError(StatusCodes.ImATeapot, esResponse)))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticSearchIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.InternalServerError
        responseAs[Error].tpe shouldEqual classNameOf[KgError.InternalError]
      }
    }

    "search for resources on a custom SparqlView" in new Views {
      val query  = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      val result = jsonContentOf("/search/sparql-query-result.json")
      when(sparql.copy(namespace = s"kg_${defaultSQLView.name}")).thenReturn(sparql)
      when(sparql.queryRaw(query)).thenReturn(Task.pure(result.as[SparqlResults].right.value))

      Post(
        s"/v1/views/$organization/$project/nxv:defaultSparqlIndex/sparql",
        HttpEntity(RdfMediaTypes.`application/sparql-query`, query)) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual result
      }
    }

    "return sparql error when sparql search has a client error" in new Views {
      val query = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      when(sparql.copy(namespace = s"kg_${defaultSQLView.name}")).thenReturn(sparql)
      when(sparql.queryRaw(query)).thenReturn(Task.raiseError(SparqlClientError(StatusCodes.BadRequest, "some error")))

      Post(
        s"/v1/views/$organization/$project/nxv:defaultSparqlIndex/sparql",
        HttpEntity(RdfMediaTypes.`application/sparql-query`, query)) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual "some error"
      }
    }

    "search for resources on a AggSparqlView" in new Views {
      val query     = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      val response1 = jsonContentOf("/search/sparql-query-result.json")
      val response2 = jsonContentOf("/search/sparql-query-result2.json")

      val sparql1 = mock[BlazegraphClient[Task]]
      val sparql2 = mock[BlazegraphClient[Task]]

      when(sparql.copy(namespace = s"kg_${defaultSQLView.name}")).thenReturn(sparql1)
      when(sparql.copy(namespace = s"kg_${otherSQLView.name}")).thenReturn(sparql2)
      when(sparql1.queryRaw(query)).thenReturn(Task.pure(response1.as[SparqlResults].right.value))
      when(sparql2.queryRaw(query)).thenReturn(Task.pure(response2.as[SparqlResults].right.value))

      Post(
        s"/v1/views/$organization/$project/nxv:aggSparql/sparql",
        HttpEntity(RdfMediaTypes.`application/sparql-query`, query)) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        eventually {
          responseAs[Json] should equalIgnoreArrayOrder(jsonContentOf("/search/sparql-query-result-combined.json"))
        }
      }
    }

    "reject searching on a view that does not exists" in new Views {
      Post(s"/v1/views/$organization/$project/nxv:some/_search?size=23&other=value", Json.obj()) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[NotFound]
      }
    }

    "performing operations on schemas" should {

      "create a schema without @id" in new Schema {
        when(
          resources.create(mEq(projectMeta.base), mEq(schemaRef), mEq(schema))(mEq(subject),
                                                                               projectMatcher,
                                                                               isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)))

        Post(s"/v1/schemas/$organization/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "create a schema with @id" in new Schema {
        when(
          resources.create(mEq(id), mEq(schemaRef), mEq(schema))(mEq(subject),
                                                                 projectMatcher,
                                                                 isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)))

        Put(s"/v1/schemas/$organization/$project/nxv:$genUuid", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "update a schema" in new Schema {
        when(
          resources
            .update(mEq(id), mEq(1L), mEq(schemaRef), mEq(schema))(mEq(subject),
                                                                   projectMatcher,
                                                                   isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)))

        Put(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "add a tag to a schema" in new Schema {
        val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(1L)).addContext(tagCtxUri)
        when(resources.tag(id, 1L, schemaRef, tag)).thenReturn(
          EitherT.rightT[Task, Rejection](
            ResourceF
              .simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)
              .copy(tags = Map("some" -> 2L))))

        Post(s"/v1/schemas/$organization/$project/nxv:$genUuid/tags?rev=1", tag) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      val set = Set(ResourceTag(1L, "name1"), ResourceTag(2L, "name2"))

      "listing tags for schemas" in new Schema {
        when(resources.fetchTags(id, 1L, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](set))
        val endpoints = List(s"/v1/schemas/$organization/$project/nxv:$genUuid/tags?rev=1",
                             s"/v1/resources/$organization/$project/schema/nxv:$genUuid/tags?rev=1")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] should equalIgnoreArrayOrder(jsonContentOf("/resources/tags.json"))
          }
        }
      }

      "listing tags for resolvers" in new Resolver {
        when(resources.fetchTags(id, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](set))
        val endpoints = List(s"/v1/resolvers/$organization/$project/nxv:$genUuid/tags",
                             s"/v1/resources/$organization/$project/resolver/nxv:$genUuid/tags")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] should equalIgnoreArrayOrder(jsonContentOf("/resources/tags.json"))
          }
        }
      }

      "listing tags for views" in new Views {
        when(resources.fetchTags(id, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](set))
        val endpoints = List(s"/v1/views/$organization/$project/nxv:$genUuid/tags",
                             s"/v1/resources/$organization/$project/view/nxv:$genUuid/tags")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] should equalIgnoreArrayOrder(jsonContentOf("/resources/tags.json"))
          }
        }
      }

      "get a resource with different output formats" in new Ctx {
        val json     = jsonContentOf("/resources/resource-embed.json")
        val id2      = Id(projectRef, url"https://bluebrain.github.io/nexus/vocabulary/me".value)
        val resource = ResourceF.simpleF(id2, json, created = subject, updated = subject, schema = schemaRef)
        val resourceV =
          resource.copy(
            value =
              Value(json,
                    json.contextValue,
                    RootedGraph(IriNode(id.value), json.asGraph(id.value).right.value ++ Graph(resource.metadata()))))

        when(resources.fetch(id2)).thenReturn(EitherT.rightT[Task, Rejection](resource))
        when(resources.fetch(id2, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](resource))
        when(resources.materializeWithMeta(mEq(resource), mEq(false))(projectMatcher))
          .thenReturn(EitherT.rightT[Task, Rejection](resourceV))

        Get(s"/v1/resources/$organization/$project/_/nxv:me") ~> Accept(`application/n-triples`) ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual OutputFormat.Triples.contentType
          val expected = contentOf("/resources/resource-triples.txt",
                                   Map(quote("{org}") -> organization, quote("{proj}") -> project))
          val string = responseEntity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).futureValue
          string.split("\n").sorted shouldEqual expected.split("\n").sorted
        }

        Get(s"/v1/resources/$organization/$project/_/nxv:me") ~> addCredentials(oauthToken) ~> Accept(`text/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual OutputFormat.DOT.contentType
          val expected =
            contentOf("/resources/resource-dot.txt", Map(quote("{org}") -> organization, quote("{proj}") -> project))
          val string = responseEntity.dataBytes.runFold(ByteString(""))(_ ++ _).map(_.utf8String).futureValue
          string.split("\n").sorted shouldEqual expected.split("\n").sorted
        }

        Get(s"/v1/resources/$organization/$project/_/nxv:me?format=expanded") ~> Accept(`*/*`) ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual `application/ld+json`.toContentType
          val expected = jsonContentOf("/resources/resource-expanded.json",
                                       Map(quote("{org}") -> organization, quote("{proj}") -> project))
          responseAs[Json] shouldEqual expected
        }

        Get(s"/v1/resources/$organization/$project/_/nxv:me?format=compacted") ~> Accept(`*/*`) ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual `application/ld+json`.toContentType
          val expected = jsonContentOf("/resources/resource-with-meta.json",
                                       Map(quote("{org}") -> organization, quote("{proj}") -> project))
          responseAs[Json] shouldEqual expected
        }
      }

      "deprecate a schema" in new Schema {
        val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(1L)).addContext(tagCtxUri)
        when(resources.deprecate(id, 1L, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, schema, deprecated = true, created = subject, updated = subject, schema = schemaRef)))

        Delete(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1", tag) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse(deprecated = true)
        }
      }

      "reject getting a schema with both tag and rev query params" in new Schema {
        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1&tag=2") ~> Accept(`*/*`) ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[Error].tpe shouldEqual "MalformedQueryParam"
        }
      }

      "get schema" ignore new Schema {
        val resource = ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)
        val temp     = simpleV(id, schema, created = subject, updated = subject, schema = schemaRef)
        val ctx      = schema.appendContextOf(shaclCtx)
        val resourceV =
          temp.copy(
            value = Value(schema,
                          ctx.contextValue,
                          RootedGraph(IriNode(id.value), ctx.asGraph(id.value).right.value ++ Graph(temp.metadata()))))

        when(resources.fetch(id, 1L, schemaRef)).thenReturn(EitherT.rightT[Task, Rejection](resource))
        when(resources.materializeWithMeta(resource)).thenReturn(EitherT.rightT[Task, Rejection](resourceV))

        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "reject getting a schema with wrong format" ignore new Schema {

        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1&format=wrong") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[Error].tpe shouldEqual "InvalidOutputFormat"
        }
      }

      "reject when creating a schema and the resources/create permissions are not present" in new Schema(Set(read)) {
        Post(s"/v1/schemas/$organization/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Forbidden
          responseAs[Error].tpe shouldEqual "AuthorizationFailed"
        }
      }

      "reject returning an appropriate error message when exception thrown" in new Schema {
        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> Accept(`*/*`) ~> routes ~> check {
          status shouldEqual StatusCodes.InternalServerError
          responseAs[Error].tpe shouldEqual classNameOf[KgError.InternalError]
        }
      }

      "reject when the resource is not available" in new Schema {
        Get(s"/v1/other/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          responseAs[Error].tpe shouldEqual "NotFound"
        }
      }
    }
  }
}
