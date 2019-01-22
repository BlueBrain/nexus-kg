package ch.epfl.bluebrain.nexus.kg.routes

import java.nio.file.{Files, Paths}
import java.time.{Clock, Instant, ZoneId}
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.MediaTypes.{`application/json`, `text/plain`}
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, OAuth2BearerToken}
import akka.http.scaladsl.server.Directives.handleRejections
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import akka.util.ByteString
import cats.data.{EitherT, OptionT}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.{ElasticClientError, ElasticUnexpectedError}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes.`application/ld+json`
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.http.{HttpClient, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.async._
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{Contexts, Schemas, Settings}
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateElasticView, ElasticView, SparqlView, ViewRef}
import ch.epfl.bluebrain.nexus.kg.indexing.{View => IndexingView}
import ch.epfl.bluebrain.nexus.kg.marshallers.RejectionHandling
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes}
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore
import ch.epfl.bluebrain.nexus.kg.resources.file.FileStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.{Error, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import ch.epfl.bluebrain.nexus.rdf.{Graph, Iri}
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.ArgumentMatchers.{any, eq => mEq}
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import shapeless.Typeable

//noinspection TypeAnnotation
class ResourceRoutesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with BeforeAndAfter
    with ScalatestRouteTest
    with test.Resources
    with ScalaFutures
    with Randomness
    with TestHelper
    with Inspectors {

  private implicit val appConfig = Settings(system).appConfig
  private val iamUri             = appConfig.iam.baseUri
  private val adminUri           = appConfig.admin.baseUri
  private implicit val clock     = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  private implicit val adminClient   = mock[AdminClient[Task]]
  private implicit val iamClient     = mock[IamClient[Task]]
  private implicit val projectCache  = mock[ProjectCache[Task]]
  private implicit val viewCache     = mock[ViewCache[Task]]
  private implicit val resolverCache = mock[ResolverCache[Task]]
  private implicit val store         = mock[FileStore[Task, AkkaIn, AkkaOut]]
  private implicit val resources     = mock[Resources[Task]]

  private implicit val cacheAgg = Caches(projectCache, viewCache, resolverCache)

  private implicit val ec         = system.dispatcher
  private implicit val mt         = ActorMaterializer()
  private implicit val utClient   = untyped[Task]
  private implicit val qrClient   = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient = withUnmarshaller[Task, Json]
  private val sparql              = mock[BlazegraphClient[Task]]
  private implicit val elastic    = mock[ElasticClient[Task]]
  private implicit val aclsOps    = mock[AclsOps]
  private implicit val clients    = Clients(sparql)

  private val user                              = User("dmontero", "realm")
  private implicit val subject: Subject         = user
  private implicit val token: Option[AuthToken] = Some(AuthToken("valid"))

  private val oauthToken     = OAuth2BearerToken("valid")
  private val read           = Set(Permission.unsafe("resources/read"))
  private val manageRes      = Set(Permission.unsafe("resources/read"), Permission.unsafe("resources/write"))
  private val manageResolver = Set(Permission.unsafe("resources/read"), Permission.unsafe("resolvers/write"))
  private val manageSchemas  = Set(Permission.unsafe("resources/read"), Permission.unsafe("schemas/write"))
  private val manageFiles    = Set(Permission.unsafe("resources/read"), Permission.unsafe("files/write"))
  private val manageViews =
    Set(Permission.unsafe("resources/read"), Permission.unsafe("views/query"), Permission.unsafe("views/write"))
  private val routes = (handleRejections(RejectionHandling().withFallback(RejectionHandling.notFound))) {
    Routes(resources)
  }

  abstract class Context(perms: Set[Permission]) {
    val organization = genString(length = 4)
    val project      = genString(length = 4)
    val defaultPrefixMapping: Map[String, AbsoluteIri] = Map(
      "nxc"       -> Contexts.base,
      "nxs"       -> Schemas.base,
      "resource"  -> Schemas.resourceSchemaUri,
      "schema"    -> Schemas.shaclSchemaUri,
      "view"      -> Schemas.viewSchemaUri,
      "resolver"  -> Schemas.resolverSchemaUri,
      "file"      -> Schemas.fileSchemaUri,
      "nxv"       -> nxv.base,
      "documents" -> nxv.defaultElasticIndex,
      "graph"     -> nxv.defaultSparqlIndex,
      "defaultResolver"   -> nxv.defaultResolver
    )
    val mappings =
      Map("nxv" -> nxv.base, "resource" -> resourceSchemaUri, "view" -> viewSchemaUri, "resolver" -> resolverSchemaUri)
    implicit val projectMeta =
      Project(genIri,
              project,
              organization,
              None,
              nxv.projects,
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

    val organizationMeta = Organization(genIri,
                                        organization,
                                        "description",
                                        genUUID,
                                        1L,
                                        false,
                                        Instant.EPOCH,
                                        genIri,
                                        Instant.EPOCH,
                                        genIri)
    val projectRef      = ProjectRef(projectMeta.uuid)
    val organizationRef = OrganizationRef(organizationMeta.uuid)
    val genUuid         = genUUID
    val id              = Id(projectRef, nxv.withSuffix(genUuid.toString))
    val defaultEsView =
      ElasticView(Json.obj(),
                  Set.empty,
                  None,
                  false,
                  true,
                  projectRef,
                  nxv.defaultElasticIndex.value,
                  genUUID,
                  1L,
                  false)

    val otherEsView =
      ElasticView(Json.obj(),
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

    val aggView = AggregateElasticView(
      Set(ViewRef(projectRef, nxv.defaultElasticIndex.value), ViewRef(projectRef, nxv.withSuffix("otherEs").value)),
      projectRef,
      genUUID,
      nxv.withSuffix("agg").value,
      1L,
      false
    )

    private val label = ProjectLabel(organization, project)
    when(projectCache.getBy(label)).thenReturn(Task.pure(Some(projectMeta)))
    when(projectCache.getLabel(projectRef)).thenReturn(Task.pure(Some(label)))
    when(projectCache.get(projectRef)).thenReturn(Task.pure(Some(projectMeta)))
    when(viewCache.get(projectRef))
      .thenReturn(Task.pure(Set[IndexingView](defaultEsView, defaultSQLView, aggView, otherEsView)))
    when(
      viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.defaultElasticIndex.value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(defaultEsView)))
    when(viewCache.getBy[ElasticView](mEq(projectRef), mEq(nxv.defaultElasticIndex.value))(any[Typeable[ElasticView]]))
      .thenReturn(Task.pure(Some(defaultEsView)))
    when(
      viewCache.getBy[ElasticView](mEq(projectRef), mEq(nxv.withSuffix("otherEs").value))(any[Typeable[ElasticView]]))
      .thenReturn(Task.pure(Some(otherEsView)))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.withSuffix("agg").value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(aggView)))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.withSuffix("some").value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(None: Option[IndexingView]))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(nxv.defaultSparqlIndex.value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(Some(defaultSQLView)))
    when(viewCache.getBy[IndexingView](mEq(projectRef), mEq(id.value))(any[Typeable[IndexingView]]))
      .thenReturn(Task.pure(None))

    when(iamClient.identities).thenReturn(Task.pure(Caller(user, Set(Anonymous))))
    val acls = AccessControlLists(/ -> resourceAcls(AccessControlList(Anonymous -> perms)))
    when(aclsOps.fetch()).thenReturn(Task.pure(acls))
    when(projectCache.getProjectLabels(Set(projectRef))).thenReturn(Task.pure(Map(projectRef -> Some(label))))

    def schemaRef: Ref

    def response(deprecated: Boolean = false): Json =
      Json
        .obj(
          "@id"            -> Json.fromString(s"nxv:$genUuid"),
          "_constrainedBy" -> Json.fromString(s"nxs:${schemaRef.iri.show.reverse.takeWhile(_ != '/').reverse}"),
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
  }

  abstract class Ctx(perms: Set[Permission] = manageRes) extends Context(perms) {
    val ctx       = Json.obj("nxv" -> Json.fromString(nxv.base.show), "_rev" -> Json.fromString(nxv.rev.show))
    val schemaRef = Ref(resourceSchemaUri)

    def ctxResponse: Json =
      response() deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resources/$organization/$project/resource/nxv:$genUuid"))
  }

  abstract class File extends Context(manageFiles) {
    val ctx       = Json.obj("nxv" -> Json.fromString(nxv.base.show), "_rev" -> Json.fromString(nxv.rev.show))
    val schemaRef = Ref(fileSchemaUri)

    val metadataRanges: Seq[MediaRange] = List(`application/json`, `application/ld+json`)
  }

  abstract class Schema(perms: Set[Permission] = manageSchemas) extends Context(perms) {
    val schema    = jsonContentOf("/schemas/resolver.json")
    val schemaRef = Ref(shaclSchemaUri)

    def schemaResponse(deprecated: Boolean = false): Json =
      response(deprecated) deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/schemas/$organization/$project/nxv:$genUuid"))
  }

  abstract class Resolver extends Context(manageResolver) {
    val resolver = jsonContentOf("/resolve/cross-project.json") deepMerge Json
      .obj("@id" -> Json.fromString(id.value.show))
      .addContext(resolverCtxUri)
    val types     = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)
    val schemaRef = Ref(resolverSchemaUri)

    def resolverResponse(): Json =
      response() deepMerge Json.obj(
        "@type" -> Json.arr(Json.fromString("nxv:CrossProject"), Json.fromString("nxv:Resolver")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resolvers/$organization/$project/nxv:$genUuid")
      )
  }

  abstract class Views(perms: Set[Permission] = manageViews) extends Context(perms) {
    val view = jsonContentOf("/view/elasticview.json")
      .removeKeys("_uuid")
      .deepMerge(Json.obj("@id" -> Json.fromString(id.value.show)))
      .addContext(viewCtxUri)

    val types     = Set[AbsoluteIri](nxv.View, nxv.ElasticView, nxv.Alpha)
    val schemaRef = Ref(viewSchemaUri)

    def viewResponse(): Json =
      response() deepMerge Json.obj(
        "@type" -> Json
          .arr(Json.fromString("nxv:View"), Json.fromString("nxv:ElasticView"), Json.fromString("nxv:Alpha")),
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
          resources.create(mEq(projectRef), mEq(projectMeta.base), mEq(schemaRef), mEq(resolver))(
            subject = mEq(subject),
            additional = isA[AdditionalValidation[Task]]))
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

    "performing operations on views" should {

      "create a view without @id" in new Views {
        private val expected =
          ResourceF.simpleF(id, view, created = subject, updated = subject, schema = schemaRef, types = types)
        when(
          resources.create(mEq(projectRef),
                           mEq(projectMeta.base),
                           mEq(schemaRef),
                           matches[Json](_.removeKeys("_uuid") == view))(mEq(subject), isA[AdditionalValidation[Task]]))
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
          status shouldEqual StatusCodes.Unauthorized
          responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
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
                             mEq(Some(Latest(viewSchemaUri))),
                             matches[Json](_.removeKeys("_uuid") == jsonUpdateTransformed))(
              mEq(subject),
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

      "create a context without @id" in new Ctx {
        when(
          resources.create(mEq(projectRef), mEq(projectMeta.base), mEq(schemaRef), mEq(ctx))(
            mEq(subject),
            isA[AdditionalValidation[Task]])).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, ctx, created = subject, updated = subject, schema = schemaRef)))

        Post(s"/v1/resources/$organization/$project/resource", ctx) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual ctxResponse
        }
      }

      "create a context with @id" in new Ctx {
        when(resources.create(mEq(id), mEq(schemaRef), mEq(ctx))(mEq(subject), isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, ctx, created = subject, updated = subject, schema = schemaRef)))

        Put(s"/v1/resources/$organization/$project/resource/nxv:$genUuid", ctx) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual ctxResponse
        }
      }

      "list resources constrained by a schema" in new Ctx {
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(schema = Some(resourceSchemaUri))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticClient[Task]]
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

      "list views" in new Ctx(read) {
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(schema = Some(viewSchemaUri))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticClient[Task]]
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

      "list resolvers not deprecated" in new Ctx(read) {
        when(
          resources.list(mEq(Some(defaultEsView)),
                         mEq(SearchParams(deprecated = Some(false), schema = Some(resolverSchemaUri))),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticClient[Task]]
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
            isA[ElasticClient[Task]]
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
          isA[ElasticClient[Task]]
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
      val path    = Paths.get(getClass.getResource("/resources/file.txt").toURI)
      val at1     = FileAttributes("uuid1", path, "file.txt", "text/plain", 1024, Digest("SHA-256", "digest1"))
      val content = new String(Files.readAllBytes(path))
      val source =
        Source.single(ByteString(content)).mapMaterializedValue(_ => FileIO.fromPath(path).to(Sink.ignore).run())

      when(resources.fetch(id, None)).thenReturn(OptionT.some[Task](resource))
      when(resources.fetchFile(id, 1L)).thenReturn(OptionT.some[Task](at1 -> source))
      when(resources.fetchFile(id, 2L)).thenReturn(OptionT.some[Task](at1 -> source))
      when(resources.fetchFile(id, 3L)).thenReturn(OptionT.some[Task](at1 -> source))

      val endpoints = List(
        s"/v1/files/$organization/$project/nxv:$genUuid?rev=1",
        s"/v1/resources/$organization/$project/file/nxv:$genUuid?rev=2",
        s"/v1/resources/$organization/$project/_/nxv:$genUuid?rev=3"
      )
      forAll(endpoints) { endpoint =>
        Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
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
      val at1 =
        FileAttributes("uuid1", Paths.get("some1"), "filename1.txt", "text/plain", 1024, Digest("SHA-256", "digest1"))
      val resourceV =
        simpleV(id, Json.obj(), created = subject, updated = subject, schema = schemaRef).copy(file = Some(at1))
      when(resources.fetch(id, 1L, Some(schemaRef))).thenReturn(OptionT.some[Task](resource))
      when(resources.fetch(id, 1L, None)).thenReturn(OptionT.some[Task](resource))
      val newProject =
        projectMeta.copy(apiMappings = projectMeta.apiMappings ++ defaultPrefixMapping + ("base" -> nxv.projects.value))
      when(resources.materializeWithMeta(resource)(newProject)).thenReturn(EitherT.rightT[Task, Rejection](
        resourceV.copy(value = resourceV.value.copy(graph = Graph(resourceV.metadata)))))

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
      Get(s"/v1/files/$organization/$project/nxv:$genUuid?rev=1&tag=2") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalParameter.type]
      }
    }

    "reject getting a file metadata with both tag and rev query params" in new File {
      Get(s"/v1/files/$organization/$project/nxv:$genUuid?rev=1&tag=2") ~> addCredentials(oauthToken) ~> Accept(
        metadataRanges: _*) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[IllegalParameter.type]
      }
    }

    "reject getting a resource that does not exist" in new Ctx {
      when(resources.fetch(id, None)).thenReturn(OptionT.none[Task, Resource])

      Get(s"/v1/resources/$organization/$project/_/nxv:$genUuid") ~> addHeader("Accept", "application/json") ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[NotFound.type]
        responseAs[Error].message.value shouldEqual s"Resource 'https://bluebrain.github.io/nexus/vocabulary/$genUuid' not found."
      }
    }

    "reject getting a revision that does not exist" in new Ctx {

      val resource = ResourceF.simpleF(id, Json.obj(), created = subject, updated = subject, schema = schemaRef)
      when(resources.fetch(id, None)).thenReturn(OptionT.some[Task](resource))
      when(resources.fetch(id, 1L, Some(resourceRef))).thenReturn(OptionT.none[Task, Resource])

      Get(s"/v1/resources/$organization/$project/_/nxv:$genUuid?rev=1") ~> addHeader("Accept", "application/json") ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[NotFound.type]
        responseAs[Error].message.value shouldEqual s"Resource 'https://bluebrain.github.io/nexus/vocabulary/$genUuid' not found at revision 1."
      }
    }

    "reject getting a tag that does not exist" in new Ctx {

      val resource = ResourceF.simpleF(id, Json.obj(), created = subject, updated = subject, schema = schemaRef)
      when(resources.fetch(id, None)).thenReturn(OptionT.some[Task](resource))
      when(resources.fetch(id, "one", Some(resourceRef))).thenReturn(OptionT.none[Task, Resource])

      Get(s"/v1/resources/$organization/$project/_/nxv:$genUuid?tag=one") ~> addHeader("Accept", "application/json") ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[NotFound.type]
        responseAs[Error].message.value shouldEqual s"Resource 'https://bluebrain.github.io/nexus/vocabulary/$genUuid' not found at tag 'one'."
      }
    }

    "search for resources on a ElasticView" in new Views {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "search for resources on a AggElasticView" in new Views {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}", s"kg_${otherEsView.name}")), mEq(Query()))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$organization/$project/nxv:agg/_search", query) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from Elastic Search " in new Views {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-error-response.json")

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.raiseError(ElasticClientError(StatusCodes.BadRequest, esResponse.noSpaces)))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from Elastic Search when response is not JSON" in new Views {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.raiseError(ElasticClientError(StatusCodes.BadRequest, esResponse)))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual esResponse
      }
    }

    "return 502 Bad Gateway when received unexpected response from ES" in new Views {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.raiseError(ElasticUnexpectedError(StatusCodes.ImATeapot, esResponse)))

      Post(s"/v1/views/$organization/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[DownstreamServiceError] shouldEqual DownstreamServiceError("Error communicating with ElasticSearch")
      }
    }

    "search for resources on a custom SparqlView" in new Views {
      val query  = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      val result = Json.obj("key1" -> Json.fromString("value1"))
      when(sparql.copy(namespace = defaultSQLView.name)).thenReturn(sparql)
      when(sparql.queryRaw(query)).thenReturn(Task.pure(result))

      Post(
        s"/v1/views/$organization/$project/nxv:defaultSparqlIndex/sparql",
        HttpEntity(RdfMediaTypes.`application/sparql-query`, query)) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual result
      }
    }

    "reject searching on a view that does not exists" in new Views {
      Post(s"/v1/views/$organization/$project/nxv:some/_search?size=23&other=value", Json.obj()) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[NotFound.type]
      }
    }

    "performing operations on schemas" should {

      "create a schema without @id" in new Schema {
        when(
          resources.create(mEq(projectRef), mEq(projectMeta.base), mEq(schemaRef), mEq(schema))(
            mEq(subject),
            isA[AdditionalValidation[Task]])).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)))

        Post(s"/v1/schemas/$organization/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "create a schema with @id" in new Schema {
        when(resources.create(mEq(id), mEq(schemaRef), mEq(schema))(mEq(subject), isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)))

        Put(s"/v1/schemas/$organization/$project/nxv:$genUuid", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "update a schema" in new Schema {
        when(
          resources.update(mEq(id), mEq(1L), mEq(Some(schemaRef)), mEq(schema))(mEq(subject),
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
        when(resources.tag(id, 1L, Some(schemaRef), tag)).thenReturn(
          EitherT.rightT[Task, Rejection](
            ResourceF
              .simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)
              .copy(tags = Map("some" -> 2L))))

        Post(s"/v1/schemas/$organization/$project/nxv:$genUuid/tags?rev=1", tag) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      val map = Map("name1" -> 1L, "name2" -> 2L)

      "listing tags for schemas" in new Schema {
        when(resources.fetchTags(id, 1L, Some(schemaRef))).thenReturn(OptionT.some[Task](map))
        val endpoints = List(s"/v1/schemas/$organization/$project/nxv:$genUuid/tags?rev=1",
                             s"/v1/resources/$organization/$project/schema/nxv:$genUuid/tags?rev=1")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual jsonContentOf("/resources/tags.json")
          }
        }
      }

      "listing tags for resolvers" in new Resolver {
        when(resources.fetchTags(id, Some(schemaRef))).thenReturn(OptionT.some[Task](map))
        val endpoints = List(s"/v1/resolvers/$organization/$project/nxv:$genUuid/tags",
                             s"/v1/resources/$organization/$project/resolver/nxv:$genUuid/tags")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual jsonContentOf("/resources/tags.json")
          }
        }
      }

      "listing tags for views" in new Views {
        when(resources.fetchTags(id, Some(schemaRef))).thenReturn(OptionT.some[Task](map))
        val endpoints = List(s"/v1/views/$organization/$project/nxv:$genUuid/tags",
                             s"/v1/resources/$organization/$project/view/nxv:$genUuid/tags")
        forAll(endpoints) { endpoint =>
          Get(endpoint) ~> addCredentials(oauthToken) ~> routes ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[Json] shouldEqual jsonContentOf("/resources/tags.json")
          }
        }
      }

      "deprecate a schema" in new Schema {
        val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(1L)).addContext(tagCtxUri)
        when(resources.deprecate(id, 1L, Some(schemaRef))).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, schema, deprecated = true, created = subject, updated = subject, schema = schemaRef)))

        Delete(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1", tag) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse(deprecated = true)
        }
      }

      "reject getting a schema with both tag and rev query params" in new Schema {
        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1&tag=2") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[Error].code shouldEqual classNameOf[IllegalParameter.type]
        }
      }

      "get schema" ignore new Schema {
        val resource = ResourceF.simpleF(id, schema, created = subject, updated = subject, schema = schemaRef)
        val temp     = simpleV(id, schema, created = subject, updated = subject, schema = schemaRef)
        val ctx      = schema.appendContextOf(shaclCtx)
        val resourceV =
          temp.copy(value = Value(schema, ctx.contextValue, ctx.asGraph.right.value ++ Graph(temp.metadata)))

        when(resources.fetch(id, 1L, Some(schemaRef))).thenReturn(OptionT.some[Task](resource))
        when(resources.materializeWithMeta(resource)).thenReturn(EitherT.rightT[Task, Rejection](resourceV))

        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "reject when creating a schema and the resources/create permissions are not present" in new Schema(read) {
        Post(s"/v1/schemas/$organization/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Unauthorized
          responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
        }
      }

      "reject returning an appropriate error message when exception thrown" in new Schema {
        Get(s"/v1/schemas/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.InternalServerError
          responseAs[Error].code shouldEqual classNameOf[Unexpected.type]
        }
      }

      "reject when the resource is not available" in new Schema {
        Get(s"/v1/other/$organization/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.NotFound
          responseAs[Json] shouldEqual jsonContentOf("/resources/rejection.json")
        }
      }
    }
  }
}
