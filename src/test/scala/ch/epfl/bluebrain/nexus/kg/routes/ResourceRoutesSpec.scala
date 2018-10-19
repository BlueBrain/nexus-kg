package ch.epfl.bluebrain.nexus.kg.routes

import java.nio.file.Paths
import java.time.{Clock, Instant, ZoneId}
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.model.{HttpEntity, StatusCodes, Uri}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import cats.data.{EitherT, OptionT}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.{Account, Project}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.{ElasticClientError, ElasticUnexpectedError}
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.commons.http.{HttpClient, RdfMediaTypes}
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.types.search.{Pagination, QueryResults}
import ch.epfl.bluebrain.nexus.iam.client.Caller.AuthenticatedCaller
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Address._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.acls.AclsOps
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.config.{Contexts, Schemas, Settings}
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.{View => IndexingView}
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{DownstreamServiceError, IllegalParameter, NotFound, Unexpected}
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.Attachment.{BinaryAttributes, Digest}
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.{Error, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.ArgumentMatchers.{any, eq => mEq}
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class ResourceRoutesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with OptionValues
    with MockitoSugar
    with BeforeAndAfter
    with ScalatestRouteTest
    with test.Resources
    with Randomness
    with TestHelper {

  private implicit val appConfig = new Settings(ConfigFactory.parseResources("app.conf").resolve()).appConfig
  private val iamUri             = appConfig.iam.baseUri
  private val adminUri           = appConfig.admin.baseUri
  private implicit val clock     = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  private implicit val adminClient = mock[AdminClient[Task]]
  private implicit val iamClient   = mock[IamClient[Task]]
  private implicit val cache       = mock[DistributedCache[Task]]
  private implicit val store       = mock[AttachmentStore[Task, AkkaIn, AkkaOut]]
  private implicit val resources   = mock[Resources[Task]]

  private implicit val ec       = system.dispatcher
  private implicit val mt       = ActorMaterializer()
  private implicit val utClient = HttpClient.taskHttpClient
  private implicit val qrClient = HttpClient.withTaskUnmarshaller[QueryResults[Json]]
  private val sparql            = mock[BlazegraphClient[Task]]
  private implicit val elastic  = mock[ElasticClient[Task]]
  private implicit val aclsOps  = mock[AclsOps]
  private implicit val clients  = Clients(sparql)

  private val user                              = UserRef("realm", "dmontero")
  private implicit val identity: Identity       = user
  private implicit val token: Option[AuthToken] = Some(AuthToken("valid"))
  private val oauthToken                        = OAuth2BearerToken("valid")
  private val read                              = Permissions(Permission("resources/read"))
  private val manageRes                         = Permissions(Permission("resources/manage"))
  private val manageResolver                    = Permissions(Permission("resolvers/manage"))
  private val manageViews                       = Permissions(Permission("views/manage"))
  private val manageSchemas                     = Permissions(Permission("schemas/manage"))
  private val routes                            = CombinedRoutes(resources)

  abstract class Context(perms: Permissions = manageRes) {
    val account = genString(length = 4)
    val project = genString(length = 4)
    val defaultPrefixMapping: Map[String, AbsoluteIri] = Map(
      "nxv"       -> nxv.base,
      "nxs"       -> Schemas.base,
      "nxc"       -> Contexts.base,
      "resource"  -> Schemas.resourceSchemaUri,
      "documents" -> nxv.defaultElasticIndex,
      "graph"     -> nxv.defaultSparqlIndex
    )
    val projectMeta = Project(
      "name",
      project,
      Map("nxv" -> nxv.base, "resource" -> resourceSchemaUri, "view" -> viewSchemaUri, "resolver" -> resolverSchemaUri),
      nxv.projects,
      1L,
      deprecated = false,
      uuid
    )
    val accountMeta = Account("accountName", 1L, account, false, uuid)
    val projectRef  = ProjectRef(projectMeta.uuid)
    val accountRef  = AccountRef(uuid)
    val genUuid     = uuid
    val iri         = nxv.withSuffix(genUuid)
    val id          = Id(projectRef, iri)
    val defaultEsView =
      ElasticView(Json.obj(), Set.empty, None, false, true, projectRef, nxv.defaultElasticIndex.value, uuid, 1L, false)

    val defaultSQLView = SparqlView(projectRef, nxv.defaultSparqlIndex.value, genUuid, 1L, false)

    implicit val labeledProject = LabeledProject(ProjectLabel(account, project), projectMeta, accountRef)

    when(cache.project(labeledProject.label))
      .thenReturn(Task.pure(Some(projectMeta): Option[Project]))
    when(cache.project(projectRef))
      .thenReturn(Task.pure(Some(projectMeta): Option[Project]))
    when(cache.views(projectRef))
      .thenReturn(Task.pure(Set(defaultEsView, defaultSQLView): Set[IndexingView]))
    when(cache.accountRef(projectRef))
      .thenReturn(Task.pure(Some(accountRef): Option[AccountRef]))
    when(cache.account(accountRef)).thenReturn(Task.pure(Some(accountMeta): Option[Account]))
    when(iamClient.getCaller(filterGroups = true))
      .thenReturn(Task.pure(AuthenticatedCaller(token.value, user, Set(Anonymous))))
    when(aclsOps.fetch()).thenReturn(Task.pure(FullAccessControlList((Anonymous, Address./, perms))))

    def genIri = url"${projectMeta.base}/$uuid"

    def schemaRef: Ref

    def response(deprecated: Boolean = false): Json =
      Json
        .obj(
          "@id"            -> Json.fromString(s"nxv:$genUuid"),
          "_constrainedBy" -> Json.fromString(s"nxs:${schemaRef.iri.show.reverse.takeWhile(_ != '/').reverse}"),
          "_createdAt"     -> Json.fromString(clock.instant().toString),
          "_createdBy"     -> Json.fromString(iamUri.append("realms" / user.realm / "users" / user.sub).toString()),
          "_deprecated"    -> Json.fromBoolean(deprecated),
          "_rev"           -> Json.fromLong(1L),
          "_project"       -> Json.fromString(adminUri.append("projects" / account / project).toString()),
          "_updatedAt"     -> Json.fromString(clock.instant().toString),
          "_updatedBy"     -> Json.fromString(iamUri.append("realms" / user.realm / "users" / user.sub).toString())
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
          val id = appConfig.http.publicUri.append("resources" / account / project / "resource" / s"resource:$i")
          jsonContentOf("/resources/es-metadata.json", Map(quote("{id}") -> id.toString()))
        }): _*
      )
    )
  }

  abstract class Ctx(perms: Permissions = manageRes) extends Context(perms) {
    val ctx       = Json.obj("nxv" -> Json.fromString(nxv.base.show), "_rev" -> Json.fromString(nxv.rev.show))
    val schemaRef = Ref(resourceSchemaUri)

    def ctxResponse: Json =
      response() deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resources/$account/$project/resource/nxv:$genUuid"))
  }

  abstract class Schema(perms: Permissions = manageSchemas) extends Context(perms) {
    val schema    = jsonContentOf("/schemas/resolver.json")
    val schemaRef = Ref(shaclSchemaUri)

    def schemaResponse(deprecated: Boolean = false): Json =
      response(deprecated) deepMerge Json.obj(
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/schemas/$account/$project/nxv:$genUuid"))
  }

  abstract class Resolver(perms: Permissions = manageResolver) extends Context(perms) {
    val resolver = jsonContentOf("/resolve/cross-project.json") deepMerge Json.obj(
      "@id" -> Json.fromString(id.value.show))
    val types     = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)
    val schemaRef = Ref(resolverSchemaUri)

    def resolverResponse(): Json =
      response() deepMerge Json.obj(
        "@type" -> Json.arr(Json.fromString("nxv:CrossProject"), Json.fromString("nxv:Resolver")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/resolvers/$account/$project/nxv:$genUuid")
      )

    val resolverSet = Set(
      InProjectResolver(projectRef, nxv.deprecated, 1L, deprecated = false, 20),
      InAccountResolver(Set(nxv.Schema), List(Anonymous), accountRef, projectRef, nxv.sub, 2L, deprecated = true, 1),
      CrossProjectResolver(Set(nxv.Schema),
                           Set(projectRef),
                           List(Anonymous),
                           projectRef,
                           nxv.group,
                           2L,
                           deprecated = false,
                           30),
      InAccountResolver(Set(nxv.Schema), List(Anonymous), accountRef, projectRef, nxv.realm, 2L, deprecated = false, 10)
    )
  }

  abstract class View(perms: Permissions = manageViews) extends Context(perms) {
    val view = jsonContentOf("/view/elasticview.json")
      .removeKeys("_uuid")
      .deepMerge(Json.obj("@id" -> Json.fromString(id.value.show)))

    val types           = Set[AbsoluteIri](nxv.View, nxv.ElasticView, nxv.Alpha)
    val schemaRef       = Ref(viewSchemaUri)
    private val mapping = jsonContentOf("/elastic/mapping.json")

    val views = Set(
      ElasticView(
        mapping,
        Set(nxv.Schema, nxv.Resource),
        Some("one"),
        false,
        true,
        ProjectRef("ref"),
        url"http://example.com/id".value,
        "3aa14a1a-81e7-4147-8306-136d8270bb01",
        1L,
        false
      ),
      SparqlView(ProjectRef("ref"),
                 url"http://example.com/id2".value,
                 "247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1",
                 1L,
                 false)
    )

    def viewResponse(): Json =
      response() deepMerge Json.obj(
        "@type" -> Json
          .arr(Json.fromString("nxv:View"), Json.fromString("nxv:ElasticView"), Json.fromString("nxv:Alpha")),
        "_self" -> Json.fromString(s"http://127.0.0.1:8080/v1/views/$account/$project/nxv:$genUuid")
      )
  }

  "The routes" when {

    "performing operations on resolvers" should {

      "create a resolver without @id" in new Resolver {
        val resolverWithCtx = resolver.addContext(resolverCtxUri)
        private val expected = ResourceF
          .simpleF(id, resolverWithCtx, created = identity, updated = identity, schema = schemaRef, types = types)
        when(
          resources.create(mEq(projectRef), mEq(projectMeta.base), mEq(schemaRef), mEq(resolverWithCtx))(
            identity = mEq(identity),
            additional = isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Post(s"/v1/resolvers/$account/$project", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
        Post(s"/v1/resources/$account/$project/resolver", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(resolverResponse())
        }
      }

      "list resolvers" in new Resolver {
        val json = jsonContentOf("/resources/resolvers-list.json",
                                 Map(quote("{account}") -> accountMeta.label, quote("{proj}") -> projectMeta.label))
        when(cache.resolvers(projectRef)).thenReturn(Task.pure(resolverSet))
        Get(s"/v1/resolvers/$account/$project") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual json
        }
        Get(s"/v1/resources/$account/$project/resolver") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual json
        }
      }

      "list resolvers not deprecated" in new Resolver {
        val json = jsonContentOf("/resources/resolvers-list-no-deprecated.json",
                                 Map(quote("{account}") -> accountMeta.label, quote("{proj}") -> projectMeta.label))
        when(cache.resolvers(projectRef)).thenReturn(Task.pure(resolverSet))
        Get(s"/v1/resolvers/$account/$project?deprecated=false") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual json
        }
        Get(s"/v1/resources/$account/$project/resolver?deprecated=false") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual json
        }
      }
    }

    "performing operations on views" should {

      "create a view without @id" in new View {
        val viewWithCtx = view.addContext(viewCtxUri)
        private val expected =
          ResourceF.simpleF(id, viewWithCtx, created = identity, updated = identity, schema = schemaRef, types = types)
        when(
          resources.create(
            mEq(projectRef),
            mEq(projectMeta.base),
            mEq(schemaRef),
            matches[Json](_.removeKeys("_uuid") == viewWithCtx))(mEq(identity), isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Post(s"/v1/views/$account/$project", view) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }

        Post(s"/v1/resources/$account/$project/view", view) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }

      "reject when not enough permissions" in new View(Permissions(Permission("views/read"))) {
        val mapping = Json.obj("mapping" -> Json.obj("key" -> Json.fromString("value")))
        Put(s"/v1/views/$account/$project/nxv:$genUuid", view deepMerge mapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Unauthorized
          responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
        }
      }

      "create a view with @id" in new View {
        val mapping = Json.obj("mapping" -> Json.obj("key" -> Json.fromString("value")))
        val viewWithCtx = view.addContext(viewCtxUri) deepMerge Json.obj(
          "mapping" -> Json.fromString("""{"key":"value"}"""))
        private val expected =
          ResourceF.simpleF(id, viewWithCtx, created = identity, updated = identity, schema = schemaRef, types = types)
        when(
          resources.createWithId(mEq(id), mEq(schemaRef), matches[Json](_.removeKeys("_uuid") == viewWithCtx))(
            mEq(identity),
            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Put(s"/v1/views/$account/$project/nxv:$genUuid", view deepMerge mapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }

        Put(s"/v1/resources/$account/$project/view/nxv:$genUuid", view deepMerge mapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse())
        }
      }

      "update a view" in new View {
        val mapping = Json.obj("mapping" -> Json.obj("key" -> Json.fromString("value")))
        val viewWithCtx = view.addContext(viewCtxUri) deepMerge Json.obj(
          "mapping" -> Json.fromString("""{"key":"value"}"""))
        private val expected =
          ResourceF.simpleF(id, viewWithCtx, created = identity, updated = identity, schema = schemaRef, types = types)
        when(
          resources.createWithId(mEq(id), mEq(schemaRef), matches[Json](_.removeKeys("_uuid") == viewWithCtx))(
            mEq(identity),
            isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Put(s"/v1/views/$account/$project/nxv:$genUuid", view deepMerge mapping) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
        }
        val mappingUpdated = Json.obj("mapping" -> Json.obj("key2" -> Json.fromString("value2")))

        val uuidJson       = Json.obj("_uuid" -> Json.fromString("uuid1"))
        val expectedUpdate = expected.copy(value = view.deepMerge(uuidJson).appendContextOf(viewCtx))
        when(resources.fetch(id, Some(Latest(viewSchemaUri)))).thenReturn(OptionT.some[Task](expectedUpdate))
        val jsonUpdate = view.addContext(viewCtxUri) deepMerge Json.obj(
          "mapping" -> Json.fromString("""{"key2":"value2"}""")) deepMerge uuidJson
        when(
          resources.update(mEq(id), mEq(1L), mEq(Some(Latest(viewSchemaUri))), mEq(jsonUpdate))(
            mEq(identity),
            isA[AdditionalValidation[Task]])).thenReturn(EitherT.rightT[Task, Rejection](expectedUpdate.copy(rev = 2L)))
        when(resources.materializeWithMeta(expectedUpdate))
          .thenReturn(EitherT.rightT[Task, Rejection](simpleV(expectedUpdate)))
        Put(s"/v1/views/$account/$project/nxv:$genUuid?rev=1", view deepMerge mappingUpdated) ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse() deepMerge Json.obj("_rev" -> Json.fromLong(2L)))
        }

        Put(s"/v1/resources/$account/$project/view/nxv:$genUuid?rev=1", view deepMerge mappingUpdated) ~> addCredentials(
          oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] should equalIgnoreArrayOrder(viewResponse() deepMerge Json.obj("_rev" -> Json.fromLong(2L)))
        }
      }

      "list views" in new View {
        when(cache.views(projectRef)).thenReturn(Task.pure(views))
        Get(s"/v1/views/$account/$project") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual jsonContentOf("/view/view-list-resp.json")
        }
        Get(s"/v1/resources/$account/$project/view") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual jsonContentOf("/view/view-list-resp.json")
        }
      }

      "list views not deprecated" in new View {
        when(cache.views(projectRef)).thenReturn(
          Task.pure(
            views + SparqlView(projectRef,
                               url"http://example.com/id3".value,
                               "317d223b-1d38-4c6e-8fed-f9a8c2ccb4a1",
                               1L,
                               true)))
        Get(s"/v1/views/$account/$project?deprecated=false") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual jsonContentOf("/view/view-list-resp.json")
        }
        Get(s"/v1/resources/$account/$project/view?deprecated=false") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual jsonContentOf("/view/view-list-resp.json")
        }
      }
    }

    "performing operations on resources" should {

      "create a context without @id" in new Ctx {
        when(
          resources.create(mEq(projectRef), mEq(projectMeta.base), mEq(schemaRef), mEq(ctx))(
            mEq(identity),
            isA[AdditionalValidation[Task]])).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, ctx, created = identity, updated = identity, schema = schemaRef)))

        Post(s"/v1/resources/$account/$project/resource", ctx) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual ctxResponse
        }
      }

      "create a context with @id" in new Ctx {
        when(resources.createWithId(mEq(id), mEq(schemaRef), mEq(ctx))(mEq(identity), isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, ctx, created = identity, updated = identity, schema = schemaRef)))

        Put(s"/v1/resources/$account/$project/resource/nxv:$genUuid", ctx) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual ctxResponse
        }
      }

      def metadata(account: String, project: String, i: Int): Json = {
        val id = url"${appConfig.http.publicUri.copy(
          path = appConfig.http.publicUri.path / "resources" / account / project / "resource" / s"resource:$i")}".value
        jsonContentOf("/resources/es-metadata.json", Map(quote("{id}") -> id.asString)) deepMerge Json.obj(
          "_original_source" -> Json.fromString(Json.obj("k" -> Json.fromInt(1)).noSpaces))
      }

      "list resources constrained by a schema" in new Ctx {

        when(
          resources.list(mEq(Set(defaultEsView, defaultSQLView)),
                         mEq(None),
                         mEq(resourceSchemaUri),
                         mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticClient[Task]]
          )
        ).thenReturn(Task.pure(
          UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(account, project, i))))))
        Get(s"/v1/resources/$account/$project/resource") ~> addCredentials(oauthToken) ~> addHeader(
          "Accept",
          "application/json") ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual listingResponse()
        }
      }

      "list resources" in new Ctx {
        when(
          resources.list(mEq(Set(defaultEsView, defaultSQLView)), mEq(None), mEq(Pagination(0, 20)))(
            isA[HttpClient[Task, QueryResults[Json]]],
            isA[ElasticClient[Task]]
          )
        ).thenReturn(Task.pure(
          UnscoredQueryResults(5, List.range(1, 6).map(i => UnscoredQueryResult(metadata(account, project, i))))))

        Get(s"/v1/resources/$account/$project") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual listingResponse()
        }

        Get(s"/v1/data/$account/$project") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual listingResponse()
        }
      }
    }

    "get a resource with attachments" in new Ctx {
      val resource = ResourceF.simpleF(id, ctx, created = identity, updated = identity, schema = schemaRef)
      val at1 =
        BinaryAttributes("uuid1", Paths.get("some1"), "filename1.txt", "text/plain", 1024, Digest("SHA-256", "digest1"))
      val at2 =
        BinaryAttributes("uuid2", Paths.get("some2"), "filename2.txt", "text/plain", 2048, Digest("SHA-256", "digest2"))
      val resourceV =
        simpleV(id, ctx, created = identity, updated = identity, schema = schemaRef).copy(attachments = Set(at1, at2))

      when(resources.fetch(id, 1L, Some(schemaRef))).thenReturn(OptionT.some[Task](resource))
      when(resources.fetch(id, 1L, None)).thenReturn(OptionT.some[Task](resource))
      val lb = labeledProject.copy(project = labeledProject.project.copy(
        prefixMappings = labeledProject.project.prefixMappings ++ defaultPrefixMapping + ("base" -> nxv.projects.value)))
      when(resources.materializeWithMeta(resource)(lb)).thenReturn(EitherT.rightT[Task, Rejection](
        resourceV.copy(value = resourceV.value.copy(graph = Graph(resourceV.metadata)))))

      val replacements = Map(quote("{account}") -> account, quote("{proj}") -> project, quote("{uuid}") -> genUuid)

      Get(s"/v1/resources/$account/$project/resource/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(
          jsonContentOf("/resources/resource-with-at.json", replacements))
      }

      Get(s"/v1/data/$account/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json].removeKeys("@context") should equalIgnoreArrayOrder(
          jsonContentOf("/resources/resource-with-at.json", replacements))
      }
    }

    "reject getting a resource that does not exist" in new Ctx {

      when(resources.fetch(id, 1L, Some(schemaRef))).thenReturn(OptionT.none[Task, Resource])

      Get(s"/v1/resources/$account/$project/resource/nxv:$genUuid?rev=1") ~> addHeader("Accept", "application/json") ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[NotFound.type]
      }
    }

    "search for resources on a custom ElasticView" in new View {
      val query      = Json.obj("query" -> Json.obj("match_all" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-response.json")

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.pure(esResponse))

      Post(s"/v1/views/$account/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from Elastic Search " in new View {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = jsonContentOf("/view/search-error-response.json")

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.raiseError(ElasticClientError(StatusCodes.BadRequest, esResponse.noSpaces)))

      Post(s"/v1/views/$account/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Json] shouldEqual esResponse
      }
    }

    "return 400 Bad Request from Elastic Search when response is not JSON" in new View {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.raiseError(ElasticClientError(StatusCodes.BadRequest, esResponse)))

      Post(s"/v1/views/$account/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[String] shouldEqual esResponse
      }
    }

    "return 502 Bad Gateway when received unexpected response from ES" in new View {
      val query      = Json.obj("query" -> Json.obj("error" -> Json.obj()))
      val esResponse = "some error response"

      when(
        elastic.searchRaw(mEq(query), mEq(Set(s"kg_${defaultEsView.name}")), mEq(Uri.Query(Map("other" -> "value"))))(
          any[HttpClient[Task, Json]]()))
        .thenReturn(Task.raiseError(ElasticUnexpectedError(StatusCodes.ImATeapot, esResponse)))

      Post(s"/v1/views/$account/$project/nxv:defaultElasticIndex/_search?other=value", query) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[DownstreamServiceError] shouldEqual DownstreamServiceError("Error communicating with ElasticSearch")
      }
    }

    "search for resources on a custom SparqlView" in new View {
      val query  = "SELECT ?s where {?s ?p ?o} LIMIT 10"
      val result = Json.obj("key1" -> Json.fromString("value1"))
      when(sparql.copy(namespace = defaultSQLView.name)).thenReturn(sparql)
      when(sparql.queryRaw(query)).thenReturn(Task.pure(result))

      Post(
        s"/v1/views/$account/$project/nxv:defaultSparqlIndex/sparql",
        HttpEntity(RdfMediaTypes.`application/sparql-query`, query)) ~> addCredentials(oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual result
      }
    }

    "reject searching on a view that does not exists" in new View {
      Post(s"/v1/views/$account/$project/nxv:some/_search?size=23&other=value", Json.obj()) ~> addCredentials(
        oauthToken) ~> routes ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[NotFound.type]
      }
    }

    "performing operations on schemas" should {

      "create a schema without @id" in new Schema {
        when(
          resources.create(mEq(projectRef), mEq(projectMeta.base), mEq(schemaRef), mEq(schema))(
            mEq(identity),
            isA[AdditionalValidation[Task]])).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)))

        Post(s"/v1/schemas/$account/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "create a schema with @id" in new Schema {
        when(
          resources.createWithId(mEq(id), mEq(schemaRef), mEq(schema))(mEq(identity), isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)))

        Put(s"/v1/schemas/$account/$project/nxv:$genUuid", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "update a schema" in new Schema {
        when(
          resources.update(mEq(id), mEq(1L), mEq(Some(schemaRef)), mEq(schema))(mEq(identity),
                                                                                isA[AdditionalValidation[Task]]))
          .thenReturn(EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)))

        Put(s"/v1/schemas/$account/$project/nxv:$genUuid?rev=1", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "add a tag to a schema" in new Schema {
        val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(1L)).addContext(tagCtxUri)
        when(resources.tag(id, 1L, Some(schemaRef), tag)).thenReturn(
          EitherT.rightT[Task, Rejection](
            ResourceF
              .simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)
              .copy(tags = Map("some" -> 2L))))

        Put(s"/v1/schemas/$account/$project/nxv:$genUuid/tags?rev=1", tag) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "deprecate a schema" in new Schema {
        val tag = Json.obj("tag" -> Json.fromString("some"), "rev" -> Json.fromLong(1L)).addContext(tagCtxUri)
        when(resources.deprecate(id, 1L, Some(schemaRef))).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, schema, deprecated = true, created = identity, updated = identity, schema = schemaRef)))

        Delete(s"/v1/schemas/$account/$project/nxv:$genUuid?rev=1", tag) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse(deprecated = true)
        }
      }

      "remove attachment from a schema" in new Schema {
        when(resources.unattach(id, 1L, Some(schemaRef), "name")).thenReturn(EitherT.rightT[Task, Rejection](
          ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)))

        Delete(s"/v1/schemas/$account/$project/nxv:$genUuid/attachments/name?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "reject getting a schema with both tag and rev query params" in new Schema {
        Get(s"/v1/schemas/$account/$project/nxv:$genUuid?rev=1&tag=2") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[Error].code shouldEqual classNameOf[IllegalParameter.type]
        }
      }

      "reject getting a schema attachment with both tag and rev query params" in new Schema {
        Get(s"/v1/schemas/$account/$project/nxv:$genUuid/attachments/some?rev=1&tag=2") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.BadRequest
          responseAs[Error].code shouldEqual classNameOf[IllegalParameter.type]
        }
      }

      "get schema" ignore new Schema {
        val resource = ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)
        val temp     = simpleV(id, schema, created = identity, updated = identity, schema = schemaRef)
        val ctx      = schema.appendContextOf(shaclCtx)
        val resourceV =
          temp.copy(value =
            Value(schema, ctx.contextValue, ctx.asGraph.right.value ++ Graph(temp.metadata ++ temp.typeTriples)))

        when(resources.fetch(id, 1L, Some(schemaRef))).thenReturn(OptionT.some[Task](resource))
        when(resources.materializeWithMeta(resource)).thenReturn(EitherT.rightT[Task, Rejection](resourceV))

        Get(s"/v1/schemas/$account/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "reject when creating a schema and the resources/create permissions are not present" in new Schema(read) {
        Post(s"/v1/schemas/$account/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Unauthorized
          responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
        }
      }

      "reject returning an appropriate error message when exception thrown" in new Schema {
        Get(s"/v1/schemas/$account/$project/nxv:$genUuid?rev=1") ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.InternalServerError
          responseAs[Error].code shouldEqual classNameOf[Unexpected.type]
        }
      }
    }
  }
}
