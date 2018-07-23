package ch.epfl.bluebrain.nexus.kg.routes

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.OAuth2BearerToken
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import cats.data.{EitherT, OptionT}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.syntax.circe._
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.iam.client.Caller.AuthenticatedCaller
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.Address._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.Error.classNameOf
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas.{crossResolverSchemaUri, shaclSchemaUri}
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{IllegalParameter, Unexpected}
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore
import ch.epfl.bluebrain.nexus.kg.resources.attachment.AttachmentStore.{AkkaIn, AkkaOut}
import ch.epfl.bluebrain.nexus.kg.{Error, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import com.typesafe.config.ConfigFactory
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.ArgumentMatchers.{isA, eq => mEq}
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
  private implicit val clients  = Clients(sparql)

  private val user                              = UserRef("realm", "dmontero")
  private implicit val identity: Identity       = user
  private implicit val token: Option[AuthToken] = Some(AuthToken("valid"))
  private val oauthToken                        = OAuth2BearerToken("valid")
  private val read                              = Permissions(Permission("resources/read"))
  private val manage                            = Permissions(Permission("resources/manage"))
  private val routes                            = ResourceRoutes(resources).routes

  abstract class Context(perms: Permissions = manage) {
    val account     = genString(length = 4)
    val project     = genString(length = 4)
    val projectMeta = Project("name", project, Map("nxv" -> nxv.base), nxv.projects, 1L, false, uuid)
    val projectRef  = ProjectRef(projectMeta.uuid)
    val genUuid     = uuid
    val iri         = nxv.withPath(genUuid)
    val id          = Id(projectRef, iri)

    when(cache.project(ProjectLabel(account, project)))
      .thenReturn(Task.pure(Some(projectMeta): Option[Project]))
    when(iamClient.getCaller(filterGroups = true))
      .thenReturn(Task.pure(AuthenticatedCaller(token.value, user, Set.empty)))
    when(adminClient.getProjectAcls(account, project, parents = true, self = true))
      .thenReturn(Task.pure(Some(FullAccessControlList((Anonymous, account / project, perms)))))

    def genIri = url"${projectMeta.base}/$uuid"

    def eqProjectRef             = mEq(projectRef.id).asInstanceOf[ProjectRef]
    def isAnAdditionalValidation = isA(classOf[AdditionalValidation[Task]])
  }

  abstract class Schema(perms: Permissions = manage) extends Context(perms) {
    val schema    = jsonContentOf("/schemas/cross-project-resolver.json")
    val schemaRef = Ref(shaclSchemaUri)

    def schemaResponse(deprecated: Boolean = false): Json =
      Json
        .obj(
          "@id"            -> Json.fromString(s"nxv:$genUuid"),
          "_constrainedBy" -> Json.fromString(shaclSchemaUri.show),
          "_createdAt"     -> Json.fromString(clock.instant().toString),
          "_createdBy"     -> Json.fromString(iamUri.append("realms" / user.realm / "users" / user.sub).toString()),
          "_deprecated"    -> Json.fromBoolean(deprecated),
          "_rev"           -> Json.fromLong(1L),
          "_updatedAt"     -> Json.fromString(clock.instant().toString),
          "_updatedBy"     -> Json.fromString(iamUri.append("realms" / user.realm / "users" / user.sub).toString())
        )
        .addContext(resourceCtxUri)
  }

  abstract class Resolver(perms: Permissions = manage) extends Context(perms) {
    val resolver  = jsonContentOf("/resolve/cross-project.json") deepMerge Json.obj("@id" -> Json.fromString(id.show))
    val types     = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)
    val schemaRef = Ref(crossResolverSchemaUri)

    def resolverResponse(deprecated: Boolean = false): Json =
      Json
        .obj(
          "@id"            -> Json.fromString(s"nxv:$genUuid"),
          "_constrainedBy" -> Json.fromString(crossResolverSchemaUri.show),
          "_createdAt"     -> Json.fromString(clock.instant().toString),
          "_createdBy"     -> Json.fromString(iamUri.append("realms" / user.realm / "users" / user.sub).toString()),
          "_deprecated"    -> Json.fromBoolean(deprecated),
          "_rev"           -> Json.fromLong(1L),
          "_updatedAt"     -> Json.fromString(clock.instant().toString),
          "_updatedBy"     -> Json.fromString(iamUri.append("realms" / user.realm / "users" / user.sub).toString())
        )
        .addContext(resourceCtxUri)
  }

  "The routes" when {

    "performing operations on resolvers" should {

      "create a resolver without @id" in new Resolver {
        val resolverWithCtx = resolver.addContext(resolverCtxUri)
        private val expected = ResourceF
          .simpleF(id, resolverWithCtx, created = identity, updated = identity, schema = schemaRef, types = types)
        when(
          resources.create(eqProjectRef, mEq(projectMeta.base), mEq(schemaRef), mEq(resolverWithCtx))(
            mEq(identity),
            isAnAdditionalValidation))
          .thenReturn(EitherT.rightT[Task, Rejection](expected))

        Post(s"/v1/resolvers/$account/$project", resolver) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          val response = responseAs[Json]
          response.removeKeys("@type") shouldEqual resolverResponse()
          response.hcursor.downField("@type").focus.flatMap(_.asArray).value should contain theSameElementsAs (Vector(
            Json.fromString("nxv:CrossProject"),
            Json.fromString("nxv:Resolver")))
        }
      }
    }

    "performing operations on schemas" should {

      "create a schema without @id" in new Schema {
        when(resources.create(projectRef, projectMeta.base, schemaRef, schema)).thenReturn(
          EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)))

        Post(s"/v1/schemas/$account/$project", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
          status shouldEqual StatusCodes.Created
          responseAs[Json] shouldEqual schemaResponse()
        }
      }

      "create a schema with @id" in new Schema {
        when(resources.createWithId(mEq(id), mEq(schemaRef), mEq(schema))(mEq(identity), isAnAdditionalValidation))
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
                                                                                isAnAdditionalValidation)).thenReturn(
          EitherT.rightT[Task, Rejection](
            ResourceF.simpleF(id, schema, created = identity, updated = identity, schema = schemaRef)))

        Put(s"/v1/schemas/$account/$project/nxv:${genUuid}?rev=1", schema) ~> addCredentials(oauthToken) ~> routes ~> check {
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
          temp.copy(value = Value(schema, ctx.contextValue, ctx.asGraph ++ temp.metadata ++ temp.typeGraph))

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
