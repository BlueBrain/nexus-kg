package ch.epfl.bluebrain.nexus.kg.directives

import java.time.Instant
import java.util.UUID

import _root_.org.mockito.{IdiomaticMockito, Mockito}
import _root_.org.scalatest.{BeforeAndAfter, EitherValues, Matchers, WordSpecLike}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.iam.client.IamClientError
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.Error._
import ch.epfl.bluebrain.nexus.kg.KgError.{OrganizationNotFound, ProjectIsDeprecated, ProjectNotFound}
import ch.epfl.bluebrain.nexus.kg.cache.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.HttpConfig
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{Schemas, Settings}
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectLabel
import ch.epfl.bluebrain.nexus.kg.routes.Routes
import ch.epfl.bluebrain.nexus.kg.{Error, KgError, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import io.circe.Decoder
import io.circe.generic.auto._
import monix.eval.Task

//noinspection NameBooleanParameters
class ProjectDirectivesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with IdiomaticMockito
    with BeforeAndAfter
    with ScalatestRouteTest
    with TestHelper {

  private val appConfig                 = Settings(system).appConfig
  private implicit val http: HttpConfig = appConfig.http

  private implicit val projectCache: ProjectCache[Task] = mock[ProjectCache[Task]]
  private implicit val client: AdminClient[Task]        = mock[AdminClient[Task]]
  private implicit val cred: Option[AuthToken]          = None

  before {
    Mockito.reset(projectCache)
    Mockito.reset(client)
  }

  private val id = genIri

  private implicit val orgDecoder: Decoder[Organization] =
    Decoder.instance { hc =>
      for {
        description <- hc.getOrElse[Option[String]]("description")(None)
        label       <- hc.get[String]("label")
        uuid        <- hc.get[String]("uuid").map(UUID.fromString)
        rev         <- hc.get[Long]("rev")
        deprecated  <- hc.get[Boolean]("deprecated")
        createdBy   <- hc.get[AbsoluteIri]("createdBy")
        createdAt   <- hc.get[Instant]("createdAt")
        updatedBy   <- hc.get[AbsoluteIri]("updatedBy")
        updatedAt   <- hc.get[Instant]("updatedAt")
      } yield Organization(id, label, description, uuid, rev, deprecated, createdAt, createdBy, updatedAt, updatedBy)
    }

  private implicit val projectDecoder: Decoder[Project] =
    Decoder.instance { hc =>
      for {
        organization     <- hc.get[String]("organizationLabel")
        description      <- hc.getOrElse[Option[String]]("description")(None)
        base             <- hc.get[AbsoluteIri]("base")
        vocab            <- hc.get[AbsoluteIri]("vocab")
        apiMap           <- hc.get[Map[String, AbsoluteIri]]("apiMappings")
        label            <- hc.get[String]("label")
        uuid             <- hc.get[String]("uuid").map(UUID.fromString)
        organizationUuid <- hc.get[String]("organizationUuid").map(UUID.fromString)
        rev              <- hc.get[Long]("rev")
        deprecated       <- hc.get[Boolean]("deprecated")
        createdBy        <- hc.get[AbsoluteIri]("createdBy")
        createdAt        <- hc.get[Instant]("createdAt")
        updatedBy        <- hc.get[AbsoluteIri]("updatedBy")
        updatedAt        <- hc.get[Instant]("updatedAt")
      } yield
        Project(id,
                label,
                organization,
                description,
                base,
                vocab,
                apiMap,
                uuid,
                organizationUuid,
                rev,
                deprecated,
                createdAt,
                createdBy,
                updatedAt,
                updatedBy)
    }

  "A Project directives" should {

    val creator = Iri.absolute("http://example.com/subject").right.value

    def projectRoute(): Route = {
      import monix.execution.Scheduler.Implicits.global
      Routes.wrap(
        (get & project) { project =>
          complete(StatusCodes.OK -> project)
        }
      )
    }

    def orgRoute(): Route = {
      import monix.execution.Scheduler.Implicits.global
      Routes.wrap(
        (get & org) { o =>
          complete(StatusCodes.OK -> o)
        }
      )
    }

    def projectNotDepRoute(implicit proj: Project): Route =
      Routes.wrap(
        (get & projectNotDeprecated) {
          complete(StatusCodes.OK)
        }
      )

    val label = ProjectLabel("organization", "project")
    val apiMappings = Map[String, AbsoluteIri](
      "nxv"           -> nxv.base,
      "resource"      -> Schemas.unconstrainedSchemaUri,
      "elasticsearch" -> nxv.defaultElasticSearchIndex,
      "graph"         -> nxv.defaultSparqlIndex
    )
    val projectMeta = Project(
      id,
      "project",
      "organization",
      None,
      nxv.projects,
      genIri,
      apiMappings,
      genUUID,
      genUUID,
      1L,
      false,
      Instant.EPOCH,
      creator,
      Instant.EPOCH,
      creator
    )

    val orgMeta =
      Organization(id, "organization", None, genUUID, 1L, false, Instant.EPOCH, creator, Instant.EPOCH, creator)

    val apiMappingsFinal = Map[String, AbsoluteIri](
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

    val projectMetaResp =
      projectMeta.copy(apiMappings = projectMeta.apiMappings ++ apiMappingsFinal)

    "fetch the organization from admin client" in {

      client.fetchOrganization("organization") shouldReturn Task.pure(Option(orgMeta))

      Get("/organization") ~> orgRoute() ~> check {
        responseAs[Organization] shouldEqual orgMeta
      }
    }

    "reject organization when not found on the admin client" in {
      client.fetchOrganization("organization") shouldReturn Task.pure(None)

      Get("/organization") ~> orgRoute() ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[OrganizationNotFound]
      }
    }

    "fetch the project from the cache" in {

      projectCache.getBy(label) shouldReturn Task.pure(Option(projectMeta))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[Project] shouldEqual projectMetaResp
      }
    }

    "fetch the project from admin client when not present on the cache" in {
      projectCache.getBy(label) shouldReturn Task.pure(None)

      client.fetchProject("organization", "project") shouldReturn Task.pure(Option(projectMeta))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[Project] shouldEqual projectMetaResp
      }
    }

    "fetch the project from admin client when cache throws an error" in {
      projectCache.getBy(label) shouldReturn Task.raiseError(new RuntimeException)
      client.fetchProject("organization", "project") shouldReturn Task.pure(Option(projectMeta))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[Project] shouldEqual projectMetaResp
      }
    }

    "reject project when not found neither in the cache nor calling the admin client" in {
      projectCache.getBy(label) shouldReturn Task.pure(None)
      client.fetchProject("organization", "project") shouldReturn Task.pure(None)

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].tpe shouldEqual classNameOf[ProjectNotFound]
      }
    }

    "reject when admin client signals forbidden" in {
      val label = ProjectLabel("organization", "project")
      projectCache.getBy(label) shouldReturn Task.pure(None)
      client.fetchProject("organization", "project") shouldReturn Task.raiseError(IamClientError.Forbidden(""))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.Forbidden
        responseAs[Error].tpe shouldEqual "AuthorizationFailed"
      }
    }

    "reject when admin client signals another error" in {
      val label = ProjectLabel("organization", "project")
      projectCache.getBy(label) shouldReturn Task.pure(None)
      client.fetchProject("organization", "project") shouldReturn
        Task.raiseError(IamClientError.UnknownError(StatusCodes.InternalServerError, ""))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.InternalServerError
        responseAs[Error].tpe shouldEqual classNameOf[KgError.InternalError]
      }
    }

    "pass when available project is not deprecated" in {
      Get("/") ~> projectNotDepRoute(projectMeta) ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "reject when available project is deprecated" in {
      Get("/") ~> projectNotDepRoute(projectMeta.copy(deprecated = true)) ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].tpe shouldEqual classNameOf[ProjectIsDeprecated]
      }
    }
  }
}
