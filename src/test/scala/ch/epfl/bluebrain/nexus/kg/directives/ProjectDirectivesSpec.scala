package ch.epfl.bluebrain.nexus.kg.directives

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.Error._
import ch.epfl.bluebrain.nexus.kg.async.ProjectCache
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.config.{Contexts, Schemas}
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.RejectionHandling
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.ProjectLabel
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.{Error, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.instances._
import io.circe.Decoder
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, EitherValues, Matchers, WordSpecLike}

class ProjectDirectivesSpec
    extends WordSpecLike
    with Matchers
    with EitherValues
    with MockitoSugar
    with BeforeAndAfter
    with ScalatestRouteTest
    with TestHelper {

  private implicit val projectCache            = mock[ProjectCache[Task]]
  private implicit val client                  = mock[AdminClient[Task]]
  private implicit val cred: Option[AuthToken] = None

  before {
    Mockito.reset(projectCache)
    Mockito.reset(client)
  }

  private val id = genIri

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
      handleRejections(RejectionHandling()) {
        (get & project) { project =>
          complete(StatusCodes.OK -> project)
        }
      }
    }

    def projectNotDepRoute(implicit proj: Project): Route =
      handleRejections(RejectionHandling()) {
        (get & projectNotDeprecated) {
          complete(StatusCodes.OK)
        }
      }

    val label = ProjectLabel("organization", "project")
    val apiMappings = Map[String, AbsoluteIri](
      "nxv"           -> nxv.base,
      "resource"      -> Schemas.resourceSchemaUri,
      "elasticsearch" -> nxv.defaultElasticIndex,
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

    val apiMappingsFinal = Map[String, AbsoluteIri](
      "nxc"             -> Contexts.base,
      "nxs"             -> Schemas.base,
      "resource"        -> Schemas.resourceSchemaUri,
      "schema"          -> Schemas.shaclSchemaUri,
      "view"            -> Schemas.viewSchemaUri,
      "resolver"        -> Schemas.resolverSchemaUri,
      "file"            -> Schemas.fileSchemaUri,
      "nxv"             -> nxv.base,
      "documents"       -> nxv.defaultElasticIndex,
      "graph"           -> nxv.defaultSparqlIndex,
      "defaultResolver" -> nxv.defaultResolver
    )

    val projectMetaResp =
      projectMeta.copy(apiMappings = projectMeta.apiMappings ++ apiMappingsFinal + ("base" -> projectMeta.base))

    "fetch the project from the cache" in {

      when(projectCache.getBy(label)).thenReturn(Task.pure(Some(projectMeta): Option[Project]))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[Project] shouldEqual projectMetaResp
      }
    }

    "fetch the project from admin client when not present on the cache" in {
      when(projectCache.getBy(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(Some(projectMeta): Option[Project]))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[Project] shouldEqual projectMetaResp
      }
    }

    "fetch the project from admin client when cache throws an error" in {
      when(projectCache.getBy(label)).thenReturn(Task.raiseError(new RuntimeException))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(Some(projectMeta): Option[Project]))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[Project] shouldEqual projectMetaResp
      }
    }

    "reject when not found neither in the cache nor doing IAM call" in {
      when(projectCache.getBy(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(None: Option[Project]))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[ProjectsNotFound.type]
      }
    }

    "reject when IAM signals UnauthorizedAccess" in {
      val label = ProjectLabel("organization", "project")
      when(projectCache.getBy(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project")).thenReturn(Task.raiseError(UnauthorizedAccess))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "reject when IAM signals another error" in {
      val label = ProjectLabel("organization", "project")
      when(projectCache.getBy(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project"))
        .thenReturn(Task.raiseError(new RuntimeException("Something went wrong")))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[Error].code shouldEqual classNameOf[DownstreamServiceError.type]
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
        responseAs[Error].code shouldEqual classNameOf[ProjectIsDeprecated.type]
      }
    }
  }
}
