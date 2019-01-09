package ch.epfl.bluebrain.nexus.kg.directives

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.commons.types.HttpRejection.UnauthorizedAccess
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.kg.{Error, TestHelper}
import ch.epfl.bluebrain.nexus.kg.Error._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.{Contexts, Schemas}
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.directives.ProjectDirectives._
import ch.epfl.bluebrain.nexus.kg.marshallers.RejectionHandling
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.resources.{OrganizationRef, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
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

  private implicit val cache                   = mock[DistributedCache[Task]]
  private implicit val client                  = mock[AdminClient[Task]]
  private implicit val cred: Option[AuthToken] = None

  before {
    Mockito.reset(cache)
    Mockito.reset(client)
  }

  private val id = genIri

  private implicit val projectDecoder: Decoder[Project] =
    Decoder.instance { hc =>
      for {
        organization <- hc.get[String]("organization")
        description  <- hc.getOrElse[Option[String]]("description")(None)
        base         <- hc.get[AbsoluteIri]("base")
        apiMap       <- hc.get[Map[String, AbsoluteIri]]("apiMappings")
        label        <- hc.get[String]("label")
        uuid         <- hc.get[String]("uuid").map(UUID.fromString)
        rev          <- hc.get[Long]("rev")
        deprecated   <- hc.get[Boolean]("deprecated")
        createdBy    <- hc.get[AbsoluteIri]("createdBy")
        createdAt    <- hc.get[Instant]("createdAt")
        updatedBy    <- hc.get[AbsoluteIri]("updatedBy")
        updatedAt    <- hc.get[Instant]("updatedAt")
      } yield
        Project(id,
                label,
                organization,
                description,
                base,
                apiMap,
                uuid,
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

    def projectNotDepRoute(implicit proj: Project, ref: ProjectLabel): Route =
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
      apiMappings,
      genUUID,
      1L,
      false,
      Instant.EPOCH,
      creator,
      Instant.EPOCH,
      creator
    )

    val apiMappingsFinal = Map[String, AbsoluteIri](
      "nxv"           -> nxv.base,
      "nxs"           -> Schemas.base,
      "nxc"           -> Contexts.base,
      "resource"      -> Schemas.resourceSchemaUri,
      "elasticsearch" -> nxv.defaultElasticIndex,
      "base"          -> nxv.projects,
      "documents"     -> nxv.defaultElasticIndex,
      "graph"         -> nxv.defaultSparqlIndex
    )
    val projectMetaResp = projectMeta.copy(apiMappings = apiMappingsFinal)

    val organizationRef = OrganizationRef(genUUID)

    val projectRef = ProjectRef(projectMeta.uuid)

    "fetch the project from the cache" in {

      when(cache.project(label)).thenReturn(Task.pure(Some(projectMeta): Option[Project]))
      when(cache.organizationRef(projectRef)).thenReturn(Task.pure(Some(organizationRef): Option[OrganizationRef]))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[LabeledProject] shouldEqual LabeledProject(label, projectMetaResp, organizationRef)
      }
    }

    "fetch the project from admin client when not present on the cache" in {
      when(cache.project(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(Some(projectMeta): Option[Project]))
      when(cache.organizationRef(projectRef)).thenReturn(Task.pure(Some(organizationRef): Option[OrganizationRef]))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[LabeledProject] shouldEqual LabeledProject(label, projectMetaResp, organizationRef)
      }
    }

    "fetch the project from admin client when cache throws an error" in {
      when(cache.project(label)).thenReturn(Task.raiseError(new RuntimeException))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(Some(projectMeta): Option[Project]))
      when(cache.organizationRef(projectRef)).thenReturn(Task.pure(Some(organizationRef): Option[OrganizationRef]))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[LabeledProject] shouldEqual LabeledProject(label, projectMetaResp, organizationRef)
      }
    }

    "fetch organization from admin when not found on cache" in {
      when(cache.project(label)).thenReturn(Task.raiseError(new RuntimeException))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(Some(projectMeta): Option[Project]))
      when(cache.organizationRef(projectRef)).thenReturn(Task.pure(None: Option[OrganizationRef]))
      val organization = Organization(genIri,
                                      "organization",
                                      "description",
                                      organizationRef.id,
                                      1L,
                                      false,
                                      Instant.EPOCH,
                                      creator,
                                      Instant.EPOCH,
                                      creator)
      when(client.fetchOrganization("organization")).thenReturn(Task.pure(Option(organization)))

      Get("/organization/project") ~> projectRoute() ~> check {
        responseAs[LabeledProject] shouldEqual LabeledProject(label, projectMetaResp, organizationRef)
      }
    }

    "reject when organization ref not found neither on the cache nor in admin" in {
      when(cache.project(label)).thenReturn(Task.pure(Some(projectMeta): Option[Project]))
      when(cache.organizationRef(projectRef)).thenReturn(Task.pure(None: Option[OrganizationRef]))
      when(client.fetchOrganization("organization")).thenReturn(Task.pure(None: Option[Organization]))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[OrganizationNotFound.type]
      }
    }

    "reject when not found neither in the cache nor doing IAM call" in {
      when(cache.project(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project")).thenReturn(Task.pure(None: Option[Project]))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.NotFound
        responseAs[Error].code shouldEqual classNameOf[ProjectsNotFound.type]
      }
    }

    "reject when IAM signals UnauthorizedAccess" in {
      val label = ProjectLabel("organization", "project")
      when(cache.project(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project")).thenReturn(Task.raiseError(UnauthorizedAccess))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.Unauthorized
        responseAs[Error].code shouldEqual classNameOf[UnauthorizedAccess.type]
      }
    }

    "reject when IAM signals another error" in {
      val label = ProjectLabel("organization", "project")
      when(cache.project(label)).thenReturn(Task.pure(None: Option[Project]))
      when(client.fetchProject("organization", "project"))
        .thenReturn(Task.raiseError(new RuntimeException("Something went wrong")))

      Get("/organization/project") ~> projectRoute() ~> check {
        status shouldEqual StatusCodes.BadGateway
        responseAs[Error].code shouldEqual classNameOf[DownstreamServiceError.type]
      }
    }

    "pass when available project is not deprecated" in {
      Get("/") ~> projectNotDepRoute(projectMeta, label) ~> check {
        status shouldEqual StatusCodes.OK
      }
    }

    "reject when available project is deprecated" in {
      Get("/") ~> projectNotDepRoute(projectMeta.copy(deprecated = true), label) ~> check {
        status shouldEqual StatusCodes.BadRequest
        responseAs[Error].code shouldEqual classNameOf[ProjectIsDeprecated.type]
      }
    }
  }
}
