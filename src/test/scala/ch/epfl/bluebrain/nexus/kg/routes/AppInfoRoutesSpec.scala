package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.Organization
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.sparql.client.BlazegraphClient
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlLists, AuthToken}
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes.{HealthStatusGroup, ServiceDescription}
import ch.epfl.bluebrain.nexus.kg.routes.HealthStatus._
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import io.circe.Json
import io.circe.generic.auto._
import monix.eval.Task
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

class AppInfoRoutesSpec
    extends WordSpecLike
    with Matchers
    with MockitoSugar
    with BeforeAndAfter
    with ScalatestRouteTest {

  private implicit val appConfig = Settings(system).appConfig
  private val iam                = mock[IamClient[Task]]
  private val admin              = mock[AdminClient[Task]]
  private val elastic            = mock[ElasticClient[Task]]
  private val sparql             = mock[BlazegraphClient[Task]]
  private val statusGroup = HealthStatusGroup(
    mock[CassandraHealthStatus],
    mock[ClusterHealthStatus],
    new IamHealthStatus(iam),
    new AdminHealthStatus(admin),
    new ElasticSearchHealthStatus(elastic),
    new SparqlHealthStatus(sparql)
  )
  private val routes                            = AppInfoRoutes(appConfig.description, statusGroup).routes
  private implicit val token: Option[AuthToken] = None

  before {
    Mockito.reset(statusGroup.cluster, iam, admin, elastic, sparql, statusGroup.cassandra)
  }

  "An AppInfoRoutes" should {
    "return the service description" in {
      Get("/") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[ServiceDescription] shouldEqual ServiceDescription(appConfig.description.name,
                                                                      appConfig.description.version)
      }
    }

    "return the health status when everything is up" in {
      when(statusGroup.cassandra.check).thenReturn(Task.pure(true))
      when(statusGroup.cluster.check).thenReturn(Task.pure(true))
      when(iam.acls(/)).thenReturn(Task.pure(AccessControlLists()))
      when(admin.fetchOrganization("test")).thenReturn(Task.pure[Option[Organization]](None))
      when(elastic.existsIndex("test")).thenReturn(Task.pure(false))
      when(sparql.namespaceExists).thenReturn(Task.pure(false))
      Get("/health") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "cassandra" -> Json.fromString("up"),
          "cluster"   -> Json.fromString("up"),
          "iam"       -> Json.fromString("up"),
          "admin"     -> Json.fromString("up"),
          "elastic"   -> Json.fromString("up"),
          "sparql"    -> Json.fromString("up")
        )
      }
    }

    "return the health status when everything is down" in {
      when(statusGroup.cassandra.check).thenReturn(Task.pure(false))
      when(statusGroup.cluster.check).thenReturn(Task.pure(false))
      when(iam.acls(/)).thenReturn(Task.raiseError(new RuntimeException()))
      when(admin.fetchOrganization("test")).thenReturn(Task.raiseError(new RuntimeException()))
      when(elastic.existsIndex("test")).thenReturn(Task.raiseError(new RuntimeException()))
      when(sparql.namespaceExists).thenReturn(Task.raiseError(new RuntimeException()))
      Get("/health") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "cassandra" -> Json.fromString("inaccessible"),
          "cluster"   -> Json.fromString("inaccessible"),
          "iam"       -> Json.fromString("inaccessible"),
          "admin"     -> Json.fromString("inaccessible"),
          "elastic"   -> Json.fromString("inaccessible"),
          "sparql"    -> Json.fromString("inaccessible")
        )
      }
    }
  }

}
