package ch.epfl.bluebrain.nexus.kg.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.AdminClient
import ch.epfl.bluebrain.nexus.admin.client.types.{ServiceDescription => AdminServiceDescription}
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticSearchClient, ServiceDescription => EsServiceDescription}
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{untyped, withUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.sparql.client.{
  BlazegraphClient,
  ServiceDescription => BlazegraphServiceDescription
}
import ch.epfl.bluebrain.nexus.iam.client.IamClient
import ch.epfl.bluebrain.nexus.iam.client.types.{ServiceDescription => IamServiceDescription}
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.marshallers.instances._
import ch.epfl.bluebrain.nexus.kg.routes.AppInfoRoutes.{ServiceDescription, StatusGroup}
import ch.epfl.bluebrain.nexus.kg.routes.Status._
import ch.epfl.bluebrain.nexus.storage.client.StorageClient
import ch.epfl.bluebrain.nexus.storage.client.types.{ServiceDescription => StorageServiceDescription}
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import monix.eval.Task
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest.{BeforeAndAfter, Matchers, WordSpecLike}

class AppInfoRoutesSpec
    extends WordSpecLike
    with Matchers
    with IdiomaticMockito
    with BeforeAndAfter
    with ScalatestRouteTest {

  private implicit val appConfig     = Settings(system).appConfig
  private implicit val ec            = system.dispatcher
  private implicit val utClient      = untyped[Task]
  private implicit val qrClient      = withUnmarshaller[Task, QueryResults[Json]]
  private implicit val jsonClient    = withUnmarshaller[Task, Json]
  private implicit val iam           = mock[IamClient[Task]]
  private implicit val admin         = mock[AdminClient[Task]]
  private implicit val elasticSearch = mock[ElasticSearchClient[Task]]
  private implicit val sparql        = mock[BlazegraphClient[Task]]
  private implicit val storage       = mock[StorageClient[Task]]
  private val statusGroup            = StatusGroup(mock[CassandraStatus], mock[ClusterStatus])
  private implicit val clients       = Clients()
  private val routes                 = AppInfoRoutes(appConfig.description, statusGroup).routes

  before {
    Mockito.reset(statusGroup.cluster, iam, admin, elasticSearch, sparql, storage, statusGroup.cassandra)
  }

  "An AppInfoRoutes" should {
    "return the service description" in {
      Get("/") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[ServiceDescription] shouldEqual ServiceDescription(
          appConfig.description.name,
          appConfig.description.version
        )
      }
    }

    "return the status when everything is up" in {
      statusGroup.cassandra.check shouldReturn Task.pure(true)
      statusGroup.cluster.check shouldReturn Task.pure(true)
      Get("/status") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "cassandra" -> Json.fromString("up"),
          "cluster"   -> Json.fromString("up")
        )
      }
    }

    "return the status when everything is down" in {
      statusGroup.cassandra.check shouldReturn Task.pure(false)
      statusGroup.cluster.check shouldReturn Task.pure(false)
      Get("/status") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "cassandra" -> Json.fromString("inaccessible"),
          "cluster"   -> Json.fromString("inaccessible")
        )
      }
    }

    "return the version when everything is up" in {
      val iamServiceDesc        = IamServiceDescription("iam", "1.1.0")
      val adminServiceDesc      = AdminServiceDescription("admin", "1.1.1")
      val storageServiceDesc    = StorageServiceDescription("storage", "1.1.2")
      val esServiceDesc         = EsServiceDescription("elasticsearch", "1.1.3")
      val blazegraphServiceDesc = BlazegraphServiceDescription("blazegraph", "1.1.4")
      iam.serviceDescription shouldReturn Task(iamServiceDesc)
      admin.serviceDescription shouldReturn Task(adminServiceDesc)
      storage.serviceDescription shouldReturn Task(storageServiceDesc)
      elasticSearch.serviceDescription shouldReturn Task(esServiceDesc)
      sparql.serviceDescription shouldReturn Task(blazegraphServiceDesc)
      Get("/version") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "nexus"                    -> appConfig.description.version.asJson,
          appConfig.description.name -> appConfig.description.version.asJson,
          adminServiceDesc.name      -> adminServiceDesc.version.asJson,
          storageServiceDesc.name    -> storageServiceDesc.version.asJson,
          iamServiceDesc.name        -> iamServiceDesc.version.asJson,
          blazegraphServiceDesc.name -> blazegraphServiceDesc.version.asJson,
          esServiceDesc.name         -> esServiceDesc.version.asJson
        )
      }
    }

    "return the version when everything some services are unreachable" in {
      val iamServiceDesc   = IamServiceDescription("iam", "1.1.0")
      val adminServiceDesc = AdminServiceDescription("admin", "1.1.1")
      val esServiceDesc    = EsServiceDescription("elasticsearch", "1.1.3")
      iam.serviceDescription shouldReturn Task(iamServiceDesc)
      admin.serviceDescription shouldReturn Task(adminServiceDesc)
      storage.serviceDescription shouldReturn Task.raiseError(new RuntimeException())
      elasticSearch.serviceDescription shouldReturn Task(esServiceDesc)
      sparql.serviceDescription shouldReturn Task.raiseError(new RuntimeException())
      Get("/version") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
        responseAs[Json] shouldEqual Json.obj(
          "nexus"                    -> appConfig.description.version.asJson,
          appConfig.description.name -> appConfig.description.version.asJson,
          adminServiceDesc.name      -> adminServiceDesc.version.asJson,
          "remoteStorage"            -> "unknown".asJson,
          iamServiceDesc.name        -> iamServiceDesc.version.asJson,
          "blazegraph"               -> "unknown".asJson,
          esServiceDesc.name         -> esServiceDesc.version.asJson
        )
      }
    }
  }

}
