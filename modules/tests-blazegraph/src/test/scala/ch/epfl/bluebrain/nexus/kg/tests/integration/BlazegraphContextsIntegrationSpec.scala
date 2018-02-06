package ch.epfl.bluebrain.nexus.kg.tests.integration

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId, ContextRef}
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.routes.ContextRoutes.ContextConfig
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax._
import io.circe.{Encoder, Json}

import scala.collection.mutable.Map
import scala.concurrent.ExecutionContextExecutor

class BlazegraphContextsIntegrationSpec(apiUri: Uri, prefixes: PrefixUris, route: Route)(implicit
                                                                                         as: ActorSystem,
                                                                                         ec: ExecutionContextExecutor,
                                                                                         mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes) {

  import BootstrapIntegrationSpec._
  import contextEncoders._
  private implicit val e: Encoder[ContextConfig] = deriveEncoder[ContextConfig]

  "A ContextRoutes" when {
    "performing integration tests" should {
      val idsPayload = Map[ContextId, Context]()

      "create contexts successfully" in {
        forAll(contexts) {
          case (contextId, json) =>
            Put(s"/contexts/${contextId.show}", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
              idsPayload += (contextId -> Context(contextId, 2L, json, false, true))
              status shouldEqual StatusCodes.Created
              responseAs[Json] shouldEqual ContextRef(contextId, 1L).asJson.addCoreContext
            }
        }
      }
      "publish contexts successfully" in {
        forAll(contexts) {
          case (contextId, _) =>
            Patch(s"/contexts/${contextId.show}/config?rev=1", ContextConfig(published = true)) ~> addCredentials(
              ValidCredentials) ~> route ~> check {
              status shouldEqual StatusCodes.OK
              responseAs[Json] shouldEqual ContextRef(contextId, 2L).asJson.addCoreContext
            }
        }
      }

      "deprecate one context on rand organization" in {
        val (contextId, _) = contextsForOrg(contexts, "rand").head
        Delete(s"/contexts/${contextId.show}?rev=2") ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual ContextRef(contextId, 3L).asJson.addCoreContext
        }

      }
    }
  }

  private def contextsForOrg(ctxs: List[(ContextId, Json)], org: String): List[(ContextId, Json)] =
    ctxs
      .filter(c => c._1.domainId.orgId.id.equals(org))
      .sortWith(_._1.show < _._1.show)
}
