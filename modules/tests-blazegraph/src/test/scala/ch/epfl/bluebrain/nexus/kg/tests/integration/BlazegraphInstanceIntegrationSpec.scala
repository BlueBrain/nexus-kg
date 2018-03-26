package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.net.URLEncoder
import java.time.Clock

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.Source
import akka.stream.{ActorMaterializer, IOResult}
import akka.util.ByteString
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AuthenticatedCaller
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidator
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId, InstanceRef, Instances}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.syntax._
import org.scalatest.DoNotDiscover
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

@DoNotDiscover
class BlazegraphInstanceIntegrationSpec(
    apiUri: Uri,
    prefixes: PrefixUris,
    route: Route,
    instancesService: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
    validator: ShaclValidator[Future])(implicit
                                       as: ActorSystem,
                                       ec: ExecutionContextExecutor,
                                       mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes) {

  import BootstrapIntegrationSpec._
  import instanceEncoders._
  import schemaEncoders.{
    idWithLinksEncoder => schemaIdEncoder,
    queryResultEncoder => squeryResultEncoder,
    scoredQueryResultEncoder => sscoredQueryResultEncoder
  }

  "A InstanceRoutes" when {

    "performing integration tests" should {
      val idsPayload = Map[InstanceId, Instance]()

      val caller = CallerCtx(Clock.systemUTC, AuthenticatedCaller(None, mockedUser.identities))

      lazy val instances =
        pendingInstances
          .foldLeft(List.empty[(InstanceId, Json)]) {
            case (acc, (InstanceId(schemaId, _), aJson)) =>
              val json = acc match {
                case (prevId, _) :: _ =>
                  aJson deepMerge Json.obj("hasPart" -> Json.obj("@id" -> Json.fromString(prevId.qualifyAsString)))
                case _ => aJson
              }
              val ref = Await.result(instancesService.create(schemaId, json)(caller, validator), 5 second)
              idsPayload += (ref.id -> Instance(ref.id, 1L, json, None, false))
              (ref.id               -> json) :: acc
          }
          .sortWith(_._1.show < _._1.show)

      "list instances on organization nexus with full text search" in {
        val (instanceId, _) = instances.head
        val path            = s"/data/nexus?q=${instanceId.id}"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val json            = responseAs[Json]
            val score           = json.hcursor.get[Float]("maxScore").toOption.getOrElse(1F)
            val expectedResults = ScoredQueryResults(1L, score, List(ScoredQueryResult(score, instanceId)))
            val expectedLinks   = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
            json shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val (instanceId, _) = instances.head
        val path            = s"/data/nexus?q=${instanceId.id}&size=3&from=200"
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val json            = responseAs[Json]
            val score           = json.hcursor.get[Float]("maxScore").toOption.getOrElse(1F)
            val expectedResults = ScoredQueryResults(1L, score, List.empty[ScoredQueryResult[SchemaId]])
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self"     -> s"$apiUri$path",
                                      "previous" -> s"$apiUri$path".replace("from=200", "from=0"))
            json shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list instances with filter on 'desc' path" in {
        val (instanceId, json) = instances.head
        val desc               = json.hcursor.get[String]("nexusvoc:desc").toOption.getOrElse("")
        val uriContext         = URLEncoder.encode("""{"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}""", "UTF-8")
        val uriFilter =
          URLEncoder.encode(s"""{"path": "http://localhost/v0/voc/nexus/core/desc", "op": "eq", "value": "$desc"}""",
                            "UTF-8")
        val path = s"/data?context=$uriContext&filter=$uriFilter"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(instanceId)))
          val expectedLinks   = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "deprecate one instance on nexus organization and domain development" in {
        val (instanceId, _) = instances.head
        Delete(s"/data/${instanceId.show}?rev=1") ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          responseAs[Json] shouldEqual InstanceRef(instanceId, 2L).asJson.addCoreContext
        }
      }

      "list outgoing instances on one instance" in {
        val (instanceId, json) = instances.collectFirst {
          case tuple @ (_, j) if j.hcursor.get[Json]("hasPart").toOption.isDefined => tuple
        }.get
        val outgoingId: InstanceId =
          json.hcursor.downField("hasPart").get[String]("@id").toOption.flatMap(_.unqualify[InstanceId]).get
        val path = s"/data/${instanceId.show}/outgoing"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(outgoingId)))
          val expectedLinks   = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list incoming instances on one instance" in {
        val (instanceId, json) = instances.collectFirst {
          case tuple @ (_, j) if j.hcursor.get[Json]("hasPart").toOption.isDefined => tuple
        }.get
        val outgoingId: InstanceId =
          json.hcursor.downField("hasPart").get[String]("@id").toOption.flatMap(_.unqualify[InstanceId]).get
        val path = s"/data/${outgoingId.show}/incoming"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults = UnscoredQueryResults(1L, List(UnscoredQueryResult(instanceId)))
          val expectedLinks   = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }
    }
  }
}
