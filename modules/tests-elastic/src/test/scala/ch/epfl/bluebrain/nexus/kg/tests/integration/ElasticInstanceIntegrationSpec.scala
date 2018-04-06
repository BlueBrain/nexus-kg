package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.time.Clock

import akka.actor.ActorSystem
import akka.http.scaladsl.model.headers.OAuth2BearerToken
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
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.syntax._
import org.scalatest.DoNotDiscover
import org.scalatest.time.{Minutes, Seconds, Span}

import scala.collection.mutable.Map
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

@DoNotDiscover
class ElasticInstanceIntegrationSpec(
    apiUri: Uri,
    prefixes: PrefixUris,
    route: Route,
    instancesService: Instances[Future, Source[ByteString, Any], Source[ByteString, Future[IOResult]]],
    validator: ShaclValidator[Future])(implicit
                                       as: ActorSystem,
                                       ec: ExecutionContextExecutor,
                                       mt: ActorMaterializer,
                                       instanceImportResolver: InstanceImportResolver[Future])
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
              val ref = Await.result(instancesService.create(schemaId, json)(caller, validator, instanceImportResolver),
                                     5 second)
              idsPayload += (ref.id -> Instance(ref.id, 1L, json, None, deprecated = false))
              (ref.id               -> json) :: acc
          }
          .sortWith(_._1.show < _._1.show)
      lazy val randInstances = instances.collect {
        case tuple @ (InstanceId(SchemaId(DomainId(OrgId(org), _), _, _), _), _) if org == "rand" => tuple
      }
      "list all instances" in {
        eventually(timeout(Span(1, Minutes)), interval(Span(1, Seconds))) {
          instances.length shouldEqual (schemas.length - 1) * 5
        }
        eventually(timeout(Span(indexTimeout + 5, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/data") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults = UnscoredQueryResults(instances.length.toLong, instances.take(10).map {
              case (id, _) => UnscoredQueryResult(id)
            })
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri/data",
                                      "next" -> s"$apiUri/data?from=10&size=10")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list all instances for user only with access to rand organization" in {
        eventually(timeout(Span(indexTimeout + 5, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/data") ~> addCredentials(OAuth2BearerToken("org-rand")) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(randInstances.length.toLong,
                                   randInstances.map { case (id, _) => UnscoredQueryResult(id) }.take(10))
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                      "self" -> s"$apiUri/data",
                                      "next" -> s"$apiUri/data?from=10&size=10")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list all instances for user only with access to a domain " in {
        val domainId = schemas.head._1.domainId
        eventually(timeout(Span(indexTimeout + 5, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/data") ~> addCredentials(OAuth2BearerToken(s"domain-${domainId.show}")) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val resultInstances = instances.map(_._1).filter(id => id.schemaId.domainId == domainId)
            val expectedResults =
              UnscoredQueryResults(resultInstances.length.toLong, resultInstances.map(UnscoredQueryResult(_)).take(10))
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> s"$apiUri/data")
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list instances on organization rand with pagination" in {
        val pagination = Pagination(0L, 5)
        val path       = s"/data/rand?size=${pagination.size}"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(randInstances.length.toLong,
                                 randInstances.map { case (id, _) => UnscoredQueryResult(id) }.take(pagination.size))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                    "self" -> s"$apiUri$path",
                                    "next" -> s"$apiUri$path&from=5")
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list instances on organization rand with pagination retrieving full entities" in {
        val pagination = Pagination(0L, 5)
        val path       = s"/data/rand?size=${pagination.size}&fields=all"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(
              randInstances.length.toLong,
              randInstances.map { case (id, _) => UnscoredQueryResult(idsPayload(id)) }.take(pagination.size))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                    "self" -> s"$apiUri$path",
                                    "next" -> s"$apiUri$path&from=5")
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination = Pagination(0L, 5)
        val path       = s"/data/rand?size=${pagination.size}&from=500"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(randInstances.length.toLong, List.empty[UnscoredQueryResult[SchemaId]])
          val expectedLinks =
            Links("@context" -> s"${prefixes.LinksContext}",
                  "self"     -> s"$apiUri$path",
                  "previous" -> s"$apiUri$path".replace("from=500", s"from=${randInstances.length - 5}"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list instances on organization nexus and domain development and specific schema name" in {
        val (schemaId, _) = schemas.head
        val path          = s"/data/${schemaId.schemaName.show}"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(10L, instances.take(10).map { case (id, _) => UnscoredQueryResult(id) })
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list instances for one schemaId" in {
        val (instanceId, _) = instances.head
        val path            = s"/data/${instanceId.schemaId.show}"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(5L, instances.take(5).map { case (id, _) => UnscoredQueryResult(id) })
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
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

      "list instances on nexus organization and domain development that are not deprecated" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          val path = s"/data/nexus/development?deprecated=false"
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(9L, instances.slice(1, 10).map { case (id, _) => UnscoredQueryResult(id) })
            val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

    }
  }
}
