package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.net.URLEncoder
import java.time.Instant

import akka.actor.ActorSystem
import akka.http.scaladsl.model.{StatusCodes, Uri}
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event.PermissionsAdded
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.iam.acls._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.UserRef
import ch.epfl.bluebrain.nexus.commons.sparql.client.SparqlCirceSupport._
import ch.epfl.bluebrain.nexus.commons.types.search.Pagination
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.{ScoredQueryResult, UnscoredQueryResult}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.{ScoredQueryResults, UnscoredQueryResults}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef, Organization}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.indexing.acls.AclIndexer
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Links
import ch.epfl.bluebrain.nexus.kg.service.io.PrinterSettings._
import ch.epfl.bluebrain.nexus.kg.service.query.LinksQueryResults
import io.circe.Json
import io.circe.syntax._
import org.scalatest._
import org.scalatest.time.{Seconds, Span}

import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContextExecutor, Future}

@DoNotDiscover
class OrgIntegrationSpec(apiUri: Uri, prefixes: PrefixUris, route: Route, aclIndexer: AclIndexer[Future])(
    implicit
    as: ActorSystem,
    ec: ExecutionContextExecutor,
    mt: ActorMaterializer)
    extends BootstrapIntegrationSpec(apiUri, prefixes) {

  import BootstrapIntegrationSpec._
  import orgsEncoders._

  "A OrganizationRoutes" when {
    val meta = Meta(mockedUser.identities.find {
      case _: UserRef => true
      case _          => false
    }.get, Instant.ofEpochMilli(0L))

    "performing integration tests" should {
      val idsPayload = Map[OrgId, Organization]()
      "create organizations successfully" in {
        forAll(orgs) { orgId =>
          val json = Json.obj("key" -> Json.fromString(genString()))
          Put(s"/organizations/${orgId.show}", json) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.Created
            idsPayload += (orgId -> Organization(orgId, 1L, json, false))
            responseAs[Json] shouldEqual OrgRef(orgId, 1L).asJson.addCoreContext
          }
        }
      }

      "retrieve organizations successfully" in {
        forAll(orgs) { orgId =>
          Get(s"/organizations/${orgId.show}") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          }
        }
      }

      "add read permissions for root level" in {
        forAll(orgs) { orgId =>
          eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
            aclIndexer(
              PermissionsAdded("kg" / orgId.id,
                               AccessControlList(mockedUser.identities.map(AccessControl(_, Permissions(Read)))),
                               meta)).futureValue
          }
        }
      }

      "list organizations" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/organizations") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(orgs.length.toLong, orgs.map {
                UnscoredQueryResult(_)
              })
            val expectedLinks =
              Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri/organizations"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list organizations with all the fields" in {
        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(s"/organizations?fields=all") ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(orgs.length.toLong, orgs.map { id =>
                UnscoredQueryResult(idsPayload(id))
              })
            val expectedLinks =
              Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri/organizations?fields=all"))
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "list organizations with pagination" in {
        val pagination = Pagination(1L, 1)
        val path =
          s"/organizations?from=${pagination.from}&size=${pagination.size}"

        eventually(timeout(Span(indexTimeout, Seconds)), interval(Span(1, Seconds))) {
          Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
            status shouldEqual StatusCodes.OK
            contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
            val expectedResults =
              UnscoredQueryResults(orgs.length.toLong, List(UnscoredQueryResult(orgs(1))))
            val expectedLinks = Links(
              "@context" -> s"${prefixes.LinksContext}",
              "self"     -> s"$apiUri$path",
              "previous" -> s"$apiUri$path".replace("from=1", "from=0"),
              "next"     -> s"$apiUri$path".replace("from=1", "from=2")
            )
            responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
          }
        }
      }

      "output the correct total even when the from query parameter is out of scope" in {
        val pagination = Pagination(0L, 5)
        val path       = s"/organizations?size=${pagination.size}&from=100"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(orgs.length.toLong, List.empty[UnscoredQueryResult[OrgId]])
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                    "self"     -> s"$apiUri$path",
                                    "previous" -> s"$apiUri$path".replace("from=100", s"from=0"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list organization with full text search" in {
        val path = s"/organizations?q=${orgs.head.id}"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            ScoredQueryResults(1L, 1F, List(ScoredQueryResult(1F, orgs.head)))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "output the correct total in full text search even when the from query parameter is out of scope" in {
        val path = s"/organizations?q=${orgs.head.id}&size=3&from=200"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            ScoredQueryResults(1L, 1F, List.empty[ScoredQueryResult[OrgId]])
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}",
                                    "self"     -> s"$apiUri$path",
                                    "previous" -> s"$apiUri$path".replace("from=200", "from=0"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list organizations with filter on 'path' organization " in {
        val uriContext = URLEncoder.encode("""{"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"}""", "UTF-8")
        val uriFilter  = URLEncoder.encode(s"""{"path": "${"name".qualify}", "op": "eq", "value": "nexus"}""", "UTF-8")
        val path       = s"/organizations?context=$uriContext&filter=$uriFilter"
        Get(path) ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(1L, List(UnscoredQueryResult(orgs.head)))
          val expectedLinks = Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri$path"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }

      "list organizations with deprecation" in {
        Get(s"/organizations?deprecated=false") ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(orgs.length.toLong, orgs.map {
              UnscoredQueryResult(_)
            })
          val expectedLinks =
            Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri/organizations?deprecated=false"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
        Get(s"/organizations?deprecated=true") ~> addCredentials(ValidCredentials) ~> route ~> check {
          status shouldEqual StatusCodes.OK
          contentType shouldEqual RdfMediaTypes.`application/ld+json`.toContentType
          val expectedResults =
            UnscoredQueryResults(0, List.empty[UnscoredQueryResult[OrgId]])
          val expectedLinks =
            Links("@context" -> s"${prefixes.LinksContext}", "self" -> Uri(s"$apiUri/organizations?deprecated=true"))
          responseAs[Json] shouldEqual LinksQueryResults(expectedResults, expectedLinks).asJson.addSearchContext
        }
      }
    }
  }
}
