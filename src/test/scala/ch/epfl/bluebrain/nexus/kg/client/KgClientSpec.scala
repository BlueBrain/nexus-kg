package ch.epfl.bluebrain.nexus.kg.client

import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.client.RequestBuilding.Get
import akka.http.scaladsl.model.ContentTypes.`application/json`
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.Accept
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import cats.effect.IO
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.RdfMediaTypes.`application/ld+json`
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.test.io.IOOptionValues
import ch.epfl.bluebrain.nexus.iam.client.config.IamClientConfig
import ch.epfl.bluebrain.nexus.iam.client.types.AuthToken
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.User
import ch.epfl.bluebrain.nexus.iam.client.{EventSource, IamClientError}
import ch.epfl.bluebrain.nexus.kg.client.KgClientError.NotFound
import ch.epfl.bluebrain.nexus.kg.config.Schemas
import ch.epfl.bluebrain.nexus.kg.resources.Event.JsonLd._
import ch.epfl.bluebrain.nexus.kg.resources.syntax._
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Id, ResourceF, ResourceV}
import ch.epfl.bluebrain.nexus.kg.{urlEncode, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.instances._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.mockito.{ArgumentMatchersSugar, IdiomaticMockito, Mockito}
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatest.{BeforeAndAfter, Inspectors}

import scala.concurrent.duration._

class KgClientSpec
    extends TestKit(ActorSystem("IamClientSpec"))
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfter
    with IdiomaticMockito
    with ArgumentMatchersSugar
    with IOOptionValues
    with Resources
    with Eventually
    with Inspectors
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5.seconds, 15.milliseconds)

  private val config =
    KgClientConfig(url"http://example.com/".value, "v1")

  private implicit val iamClientConfig: IamClientConfig =
    IamClientConfig(url"https://nexus.example.com".value, url"https://nexus.example.com".value, "v1")

  private val resClient: HttpClient[IO, ResourceV] = mock[HttpClient[IO, ResourceV]]
  private val accept                               = Accept(`application/json`.mediaType, `application/ld+json`)

  private val source = mock[EventSource[Event]]
  private val client = new KgClient[IO](config, source, _ => resClient)

  before {
    Mockito.reset(source, resClient)
  }

  trait Ctx {
    // format: off
    val project = Project(genIri, genString(), genString(), None, genIri, genIri, Map.empty, genUUID, genUUID, 1L, false, Instant.EPOCH, genIri, Instant.EPOCH, genIri)
    // format: on
    val id         = url"http://example.com/prefix/myId".value
    val resourceId = urlEncode(id)

    val json          = jsonContentOf("/serialization/resource.json")
    private val graph = json.asGraph(id).rightValue

    val model = ResourceF(
      Id(project.ref, url"http://example.com/prefix/myId".value),
      1L,
      Set(url"https://example.com/vocab/A".value, url"https://example.com/vocab/B".value),
      deprecated = false,
      Map.empty,
      None,
      Instant.parse("2020-01-17T12:45:01.479676Z"),
      Instant.parse("2020-01-17T13:45:01.479676Z"),
      User("john", "bbp"),
      User("brenda", "bbp"),
      Schemas.unconstrainedRef,
      ResourceF.Value(Json.obj(), Json.obj(), graph)
    )
    implicit val token: Option[AuthToken] = None

    val resourceEndpoint =
      s"http://example.com/v1/resources/${project.organizationLabel}/${project.label}/_/$resourceId?format=expanded"
    val eventsEndpoint = url"http://example.com/v1/resources/${project.organizationLabel}/${project.label}/events".value
  }

  "A KgClient" when {

    "fetching a resource" should {

      "succeed" in new Ctx {
        resClient(Get(resourceEndpoint).addHeader(accept)) shouldReturn IO.pure(model)
        client.resource(project, id).some shouldEqual model
      }

      "return None" in new Ctx {
        resClient(Get(resourceEndpoint).addHeader(accept)) shouldReturn IO.raiseError(NotFound(""))
        client.resource(project, id).ioValue shouldEqual None
      }

      "propagate the underlying exception" in new Ctx {
        val exs: List[Exception] = List(
          IamClientError.Unauthorized(""),
          IamClientError.Forbidden(""),
          KgClientError.UnmarshallingError(""),
          KgClientError.UnknownError(StatusCodes.InternalServerError, "")
        )
        forAll(exs) { ex =>
          resClient(Get(resourceEndpoint).addHeader(accept)) shouldReturn IO.raiseError(ex)
          client.resource(project, id).failed[Exception] shouldEqual ex
        }
      }
    }

    "fetching the event stream" should {
      def aggregate(source: Source[Event, NotUsed]): Vector[Event] =
        source.runFold(Vector.empty[Event])(_ :+ _).futureValue

      val eventsSource = Source(jsonContentOf("/events/events.json").as[List[Event]].rightValue)
      "succeed" in new Ctx {
        source(eventsEndpoint, None) shouldReturn eventsSource
        aggregate(client.events(project.projectLabel, None)) shouldEqual aggregate(eventsSource)
      }
    }
  }
}
