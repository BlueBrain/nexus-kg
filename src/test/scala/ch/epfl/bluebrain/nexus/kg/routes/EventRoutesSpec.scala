package ch.epfl.bluebrain.nexus.kg.routes

import java.nio.file.Paths
import java.time.Instant
import java.util.UUID

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.`Last-Event-ID`
import akka.http.scaladsl.model.sse.ServerSentEvent
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.persistence.query.{EventEnvelope, NoOffset, Offset, Sequence}
import akka.stream.scaladsl.Source
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.User
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.config.{AppConfig, Settings}
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes}
import ch.epfl.bluebrain.nexus.kg.resources.{Event, Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.routes.EventRoutesSpec.TestableEventRoutes
import ch.epfl.bluebrain.nexus.rdf.Iri.Path
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import org.mockito.IdiomaticMockito
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class EventRoutesSpec
    extends WordSpecLike
    with Matchers
    with ScalatestRouteTest
    with BeforeAndAfter
    with MacroBasedMatchers
    with Resources
    with ScalaFutures
    with OptionValues
    with EitherValues
    with Inspectors
    with IdiomaticMockito {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3 second, 100 milliseconds)

  override def testConfig: Config = ConfigFactory.load("test.conf")

  val base = url"http://example.com/base".value

  val instant = Instant.EPOCH
  val subject = User("uuid", "myrealm")
  val acls = AccessControlLists(
    Path./ -> ResourceAccessControlList(
      base + UUID.randomUUID().toString,
      2L,
      Set(base + "AccessControlList"),
      instant,
      subject,
      instant,
      subject,
      AccessControlList(subject -> Set(Permission.unsafe("resources/read")))
    ))
  val caller = Caller(subject, Set(subject))

  val projectUuid = UUID.randomUUID()
  val schemaRef   = Latest(base + "schema")
  val types       = Set(base + "type")

  private implicit val appConfig = Settings(system).appConfig

  private implicit val project = Project(
    base + "org" + "project",
    "project",
    "org",
    None,
    base,
    base + "vocab",
    Map(),
    UUID.randomUUID(),
    UUID.randomUUID(),
    1L,
    false,
    instant,
    base + "subject",
    instant,
    base + "subject"
  )

  val events = List(
    Created(
      Id(ProjectRef(projectUuid), base + "Created"),
      schemaRef,
      types,
      Json.obj(
        "@type" -> Json.fromString("created")
      ),
      instant,
      subject
    ),
    Updated(Id(ProjectRef(projectUuid), base + "created"),
            2L,
            types,
            Json.obj(
              "@type" -> Json.fromString("Updated")
            ),
            instant,
            subject),
    Deprecated(
      Id(ProjectRef(projectUuid), base + "created"),
      3L,
      types,
      instant,
      subject
    ),
    TagAdded(
      Id(ProjectRef(projectUuid), base + "created"),
      4L,
      2L,
      "v1.0.0",
      instant,
      subject
    ),
    FileCreated(
      Id(ProjectRef(projectUuid), base + "file"),
      FileAttributes(
        Paths.get("/", UUID.randomUUID().toString, UUID.randomUUID().toString),
        "attachment.json",
        "application/json",
        47,
        Digest("SHA-256", "00ff4b34e3f3695c3abcdec61cba72c2238ed172ef34ae1196bfad6a4ec23dda")
      ),
      instant,
      subject
    ),
    FileUpdated(
      Id(ProjectRef(projectUuid), base + "file"),
      2L,
      FileAttributes(
        Paths.get("/", UUID.randomUUID().toString, UUID.randomUUID().toString),
        "attachment.json",
        "text/json",
        47,
        Digest("SHA-256", "00ff4b34e3f3695c3abcdec61cba72c2238ed172ef34ae1196bfad6a4ec23dda")
      ),
      instant,
      subject
    )
  )

  val routes = new TestableEventRoutes(events, acls, caller).routes

  "EventRoutes" should {

    "return all events for a project" in {
      Get("/") ~> routes ~> check {
        val expected = jsonContentOf("/events/events.json").asArray.value
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual eventStreamFor(expected)
      }
    }

    "return all events for a project from the last seen" in {
      Get("/").addHeader(`Last-Event-ID`(0.toString)) ~> routes ~> check {
        val expected = jsonContentOf("/events/events.json").asArray.value
        status shouldEqual StatusCodes.OK
        responseAs[String] shouldEqual eventStreamFor(expected, 1)
      }
    }
  }

  def eventStreamFor(jsons: Vector[Json], drop: Int = 0): String =
    jsons.zipWithIndex
      .drop(drop)
      .map {
        case (json, idx) =>
          val data  = json.noSpaces
          val event = json.hcursor.get[String]("@type").right.value
          val id    = idx
          s"""data:$data
             |event:$event
             |id:$id""".stripMargin
      }
      .mkString("", "\n\n", "\n\n")

}

object EventRoutesSpec {

  class TestableEventRoutes(events: List[Event], acls: AccessControlLists, caller: Caller)(implicit as: ActorSystem,
                                                                                           project: Project,
                                                                                           config: AppConfig)
      extends EventRoutes(acls, caller) {

    private val envelopes = events.zipWithIndex.map {
      case (ev, idx) =>
        EventEnvelope(Sequence(idx.toLong), "persistenceid", 1l, ev)
    }

    override protected def source(
        tag: String,
        offset: Offset,
        toSse: EventEnvelope => Option[ServerSentEvent]
    ): Source[ServerSentEvent, NotUsed] = {
      val toDrop = offset match {
        case NoOffset    => 0
        case Sequence(v) => v + 1
      }
      Source(envelopes).drop(toDrop).flatMapConcat(ee => Source(toSse(ee).toList))
    }
  }
}
