package ch.epfl.bluebrain.nexus.kg.routes

import java.nio.file.Paths
import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.User
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes}
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{DiskStorage, S3Credentials, S3Settings, S3Storage}
import ch.epfl.bluebrain.nexus.rdf.Iri.Path
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import org.mockito.IdiomaticMockito
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._

class EventsSpecBase
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
      AccessControlList(subject -> Set(Permission.unsafe("resources/read"), Permission.unsafe("events/read")))
    ))
  val caller = Caller(subject, Set(subject))

  val projectUuid = UUID.fromString("7f8039a0-3141-11e9-b210-d663bd873d93")
  val schemaRef   = Latest(base + "schema")
  val types       = Set(base + "type")

  implicit val appConfig = Settings(system).appConfig

  implicit val project = Project(
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
    deprecated = false,
    instant,
    base + "subject",
    instant,
    base + "subject"
  )

  val projectRef = ProjectRef(projectUuid)

  val events = List(
    Created(
      Id(projectRef, base + "Created"),
      schemaRef,
      types,
      Json.obj(
        "@type" -> Json.fromString("created")
      ),
      instant,
      subject
    ),
    Updated(Id(projectRef, base + "created"),
            2L,
            types,
            Json.obj(
              "@type" -> Json.fromString("Updated")
            ),
            instant,
            subject),
    Deprecated(
      Id(projectRef, base + "created"),
      3L,
      types,
      instant,
      subject
    ),
    TagAdded(
      Id(projectRef, base + "created"),
      4L,
      2L,
      "v1.0.0",
      instant,
      subject
    ),
    FileCreated(
      Id(projectRef, base + "file"),
      DiskStorage.default(projectRef),
      FileAttributes(
        Paths.get("/", UUID.randomUUID().toString, UUID.randomUUID().toString).toString,
        "attachment.json",
        "application/json",
        47,
        Digest("SHA-256", "00ff4b34e3f3695c3abcdec61cba72c2238ed172ef34ae1196bfad6a4ec23dda")
      ),
      instant,
      subject
    ),
    FileUpdated(
      Id(projectRef, base + "file"),
      S3Storage(
        projectRef,
        base + "storages" + "s3",
        1L,
        deprecated = false,
        default = false,
        "MD5",
        "bucket",
        S3Settings(Some(S3Credentials("ak", "sk")), Some("endpoint"), Some("region")),
        Permission.unsafe("resources/read"),
        Permission.unsafe("files/write")
      ),
      2L,
      FileAttributes(
        Paths.get("/", UUID.randomUUID().toString, UUID.randomUUID().toString).toString,
        "attachment.json",
        "text/json",
        47,
        Digest("SHA-256", "00ff4b34e3f3695c3abcdec61cba72c2238ed172ef34ae1196bfad6a4ec23dda")
      ),
      instant,
      subject
    )
  )

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
