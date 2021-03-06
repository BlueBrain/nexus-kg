package ch.epfl.bluebrain.nexus.kg.routes

import java.time.Instant
import java.util.UUID

import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.admin.client.types.{Organization, Project}
import ch.epfl.bluebrain.nexus.commons.test.{EitherValues, Resources}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.User
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.resources.Event._
import ch.epfl.bluebrain.nexus.kg.resources.Ref.Latest
import ch.epfl.bluebrain.nexus.kg.resources.StorageReference.S3StorageReference
import ch.epfl.bluebrain.nexus.kg.resources.file.File._
import ch.epfl.bluebrain.nexus.kg.resources.{Id, OrganizationRef}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectIdentifier.ProjectRef
import ch.epfl.bluebrain.nexus.kg.storage.Storage.DiskStorage
import ch.epfl.bluebrain.nexus.rdf.Iri.Path
import ch.epfl.bluebrain.nexus.rdf.implicits._
import com.typesafe.config.{Config, ConfigFactory}
import io.circe.Json
import org.mockito.IdiomaticMockito
import org.mockito.matchers.MacroBasedMatchers
import org.scalatest.{BeforeAndAfter, Inspectors, OptionValues}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

class EventsSpecBase
    extends AnyWordSpecLike
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

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.second, 100.milliseconds)

  override def testConfig: Config = ConfigFactory.load("test.conf")

  val base = url"http://example.com/base"

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
    )
  )
  val caller = Caller(subject, Set(subject))

  val projectUuid = UUID.fromString("7f8039a0-3141-11e9-b210-d663bd873d93")
  val orgUuid     = UUID.fromString("17a62c6a-4dc4-4eaa-b418-42d0634695a1")
  val fileUuid    = UUID.fromString("a733d99e-6075-45df-b6c3-52c8071df4fb")
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
    orgUuid,
    1L,
    deprecated = false,
    instant,
    base + "subject",
    instant,
    base + "subject"
  )

  val orgRef = OrganizationRef(project.organizationUuid)

  val organization = Organization(
    base + "org",
    "org",
    None,
    orgUuid,
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
      orgRef,
      schemaRef,
      types,
      Json.obj(
        "@type" -> Json.fromString("created")
      ),
      instant,
      subject
    ),
    Updated(
      Id(projectRef, base + "created"),
      orgRef,
      2L,
      types,
      Json.obj(
        "@type" -> Json.fromString("Updated")
      ),
      instant,
      subject
    ),
    Deprecated(
      Id(projectRef, base + "created"),
      orgRef,
      3L,
      types,
      instant,
      subject
    ),
    TagAdded(
      Id(projectRef, base + "created"),
      orgRef,
      4L,
      2L,
      "v1.0.0",
      instant,
      subject
    ),
    FileCreated(
      Id(projectRef, base + "file"),
      orgRef,
      DiskStorage.default(projectRef).reference,
      FileAttributes(
        fileUuid,
        Uri("/some/location/path"),
        Uri.Path("path"),
        "attachment.json",
        `application/json`,
        47,
        Digest("SHA-256", "00ff4b34e3f3695c3abcdec61cba72c2238ed172ef34ae1196bfad6a4ec23dda")
      ),
      instant,
      subject
    ),
    FileUpdated(
      Id(projectRef, base + "file"),
      orgRef,
      S3StorageReference(base + "storages" + "s3", 1L),
      2L,
      FileAttributes(
        fileUuid,
        Uri("/some/location/path"),
        Uri.Path("path2"),
        "attachment.json",
        `text/plain(UTF-8)`,
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
          val event = json.hcursor.get[String]("@type").rightValue
          val id    = idx
          s"""data:$data
             |event:$event
             |id:$id""".stripMargin
      }
      .mkString("", "\n\n", "\n\n")

}
