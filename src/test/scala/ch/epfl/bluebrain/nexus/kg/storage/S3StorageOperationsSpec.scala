package ch.epfl.bluebrain.nexus.kg.storage

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.util.UUID

import akka.http.scaladsl.model.Uri
import akka.stream.alpakka.s3
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{FileIO, Sink}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.commons.test.io.IOValues
import ch.epfl.bluebrain.nexus.commons.test.{ActorSystemFixture, Randomness, Resources}
import ch.epfl.bluebrain.nexus.iam.client.types.Permission
import ch.epfl.bluebrain.nexus.kg.KgError
import ch.epfl.bluebrain.nexus.kg.KgError.DownstreamServiceError
import ch.epfl.bluebrain.nexus.kg.resources.file.File.{Digest, FileAttributes, FileDescription}
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.storage.Storage.{S3Settings, S3Storage}
import ch.epfl.bluebrain.nexus.rdf.syntax._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, AnonymousAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.findify.s3mock.S3Mock
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.mutable

class S3StorageOperationsSpec
    extends ActorSystemFixture("S3StorageOperationsSpec")
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll
    with IOValues
    with Randomness
    with Resources {

  private implicit val mt: Materializer = ActorMaterializer()

  private val port    = freePort
  private val address = s"http://localhost:$port"
  private val region  = "fake-region"
  private val bucket  = "bucket"
  private val s3mock  = S3Mock(port)
  private val readS3  = Permission.unsafe("s3-read")
  private val writeS3 = Permission.unsafe("s3-write")

  private val keys  = Set("http.proxyHost", "http.proxyPort", "https.proxyHost", "https.proxyPort", "http.nonProxyHosts")
  private val props = mutable.Map[String, String]()

  protected override def beforeAll(): Unit = {
    // saving the current system properties so that we can restore them later
    props ++= System.getProperties.asScala.toMap.filterKeys(keys.contains)
    keys.foreach(System.clearProperty)

    s3mock.start
    val endpoint = new EndpointConfiguration(address, region)
    val client = AmazonS3ClientBuilder.standard
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials))
      .build
    client.createBucket(bucket)
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    s3mock.stop
    keys.foreach { k =>
      props.get(k) match {
        case Some(v) => System.setProperty(k, v)
        case None    => System.clearProperty(k)
      }
    }
    super.afterAll()
  }

  "S3StorageOperations" should {

    "save and fetch files" in {
      keys.foreach(System.clearProperty)

      val base       = url"https://nexus.example.com/".value
      val projectId  = base + "org" + "proj"
      val projectRef = ProjectRef(UUID.randomUUID)
      val storage =
        S3Storage(projectRef,
                  projectId,
                  1L,
                  deprecated = false,
                  default = true,
                  "MD5",
                  bucket,
                  S3Settings(None, Some(address), Some(region)),
                  readS3,
                  writeS3)

      val verify = new S3StorageOperations.Verify[IO](storage)
      val save   = new S3StorageOperations.Save[IO](storage)
      val fetch  = new S3StorageOperations.Fetch[IO](storage)

      // bucket is empty
      verify.apply.ioValue shouldEqual Right(())

      val resid    = Id(projectRef, base + "files" + "id")
      val fileUuid = UUID.randomUUID
      val desc     = FileDescription(fileUuid, "s3.json", "text/plain")
      val filePath = "/storage/s3.json"
      val path     = Paths.get(getClass.getResource(filePath).toURI)
      val attr     = save(resid, desc, FileIO.fromPath(path)).ioValue
      // http://s3.amazonaws.com is hardcoded in S3Mock
      attr.location shouldEqual Uri(s"http://s3.amazonaws.com/$bucket/${mangle(projectRef, fileUuid)}")
      attr.mediaType shouldEqual "text/plain"
      attr.bytes shouldEqual 323L
      attr.filename shouldEqual "s3.json"
      attr.digest shouldEqual Digest("MD5", "c322c0eaa0031ed8b22cd5bc21a8e79f")

      val download =
        fetch(attr).ioValue.runWith(Sink.head).futureValue.decodeString(StandardCharsets.UTF_8)
      download shouldEqual contentOf(filePath)

      // bucket has one object
      verify.apply.ioValue shouldEqual Right(())

      val randomUuid = UUID.randomUUID
      val inexistent = fetch(
        attr.copy(uuid = randomUuid,
                  location = Uri(s"http://s3.amazonaws.com/$bucket/${mangle(projectRef, randomUuid)}")))
        .failed[KgError.InternalError]
      inexistent.msg shouldEqual s"Empty content fetching S3 object with key '${mangle(projectRef, randomUuid)}' in bucket 'bucket'"
    }

    "fail if the bucket doesn't exist" in {
      keys.foreach(System.clearProperty)

      val base       = url"https://nexus.example.com/".value
      val projectId  = base + "org" + "proj"
      val projectRef = ProjectRef(UUID.randomUUID)
      val storage =
        S3Storage(projectRef,
                  projectId,
                  1L,
                  deprecated = false,
                  default = true,
                  "MD5",
                  "foobar",
                  S3Settings(None, Some(address), Some(region)),
                  readS3,
                  writeS3)

      val verify = new S3StorageOperations.Verify[IO](storage)
      val save   = new S3StorageOperations.Save[IO](storage)
      val fetch  = new S3StorageOperations.Fetch[IO](storage)

      verify.apply.ioValue shouldEqual Left("Error accessing S3 bucket 'foobar': The specified bucket does not exist")

      val resid    = Id(projectRef, base + "files" + "id")
      val fileUuid = UUID.randomUUID
      val desc     = FileDescription(fileUuid, "s3.json", "text/plain")
      val filePath = "/storage/s3.json"
      val path     = Paths.get(getClass.getResource(filePath).toURI)
      val upload   = save(resid, desc, FileIO.fromPath(path)).failed[DownstreamServiceError]
      upload.msg shouldEqual "Error uploading S3 object with filename 's3.json' in bucket 'foobar': The specified bucket does not exist"
      val attr = new FileAttributes(
        fileUuid,
        Uri(s"http://s3.amazonaws.com/foobar/${mangle(projectRef, fileUuid)}"),
        desc.filename,
        desc.mediaType,
        323L,
        Digest("MD5", "c322c0eaa0031ed8b22cd5bc21a8e79f")
      )
      val download = fetch(attr).failed[DownstreamServiceError]
      download.msg shouldEqual s"Error fetching S3 object with key '${mangle(projectRef, fileUuid)}' in bucket 'foobar': The specified bucket does not exist"
    }

    "verify storage with no region" in {
      keys.foreach(System.clearProperty)

      val base       = url"https://nexus.example.com/".value
      val projectId  = base + "org" + "proj"
      val projectRef = ProjectRef(UUID.randomUUID)
      val storage =
        S3Storage(projectRef,
                  projectId,
                  1L,
                  deprecated = false,
                  default = true,
                  "MD5",
                  bucket,
                  S3Settings(None, Some(address), None),
                  readS3,
                  writeS3)

      val verify = new S3StorageOperations.Verify[IO](storage)
      verify.apply.ioValue shouldEqual Right(())
    }
  }

  "S3Settings" should {
    "select the system proxy" in {
      System.setProperty("http.proxyHost", "example.com")
      System.setProperty("http.proxyPort", "8080")
      System.setProperty("https.proxyHost", "secure.example.com")
      System.setProperty("https.proxyPort", "8080")
      System.setProperty("http.nonProxyHosts", "*.epfl.ch|*.cluster.local")

      S3Settings.getSystemProxy("http://s3.amazonaws.com") shouldEqual Some(s3.Proxy("example.com", 8080, "http"))
      S3Settings.getSystemProxy("https://s3.amazonaws.com") shouldEqual Some(
        s3.Proxy("secure.example.com", 8080, "http"))
      S3Settings.getSystemProxy("https://www.epfl.ch") shouldEqual None
      S3Settings.getSystemProxy("http://foo.bar.cluster.local") shouldEqual None
    }
  }

}
