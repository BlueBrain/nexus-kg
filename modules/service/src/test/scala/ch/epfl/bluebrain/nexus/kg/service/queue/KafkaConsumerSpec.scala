package ch.epfl.bluebrain.nexus.kg.service.queue

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorSystem
import akka.kafka.ConsumerSettings
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event._
import ch.epfl.bluebrain.nexus.commons.iam.acls._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.identity.IdentityId
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity._
import ch.epfl.bluebrain.nexus.commons.iam.io.serialization.EventJsonLdSerialization
import io.circe.Decoder
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.{ExecutionContextExecutor, Future}

class KafkaConsumerSpec extends WordSpecLike with Eventually with Matchers with EmbeddedKafka {

  private implicit val as: ActorSystem              = ActorSystem("embedded-kafka")
  private implicit val ec: ExecutionContextExecutor = as.dispatcher
  private implicit val eventDecoder: Decoder[Event] = EventJsonLdSerialization.eventDecoder

  private val counter          = new AtomicInteger
  private val consumerSettings = ConsumerSettings(as, new StringDeserializer, new StringDeserializer)
  private val path             = "a" / "b" / "c"
  private val instant          = Instant.ofEpochMilli(1)
  private val anonymous        = Anonymous(IdentityId("http://localhost/prefix/anonymous"))
  private val alice            = UserRef(IdentityId("http://localhost/prefix/realms/realm/users/alice"))
  private val meta             = Meta(alice, instant)
  private val ownReadWrite     = Permissions(Permission.Own, Permission.Read, Permission.Write)
  private val acl              = AccessControlList(anonymous -> ownReadWrite)
  private val messages = List(
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","identity":{"@id":"http://localhost/prefix/realms/realm/groups/some-group","realm":"realm","group":"some-group","@type":"GroupRef"},"permissions":["own","read","write"],"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsAdded"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsCleared"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","acl":[{"identity":{"@id":"http://localhost/prefix/anonymous","@type":"Anonymous"},"permissions":["own","read","write"]}],"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsCreated"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","identity":{"@id":"http://localhost/prefix/realms/realm/authenticated","realm":"realm","@type":"AuthenticatedRef"},"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsRemoved"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","identity":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"permissions":["own","read","write"],"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsSubtracted"}"""
  )

  "KafkaConsumer" should {
    "decode and index received messages" in {
      implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
      withRunningKafka {
        for (i <- 1 to 100) {
          publishStringMessageToKafka("test-topic", messages(i % 4))
        }
        KafkaConsumer.start[Event](consumerSettings, index, "test-topic")
        eventually(timeout(Span(30, Seconds)), interval(Span(5, Seconds))) {
          counter.get shouldEqual 100
        }
      }
    }
  }

  private def index(e: Event): Future[Unit] = {
    e.path shouldEqual path
    e.meta shouldEqual meta
    e match {
      case PermissionsCreated(_, accessControlList, _) => accessControlList shouldEqual acl
      case PermissionsAdded(_, _, permissions, _)      => permissions shouldEqual ownReadWrite
      case PermissionsSubtracted(_, _, permissions, _) => permissions shouldEqual ownReadWrite
      case _                                           => ()
    }
    counter.incrementAndGet()
    Future.successful(())
  }

}
