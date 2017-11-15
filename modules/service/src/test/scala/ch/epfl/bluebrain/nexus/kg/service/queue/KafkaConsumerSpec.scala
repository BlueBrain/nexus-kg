package ch.epfl.bluebrain.nexus.kg.service.queue

import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{ActorRef, ActorSystem}
import akka.kafka.ConsumerSettings
import akka.testkit.TestKit
import ch.epfl.bluebrain.nexus.commons.iam.acls.Event._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path._
import ch.epfl.bluebrain.nexus.commons.iam.acls._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity._
import ch.epfl.bluebrain.nexus.commons.iam.identity.IdentityId
import ch.epfl.bluebrain.nexus.commons.iam.io.serialization.JsonLdSerialization
import io.circe.Decoder
import journal.Logger
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Future
import scala.concurrent.duration._

class KafkaConsumerSpec
    extends TestKit(ActorSystem("embedded-kafka"))
    with WordSpecLike
    with Eventually
    with Matchers
    with BeforeAndAfterAll
    with EmbeddedKafka {

  override implicit val patienceConfig: PatienceConfig  = PatienceConfig(30.seconds, 3.seconds)
  private implicit val kafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)

  private val log          = Logger[this.type]
  private val path         = "a" / "b" / "c"
  private val instant      = Instant.ofEpochMilli(1)
  private val anonymous    = Anonymous(IdentityId("http://localhost/prefix/anonymous"))
  private val alice        = UserRef(IdentityId("http://localhost/prefix/realms/realm/users/alice"))
  private val meta         = Meta(alice, instant)
  private val ownReadWrite = Permissions(Permission.Own, Permission.Read, Permission.Write)
  private val acl          = AccessControlList(anonymous -> ownReadWrite)
  private val messages = List(
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","identity":{"@id":"http://localhost/prefix/realms/realm/groups/some-group","realm":"realm","group":"some-group","@type":"GroupRef"},"permissions":["own","read","write"],"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsAdded"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsCleared"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","acl":[{"identity":{"@id":"http://localhost/prefix/anonymous","@type":"Anonymous"},"permissions":["own","read","write"]}],"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsCreated"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","identity":{"@id":"http://localhost/prefix/realms/realm/authenticated","realm":"realm","@type":"AuthenticatedRef"},"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsRemoved"}""",
    """{"@context":"http://localhost/prefix/context","path":"a/b/c","identity":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"permissions":["own","read","write"],"meta":{"author":{"@id":"http://localhost/prefix/realms/realm/users/alice","realm":"realm","sub":"alice","@type":"UserRef"},"instant":"1970-01-01T00:00:00.001Z"},"@type":"PermissionsSubtracted"}"""
  )

  case class Message(msg: String)
  private val msgDecoder: Decoder[Message] = Decoder.instance(_.downField("msg").as[String].map(Message.apply))

  "KafkaConsumer" should {
    "decode and index received messages" in {
      val counter = new AtomicInteger

      def index(e: Event): Future[Unit] = {
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

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-1")
      withRunningKafka {
        for (i <- 1 to 100) {
          publishStringMessageToKafka("test-topic-1", messages(i % 4))
        }
        val supervisor =
          KafkaConsumer.start[Event](consumerSettings, index, "test-topic-1", JsonLdSerialization.eventDecoder)
        eventually {
          counter.get shouldEqual 100
        }
        blockingStop(supervisor)
      }
    }

    "restart from last committed offset" in {
      val counter = new AtomicInteger

      def index(msg: Message): Future[Unit] = {
        val expected =
          if (counter.get < 100) Message("foo")
          else Message("bar")

        if (msg == expected) {
          counter.incrementAndGet()
          Future.successful(())
        } else {
          Future.failed(new Exception)
        }
      }

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-2")
      withRunningKafka {
        for (_ <- 1 to 100) {
          publishStringMessageToKafka("test-topic-2", """{"msg":"foo"}""")
        }
        val supervisor = KafkaConsumer.start[Message](consumerSettings, index, "test-topic-2", msgDecoder)
        eventually {
          counter.get shouldEqual 100
        }
        KafkaConsumer.stop(supervisor)

        for (_ <- 1 to 100) {
          publishStringMessageToKafka("test-topic-2", """{"msg":"bar"}""")
        }
        eventually {
          counter.get shouldEqual 200
        }
        blockingStop(supervisor)
      }
    }

    "run in parallel" in {
      val counter1 = new AtomicInteger
      val counter2 = new AtomicInteger

      def index(msg: Message): Future[Unit] = msg match {
        case Message("foo") =>
          counter1.incrementAndGet()
          Future.successful(())
        case Message("bar") =>
          counter2.incrementAndGet()
          Future.successful(())
        case _ =>
          Future.failed(new Exception)
      }

      val as1 = ActorSystem("embedded-kafka-1")
      val as2 = ActorSystem("embedded-kafka-2")
      val topic                                 = "test-topic-3"

      val consumerSettings =
        ConsumerSettings(system, new StringDeserializer, new StringDeserializer).withGroupId("group-id-3")
      implicit val serializer: StringSerializer = new StringSerializer
      withRunningKafka {
        for (_ <- 1 to 100) {
          publishToKafka(topic, "partition-1", """{"msg":"foo"}""")
          publishToKafka(topic, "partition-2", """{"msg":"bar"}""")
        }

        val supervisor1 = KafkaConsumer.start[Message](consumerSettings, index, topic, msgDecoder)(as1)
        val supervisor2 = KafkaConsumer.start[Message](consumerSettings, index, topic, msgDecoder)(as2)

        eventually {
          counter1.get shouldEqual 100
          counter2.get shouldEqual 100
        }
        blockingStop(supervisor1)
        blockingStop(supervisor2)
      }
      TestKit.shutdownActorSystem(as1)
      TestKit.shutdownActorSystem(as2)
    }
  }

  private def blockingStop(actor: ActorRef): Unit = {
    watch(actor)
    system.stop(actor)
    expectTerminated(actor, 30.seconds)
    log.warn("Actor stopped")
    val _ = unwatch(actor)
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
  }
}
