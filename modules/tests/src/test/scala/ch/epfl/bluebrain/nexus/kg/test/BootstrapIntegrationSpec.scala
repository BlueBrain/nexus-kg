package ch.epfl.bluebrain.nexus.kg.test

import java.util.{Properties, UUID}
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.testkit.ScalatestRouteTest
import ch.epfl.bluebrain.nexus.kg.indexing.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.service.hateoas.Link
import ch.epfl.bluebrain.nexus.kg.service.routes.{DomainCustomEncoders, InstanceCustomEncoders, OrgCustomEncoders, SchemaCustomEncoders}
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import cats.instances.string._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import io.circe.generic.semiauto._
import io.circe._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.{Randomness, Resources}
import org.scalatest.{Inspectors, Matchers, WordSpecLike}
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Initialize common implicits, encoders, methods and values used on the tests.
  *
  * @param apiUri the service public uri + prefix
  * @param vocab  the nexus core vocabulary base
  * @param as     the implicitly available Actor System
  */
abstract class BootstrapIntegrationSpec(apiUri: Uri, vocab: Uri)(implicit as: ActorSystem) extends WordSpecLike
  with Eventually
  with ScalatestRouteTest
  with Matchers
  with ScalaFutures
  with Inspectors {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(5 seconds, 300 milliseconds)

  override protected def createActorSystem(): ActorSystem = as

  override protected def beforeAll(): Unit = ()

  override protected def afterAll(): Unit = ()

  implicit val linkEncoder: Encoder[Link] = deriveEncoder[Link]
  implicit val schemaConfig: Encoder[SchemaConfig] = deriveEncoder[SchemaConfig]
  implicit val qualifier: ConfiguredQualifier[String] = Qualifier.configured[String](vocab)

  val orgsEncoder = new OrgCustomEncoders(apiUri)
  val domsEncoder = new DomainCustomEncoders(apiUri)
  val schemaEncoder = new SchemaCustomEncoders(apiUri)
  val instanceEncoder = new InstanceCustomEncoders(apiUri)
}

object BootstrapIntegrationSpec extends Randomness with Resources {
  val indexTimeout = 13L

  val orgs: List[OrgId] = List(OrgId("nexus"), OrgId("other"), OrgId("rand")).sortWith(_.show < _.show)

  //37 domains. 5 domains from nexus org, 2 from other org and 30 from rand org
  val domains: List[DomainId] = (List(DomainId(orgs.head, "core"), DomainId(orgs.head, "admin"), DomainId(orgs.head, "development"), DomainId(orgs.head, "sales"), DomainId(orgs.head, "marketing"), DomainId(orgs(1), "central"), DomainId(orgs(1), "experiment"))
    ++ List.fill(15)(DomainId(orgs(2), genString(length = 4))))
    .sortWith(_.show < _.show)
  val randDomains: List[DomainId] = domains.filter(_.orgId.id == "rand")

  //3 schemas with different version patch value for each domain except for 'nexus/core' and 'nexus/other' which are deprecated
  val schemas: List[(SchemaId, Json)] = domains.drop(2).foldLeft(Vector.empty[(SchemaId, Json)])((acc, c) => {
    val schemaName = genString(length = 5)
    acc ++ (0 until 3).map(v => {
      val replacements = Map(Pattern.quote("{{max_count}}") -> (genInt() + 1).toString, Pattern.quote("{{number}}") -> v.toString)
      SchemaId(c, schemaName, Version(1, 0, v)) -> jsonContentOf("/schemas/int-value-schema-variable-max.json", replacements)
    })
  }).toList.sortWith(_._1.show < _._1.show)

  //5 instances for each schema except for first schema, which are deprecated
  val pendingInstances: List[(InstanceId, Json)] = schemas.drop(1).foldLeft(Vector.empty[(InstanceId, Json)]) { case (acc, (schemaId, _)) =>
    acc ++ (0 until 5).map(_ => InstanceId(schemaId, genUUID()) -> genJson())
  }.toList.sortWith(_._1.show < _._1.show)

  lazy val blazegraphProps: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  private def genUUID(): String = UUID.randomUUID().toString.toLowerCase

  private def genJson(): Json =
    jsonContentOf("/data/int-value-has-part.json", Map("random" -> genString(length = 4))).deepMerge(Json.obj("value" -> Json.fromInt(genInt(Int.MaxValue))))
}
