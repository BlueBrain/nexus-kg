package ch.epfl.bluebrain.nexus.kg.tests.integration

import java.util.regex.Pattern
import java.util.{Properties, UUID}

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.testkit.ScalatestRouteTest
import cats.instances.string._
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.contexts.{Context, ContextId}
import ch.epfl.bluebrain.nexus.kg.core.domains.{Domain, DomainId}
import ch.epfl.bluebrain.nexus.kg.core.instances.{Instance, InstanceId}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organization}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{Schema, SchemaId}
import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.service.config.Settings.PrefixUris
import ch.epfl.bluebrain.nexus.kg.service.io.BaseEncoder
import ch.epfl.bluebrain.nexus.kg.service.routes.SchemaRoutes.SchemaConfig
import ch.epfl.bluebrain.nexus.kg.service.routes._
import io.circe._
import io.circe.generic.semiauto._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{Resources => _, _}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

/**
  * Initialize common implicits, encoders, methods and values used on the tests.
  *
  * @param apiUri      the service public uri + prefix
  * @param prefixes    the nexus context and vocabularies URIs
  * @param as          the implicitly available Actor System
  */
abstract class BootstrapIntegrationSpec(apiUri: Uri, prefixes: PrefixUris)(implicit as: ActorSystem)
    extends Suite
    with WordSpecLike
    with Eventually
    with ScalatestRouteTest
    with Matchers
    with ScalaFutures
    with Inspectors
    with MockedIAMClient
    with CancelAfterFailure
    with Assertions {

  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(5 seconds, 300 milliseconds)

  override protected def createActorSystem(): ActorSystem = as

  override protected def beforeAll(): Unit = ()

  override protected def afterAll(): Unit = ()

  implicit val schemaConfig: Encoder[SchemaConfig]    = deriveEncoder[SchemaConfig]
  implicit val qualifier: ConfiguredQualifier[String] = Qualifier.configured[String](prefixes.CoreVocabulary)
  private implicit val domainIdExtractor              = (entity: Domain) => entity.id
  private implicit val orgIdExtractor                 = (entity: Organization) => entity.id
  private implicit val schemaIdExtractor              = (entity: Schema) => entity.id
  private implicit val contextIdExtractor             = (entity: Context) => entity.id
  private implicit val instanceIdExtractor            = (entity: Instance) => entity.id

  val orgsEncoders                      = new OrgCustomEncoders(apiUri, prefixes)
  val domsEncoders                      = new DomainCustomEncoders(apiUri, prefixes)
  val contextEncoders                   = new ContextCustomEncoders(apiUri, prefixes)
  val schemaEncoders                    = new SchemaCustomEncoders(apiUri, prefixes)
  val instanceEncoders                  = new InstanceCustomEncoders(apiUri, prefixes)
  implicit val baseEncoder: BaseEncoder = instanceEncoders
}

object BootstrapIntegrationSpec extends Randomness with Resources {
  val indexTimeout = 35L

  val orgs: List[OrgId] = List(OrgId("nexus"), OrgId("other"), OrgId("rand"))
    .sortWith(_.show < _.show)

  //37 domains. 5 domains from nexus org, 2 from other org and 30 from rand org
  val domains: List[DomainId] = (List(
    DomainId(orgs.head, "core"),
    DomainId(orgs.head, "admin"),
    DomainId(orgs.head, "development"),
    DomainId(orgs.head, "sales"),
    DomainId(orgs.head, "marketing"),
    DomainId(orgs(1), "central"),
    DomainId(orgs(1), "experiment")
  )
    ++ List.fill(15)(DomainId(orgs(2), genString(length = 4))))
    .sortWith(_.show < _.show)
  val randDomains: List[DomainId] = domains.filter(_.orgId.id == "rand")

  val instanceContext: (ContextId, Json) = {
    val contextName = genString(length = 5)
    ContextId(domains(2), contextName, Version(1, 0, 0)) -> jsonContentOf("/contexts/instance-context.json")
  }

  val schemaContext: (ContextId, Json) = {
    val contextName = genString(length = 5)
    ContextId(domains(2), contextName, Version(1, 0, 0)) -> jsonContentOf("/contexts/schema-context.json")
  }

  val contexts: List[(ContextId, Json)] = domains
    .drop(2)
    .foldLeft(Vector.empty[(ContextId, Json)])((acc, c) => {
      val contextName = genString(length = 5)
      acc ++ (0 until 3).map(v => {
        ContextId(c, contextName, Version(1, 0, v)) -> jsonContentOf("/contexts/schema-context.json")
      })
    })
    .toList
    .sortWith(_._1.show < _._1.show) ::: List(instanceContext, schemaContext)

  //3 schemas with different version patch value for each domain except for 'nexus/core' and 'nexus/other' which are deprecated
  val schemas: List[(SchemaId, Json)] = domains
    .drop(2)
    .foldLeft(Vector.empty[(SchemaId, Json)])((acc, c) => {
      val schemaName = genString(length = 5)
      acc ++ (0 until 3).map(v => {
        val replacements = Map(
          Pattern.quote("{{max_count}}") -> (genInt() + 1).toString,
          Pattern.quote("{{number}}")    -> v.toString,
          Pattern.quote("{{context}}")   -> schemaContext._1.show
        )
        SchemaId(c, schemaName, Version(1, 0, v)) -> jsonContentOf("/schemas/int-value-schema-variable-max.json",
                                                                   replacements)
      })
    })
    .toList
    .sortWith(_._1.show < _._1.show)

  //5 instances for each schema except for first schema, which are deprecated
  val pendingInstances: List[(InstanceId, Json)] = schemas
    .drop(1)
    .foldLeft(Vector.empty[(InstanceId, Json)]) {
      case (acc, (schemaId, _)) =>
        acc ++ (0 until 5).map(_ => InstanceId(schemaId, genUUID()) -> genJson())
    }
    .toList
    .sortWith(_._1.show < _._1.show)

  lazy val blazegraphProps: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  private def genUUID(): String = UUID.randomUUID().toString.toLowerCase

  private def genJson(): Json =
    jsonContentOf("/data/int-value-has-part.json",
                  Map("random" -> genString(length = 4), Pattern.quote("{{context}}") -> instanceContext._1.show))
      .deepMerge(Json.obj("value" -> Json.fromInt(genInt(Int.MaxValue))))
}
