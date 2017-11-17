package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.Clock
import java.util.Properties
import java.util.regex.Pattern

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.concurrent.ScalaFutures

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}
import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._

trait IndexerFixture extends Randomness with ScalaFutures with Resources {

  lazy val localhost = "127.0.0.1"

  lazy val properties: Map[String, String] = {
    val props = new Properties()
    props.load(getClass.getResourceAsStream("/index.properties"))
    props.asScala.toMap
  }

  private implicit val clock  = Clock.systemUTC
  private implicit val caller = AnonymousCaller(Anonymous())

  protected def genId(): String =
    genString(length = 4, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  protected def genJson(): Json =
    Json.obj("key" -> Json.fromString(genString()))

  protected def genName(): String =
    genString(length = 8, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  protected def createContext(base: Uri)(implicit ec: ExecutionContext): (Contexts[Future], Map[String, String]) = {
    val orgsAgg = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Future]

    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Future]
    val ctxsAgg =
      MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval)
        .toF[Future]
    val orgs = Organizations(orgsAgg)

    val doms = Domains(domAgg, orgs)
    val ctxs = Contexts(ctxsAgg, doms, base.toString)

    val orgRef = orgs.create(OrgId(genId()), genJson()).futureValue

    val domRef =
      doms.create(DomainId(orgRef.id, genId()), "domain").futureValue
    val contextId = ContextId(domRef.id, genName(), genVersion())

    val replacements = Map(Pattern.quote("{{base}}") -> base.toString, Pattern.quote("{{context}}") -> contextId.show)

    val contextJson = jsonContentOf("/contexts/minimal.json", replacements)

    ctxs.create(contextId, contextJson).futureValue
    ctxs.publish(contextId, 1L).futureValue

    (ctxs, replacements)

  }
}
