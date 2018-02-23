package ch.epfl.bluebrain.nexus.kg.core.contexts

import java.time.Clock
import java.util.regex.Pattern

import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri
import akka.testkit.TestKit
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.contexts.ExpanderSpec._
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations.{eval, initial, next}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

class ExpanderSpec
    extends TestKit(ActorSystem("ExpanderSpec"))
    with WordSpecLike
    with Matchers
    with Resources
    with ScalaFutures {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 300 milliseconds)

  "An Expander" should {
    import system.dispatcher
    val replacements = Map(Pattern.quote("{{base}}") -> baseUri.toString)

    val context   = jsonContentOf("/contexts/simple_context.json", replacements)
    val json      = jsonContentOf("/int-value.json")
    val expanded  = jsonContentOf("/int-value-expanded.json")
    val standards = jsonContentOf("/contexts/standards.json", replacements)
    val links     = jsonContentOf("/contexts/links.json", replacements)

    implicit val caller: CallerCtx = CallerCtx(Clock.systemUTC, AnonymousCaller(Anonymous()))
    val (orgs, doms, contexts)     = operations
    val expander                   = new JenaExpander[Future](contexts)

    Await.result(orgs.create(OrgId("nexus"), Json.obj()), 1 second)
    Await.result(doms.create(DomainId(OrgId("nexus"), "core"), "something"), 1 second)
    Await.result(contexts.create(ContextId(DomainId(OrgId("nexus"), "core"), "standards", Version(0, 1, 0)), standards),
                 3 second)
    Await.result(contexts.publish(ContextId(DomainId(OrgId("nexus"), "core"), "standards", Version(0, 1, 0)), 1L),
                 3 second)
    Await.result(contexts.create(ContextId(DomainId(OrgId("nexus"), "core"), "links", Version(0, 1, 0)), links),
                 3 second)
    Await.result(contexts.publish(ContextId(DomainId(OrgId("nexus"), "core"), "links", Version(0, 1, 0)), 1L), 3 second)

    "expand the string context" in {
      val instanceExpander = expander("nxv:Instance", context).futureValue
      instanceExpander shouldEqual "https://bbp-nexus.epfl.ch/vocabs/nexus/core/terms/v0.1.0/Instance"
      val nxvContext = Json.obj(
        "@context" -> Json.obj("nxv" -> Json.fromString("https://bbp-nexus.epfl.ch/vocabs/nexus/core/terms/v0.1.0/")))
      JenaExpander.expand("nxv:Instance", nxvContext) shouldEqual instanceExpander

      expander("rdf:type", context).futureValue shouldEqual "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      expander("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", context).futureValue shouldEqual "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      expander("http://www.w3.org/1999/02/22-rdf-syntax-ns#type", Json.obj()).futureValue shouldEqual "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    }

    "expand the json with context" in {
      expander(json).futureValue shouldEqual expanded
    }
  }

}

object ExpanderSpec {
  private val baseUri: Uri = "http://localhost/v0"

  private def operations(implicit ec: ExecutionContext) = {
    val orgs     = Organizations(MemoryAggregate("orgs")(initial, next, eval).toF[Future])
    val domAgg   = MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval).toF[Future]
    val doms     = Domains(domAgg, orgs)
    val ctxAgg   = MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval).toF[Future]
    val contexts = Contexts(ctxAgg, doms, baseUri.toString)
    (orgs, doms, contexts)
  }
}
