package ch.epfl.bluebrain.nexus.kg.service

import java.util.regex.Pattern

import _root_.io.circe.Json
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport.OrderedKeys
import ch.epfl.bluebrain.nexus.commons.test.Resources.jsonContentOf
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, Contexts}
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.service.BootstrapService.kgOrderedKeys

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

package object routes {
  private val replacements = Map(Pattern.quote("{{base}}") -> "http://localhost/v0")

  implicit val orderedKeys: OrderedKeys = kgOrderedKeys

  def createNexusContexts(orgs: Organizations[Future], doms: Domains[Future], contexts: Contexts[Future])(
      implicit caller: CallerCtx): Unit = {
    val nexusOrgRef = Await.result(orgs.create(OrgId("nexus"), Json.obj())(caller), 1.second)
    val nexusDomRef = Await.result(doms.create(DomainId(nexusOrgRef.id, "core"), "Nexus core domain")(caller), 1.second)
    val standards   = jsonContentOf("/contexts/standards.json", replacements)
    val resource    = jsonContentOf("/contexts/resource.json", replacements)
    val links       = jsonContentOf("/contexts/links.json", replacements)
    val standardsId = ContextId(nexusDomRef.id, "standards", Version(0, 1, 0))
    val resourceId  = ContextId(nexusDomRef.id, "resource", Version(0, 1, 0))
    val linksId     = ContextId(nexusDomRef.id, "links", Version(0, 1, 0))
    Await.result(contexts.create(standardsId, standards)(caller), 1.second)
    Await.result(contexts.publish(standardsId, 1L)(caller), 1.second)
    Await.result(contexts.create(resourceId, resource)(caller), 1.second)
    Await.result(contexts.publish(resourceId, 1L)(caller), 1.second)
    Await.result(contexts.create(linksId, links)(caller), 1.second)
    Await.result(contexts.publish(linksId, 1L)(caller), 1.second)
    ()
  }
}
