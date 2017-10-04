package ch.epfl.bluebrain.nexus.kg.core.domains

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.common.test.Randomness
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection._
import ch.epfl.bluebrain.nexus.kg.core.domains.Domains._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection.{OrgDoesNotExist, OrgIsDeprecated}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, TryValues, WordSpecLike}

import scala.util.Try

class DomainsSpec extends WordSpecLike
  with Matchers
  with Inspectors
  with TryValues
  with Randomness {

  private def genJson(): Json =
    Json.obj("key" -> Json.fromString(genString()))

  "A Domains instance" should {
    val orgsAgg = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Try]
    val orgs = Organizations(orgsAgg)
    val agg = MemoryAggregate("dom")(initial, next, eval).toF[Try]
    val doms = Domains(agg, orgs)

    val orgId = OrgId("org")
    val orgJson = genJson()
    val _ = orgs.create(orgId, orgJson).success.value

    def genId(orgId: OrgId = orgId): DomainId =
      DomainId(orgId, genString(length = 8, Vector.range('a', 'z') ++ Vector.range('0', '9')))

    def genDesc(): String =
      genString(length = 32)

    "create a new domain" in {
      val id = genId()
      val desc = genDesc()
      doms.create(id, desc).success.value shouldEqual DomainRef(id, 1L)
      doms.fetch(id).success.value shouldEqual Some(Domain(id, 1L, deprecated = false, desc))
    }

    "prevent duplicate deprecations" in {
      val id = genId()
      val desc = genDesc()
      doms.create(id, desc).success.value shouldEqual DomainRef(id, 1L)
      doms.deprecate(id, 1L).success.value shouldEqual DomainRef(id, 2L)
      doms.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(DomainAlreadyDeprecated)
    }

    "prevent duplicate creations" in {
      val id = genId()
      val desc = genDesc()
      doms.create(id, desc).success.value shouldEqual DomainRef(id, 1L)
      doms.create(id, desc).failure.exception shouldEqual CommandRejected(DomainAlreadyExists)
    }

    "prevent creation with illegal id" in {
      val desc = genDesc()
      forAll(List("", " ", "abv ", "123-", "ab", "abcdefakdlsjfalksjf93r83r8993798375")) { string =>
        val id = DomainId(orgId, string)
        doms.create(id, desc).failure.exception shouldEqual CommandRejected(InvalidDomainId(id.id))
      }
    }

    "prevent deprecation with incorrect rev" in {
      val id = genId()
      val desc = genDesc()
      doms.create(id, desc).success.value shouldEqual DomainRef(id, 1L)
      doms.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "return a None when fetching a domain that doesn't exists" in {
      val id = DomainId(orgId, "missing")
      doms.fetch(id).success.value shouldEqual None
    }

    "prevent creating a domain within an org that doesn't exist" in {
      val id = genId(orgId = OrgId("missing"))
      val desc = genDesc()
      doms.create(id, desc).failure.exception shouldEqual CommandRejected(OrgDoesNotExist)
    }

    "prevent creating a domain within an org that is deprecated" in {
      val depr = OrgId("depr")
      orgs.create(depr, genJson()).success
      orgs.deprecate(depr, 1L)
      val id = genId(orgId = depr)
      val desc = genDesc()
      doms.create(id, desc).failure.exception shouldEqual CommandRejected(OrgIsDeprecated)
    }
  }
}
