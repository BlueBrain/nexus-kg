package ch.epfl.bluebrain.nexus.kg.core.organizations

import java.time.Clock

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection._
import ch.epfl.bluebrain.nexus.kg.core.organizations.Organizations._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, TryValues, WordSpecLike}

import scala.util.Try

class OrganizationsSpec extends WordSpecLike with Matchers with Inspectors with TryValues with Randomness {

  private def genId(): String =
    genString(length = 4, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  private def genJson(): Json =
    Json.obj("key" -> Json.fromString(genString()))

  private implicit val caller = AnonymousCaller
  private implicit val clock  = Clock.systemUTC

  "An Organizations instance" should {
    val agg = MemoryAggregate("org")(initial, next, eval).toF[Try]
    val org = Organizations(agg)

    "create a new organization" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
    }

    "update a new organization" in {
      val id    = OrgId(genId())
      val json  = genJson()
      val json2 = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
      org.update(id, 1, json2).success.value shouldEqual OrgRef(id, 2L)
      org.fetch(id).success.value shouldEqual Some(Organization(id, 2L, json2, deprecated = false))
    }

    "prevent updates to deprecated organizations" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
      org.deprecate(id, 1L).success.value shouldEqual OrgRef(id, 2L)
      org
        .update(id, 2L, genJson())
        .failure
        .exception shouldEqual CommandRejected(OrgIsDeprecated)
    }

    "prevent duplicate deprecations" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
      org.deprecate(id, 1L).success.value shouldEqual OrgRef(id, 2L)
      org.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(OrgIsDeprecated)
    }

    "prevent duplicate creations" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
      org.create(id, json).failure.exception shouldEqual CommandRejected(OrgAlreadyExists)
    }

    "prevent update on missing org" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.update(id, 0L, json).failure.exception shouldEqual CommandRejected(OrgDoesNotExist)
    }

    "prevent creation with illegal id" in {
      forAll(List("", " ", "abv ", "123-", "ab", "abcdef")) { id =>
        val json = genJson()
        org
          .create(OrgId(id), json)
          .failure
          .exception shouldEqual CommandRejected(InvalidOrganizationId(id))
      }
    }

    "prevent update with incorrect rev" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
      org.update(id, 2L, json).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent deprecation with incorrect rev" in {
      val id   = OrgId(genId())
      val json = genJson()
      org.create(id, json).success.value shouldEqual OrgRef(id, 1L)
      org.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "return a None when fetching an org that doesn't exists" in {
      org.fetch(OrgId("missing")).success.value shouldEqual None
    }
  }
}
