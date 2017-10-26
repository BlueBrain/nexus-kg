package ch.epfl.bluebrain.nexus.kg.core.contexts

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection.DomainIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection.OrgIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, TryValues, WordSpecLike}

import scala.util.Try

//noinspection TypeAnnotation
class ContextsSpec extends WordSpecLike with Matchers with Inspectors with TryValues with Randomness with Resources {

  private def genId(): String =
    genString(length = 4, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  private def genJson(): Json =
    Json.obj("key" -> Json.fromString(genString()))

  private def genName(): String =
    genString(length = 8, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  val contextJson         = jsonContentOf("/schema-context.json")
  val optionalContextJson = jsonContentOf("/array-context.json")
  val baseUri             = "http://localhost:8080/v0"

  trait Context {
    val orgsAgg = MemoryAggregate("org")(Organizations.initial, Organizations.next, Organizations.eval).toF[Try]
    val domAgg =
      MemoryAggregate("dom")(Domains.initial, Domains.next, Domains.eval)
        .toF[Try]
    val contextsAgg =
      MemoryAggregate("contexts")(Contexts.initial, Contexts.next, Contexts.eval)
        .toF[Try]

    val orgs     = Organizations(orgsAgg)
    val doms     = Domains(domAgg, orgs)
    val contexts = Contexts(contextsAgg, doms, baseUri)

    val orgRef = orgs.create(OrgId(genId()), genJson()).success.value
    val domRef =
      doms.create(DomainId(orgRef.id, genId()), "domain").success.value
  }

  "A Contexts instance" should {

    "create a new context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 1L, contextJson, deprecated = false, published = false))
    }

    "update a new context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts
        .update(id, 1L, optionalContextJson)
        .success
        .value shouldEqual ContextRef(id, 2L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 2L, optionalContextJson, deprecated = false, published = false))
    }

    "publish a context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 2L, contextJson, deprecated = false, published = true))
    }

    "deprecate a context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 2L, contextJson, deprecated = true, published = false))
    }

    "deprecate a published context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.deprecate(id, 2L).success.value shouldEqual ContextRef(id, 3L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 3L, contextJson, deprecated = true, published = true))
    }

    "prevent double deprecations" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(ContextIsDeprecated)
    }

    "prevent publishing when deprecated" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.publish(id, 2L).failure.exception shouldEqual CommandRejected(ContextIsDeprecated)
    }

    "prevent duplicate publish" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.publish(id, 2L).failure.exception shouldEqual CommandRejected(CannotUpdatePublished)
    }

    "prevent update when deprecated" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts
        .update(id, 2L, contextJson)
        .failure
        .exception shouldEqual CommandRejected(ContextIsDeprecated)
    }

    "prevent update when published" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts
        .update(id, 2L, contextJson)
        .failure
        .exception shouldEqual CommandRejected(CannotUpdatePublished)
    }

    "prevent update with incorrect rev" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts
        .update(id, 2L, contextJson)
        .failure
        .exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent deprecate with incorrect rev" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent publish with incorrect rev" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "return None for a context that doesn't exist" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.fetch(id).success.value shouldEqual None
    }

    "prevent creation with invalid context" in new Context {
      val id    = ContextId(domRef.id, genName(), genVersion())
      val value = genJson()
      contexts.create(id, value).failure.exception match {
        case CommandRejected(ShapeConstraintViolations(vs)) =>
          vs should not be empty
        case _ => fail("context creation with invalid context was not rejected")
      }
    }

    "prevent context update with invalid context" in new Context {
      val id    = ContextId(domRef.id, genName(), genVersion())
      val value = genJson()
      contexts.create(id, contextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.update(id, 1L, value).failure.exception match {
        case CommandRejected(ShapeConstraintViolations(vs)) =>
          vs should not be empty
        case _ => fail("context creation with invalid context was not rejected")
      }
    }
  }

  trait DomainDeprecatedContext extends Context {
    val lockedId = ContextId(domRef.id, genName(), genVersion())
    val locked   = contexts.create(lockedId, contextJson).success.value
    val _        = doms.deprecate(domRef.id, domRef.rev).success.value
  }

  trait OrgDeprecatedContext extends Context {
    val lockedId = ContextId(domRef.id, genName(), genVersion())
    val locked   = contexts.create(lockedId, contextJson).success.value
    val _        = orgs.deprecate(orgRef.id, orgRef.rev).success.value
  }

  "A Contexts" when {

    "domain is deprecated" should {

      "prevent updates" in new DomainDeprecatedContext {
        contexts
          .update(lockedId, 1L, genJson())
          .failure
          .exception shouldEqual CommandRejected(DomainIsDeprecated)
      }

      "prevent publishing" in new DomainDeprecatedContext {
        contexts
          .publish(lockedId, 1L)
          .failure
          .exception shouldEqual CommandRejected(DomainIsDeprecated)
      }

      "allow deprecation" in new DomainDeprecatedContext {
        contexts.deprecate(lockedId, 1L).success.value shouldEqual ContextRef(lockedId, 2L)
        contexts.fetch(lockedId).success.value shouldEqual Some(
          Context(lockedId, 2L, contextJson, deprecated = true, published = false))
      }

      "prevent new context creation" in new DomainDeprecatedContext {
        val id = ContextId(domRef.id, genName(), genVersion())
        contexts
          .create(id, contextJson)
          .failure
          .exception shouldEqual CommandRejected(DomainIsDeprecated)
      }
    }

    "organization is deprecated" should {

      "prevent updates" in new OrgDeprecatedContext {
        contexts
          .update(lockedId, 1L, genJson())
          .failure
          .exception shouldEqual CommandRejected(OrgIsDeprecated)
      }

      "prevent publishing" in new OrgDeprecatedContext {
        contexts
          .publish(lockedId, 1L)
          .failure
          .exception shouldEqual CommandRejected(OrgIsDeprecated)
      }

      "allow deprecation" in new OrgDeprecatedContext {
        contexts.deprecate(lockedId, 1L).success.value shouldEqual ContextRef(lockedId, 2L)
        contexts.fetch(lockedId).success.value shouldEqual Some(
          Context(lockedId, 2L, contextJson, deprecated = true, published = false))
      }

      "prevent new context creation" in new OrgDeprecatedContext {
        val id = ContextId(domRef.id, genName(), genVersion())
        contexts
          .create(id, contextJson)
          .failure
          .exception shouldEqual CommandRejected(OrgIsDeprecated)
      }
    }
  }
}
