package ch.epfl.bluebrain.nexus.kg.core.contexts

import java.util.regex.Pattern.quote

import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextRejection._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainRejection.DomainIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, Domains}
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgRejection.OrgIsDeprecated
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, Organizations}
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

  val objectContextJson = jsonContentOf("/contexts/object-context.json")
  def arrayContextJson(id: ContextId) = {
    val ContextId(DomainId(OrgId(org), dom), name, ver) = id
    val replacements = Map(
      quote("{{org}}")  -> org,
      quote("{{dom}}")  -> dom,
      quote("{{name}}") -> name,
      quote("{{ver}}")  -> ver.show
    )
    jsonContentOf("/contexts/array-context.json", replacements)
  }
  val baseUri = "http://localhost/v0"

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
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 1L, objectContextJson, deprecated = false, published = false))
    }

    "update a new context" in new Context {
      val existing = ContextId(domRef.id, genName(), genVersion())
      contexts.create(existing, objectContextJson).success
      contexts.publish(existing, 1L).success

      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts
        .update(id, 1L, arrayContextJson(existing))
        .success
        .value shouldEqual ContextRef(id, 2L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 2L, arrayContextJson(existing), deprecated = false, published = false))
    }

    "publish a context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 2L, objectContextJson, deprecated = false, published = true))
    }

    "deprecate a context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 2L, objectContextJson, deprecated = true, published = false))
    }

    "deprecate a published context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.deprecate(id, 2L).success.value shouldEqual ContextRef(id, 3L)
      contexts.fetch(id).success.value shouldEqual Some(
        Context(id, 3L, objectContextJson, deprecated = true, published = true))
    }

    "expand nested contexts" in new Context {
      val ver = genVersion()
      val defaultReplacements = Map(
        quote("{{org}}") -> orgRef.id.id,
        quote("{{dom}}") -> domRef.id.id,
        quote("{{ver}}") -> ver.show
      )

      val objId    = ContextId(domRef.id, genName(), ver)
      val objValue = jsonContentOf("/contexts/object-context.json")
      contexts.create(objId, objValue).success
      contexts.publish(objId, 1L).success

      val secondObjId    = ContextId(domRef.id, genName(), ver)
      val secondObjValue = jsonContentOf("/contexts/second-object-context.json")
      contexts.create(secondObjId, secondObjValue).success
      contexts.publish(secondObjId, 1L).success

      val mixedId = ContextId(domRef.id, genName(), ver)
      val mixedValue = jsonContentOf(
        "/contexts/mixed-array-context.json",
        defaultReplacements ++ Map(
          quote("{{object}}")        -> objId.name,
          quote("{{second-object}}") -> secondObjId.name,
        )
      )
      contexts.create(mixedId, mixedValue).success
      contexts.publish(mixedId, 1L).success

      val importingId = ContextId(domRef.id, genName(), ver)
      val importingValue = jsonContentOf(
        "/contexts/importing-context.json",
        defaultReplacements ++ Map(
          quote("{{mixed}}") -> mixedId.name
        )
      )
      contexts.create(importingId, importingValue).success
      contexts.publish(importingId, 1L).success

      val input = Json.obj(
        "existing" -> Json.fromInt(2),
        "@context" -> Json.fromString(s"$baseUri/contexts/${importingId.show}")
      )

      val expected =
        s"""|{
            |  "@context" : {
            |    "owl" : "http://www.w3.org/2002/07/owl#",
            |    "xsd" : "http://www.w3.org/2001/XMLSchema#",
            |    "schema" : "http://schema.org/"
            |  },
            |  "existing" : 2
            |}""".stripMargin

      contexts.expand(input).success.value.spaces2 shouldEqual expected
    }

    "prevent double deprecations" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(ContextIsDeprecated)
    }

    "prevent publishing when deprecated" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.publish(id, 2L).failure.exception shouldEqual CommandRejected(ContextIsDeprecated)
    }

    "prevent duplicate publish" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts.publish(id, 2L).failure.exception shouldEqual CommandRejected(CannotUpdatePublished)
    }

    "prevent update when deprecated" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts
        .update(id, 2L, objectContextJson)
        .failure
        .exception shouldEqual CommandRejected(ContextIsDeprecated)
    }

    "prevent update when published" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 1L).success.value shouldEqual ContextRef(id, 2L)
      contexts
        .update(id, 2L, objectContextJson)
        .failure
        .exception shouldEqual CommandRejected(CannotUpdatePublished)
    }

    "prevent update with incorrect rev" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts
        .update(id, 2L, objectContextJson)
        .failure
        .exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent deprecate with incorrect rev" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent publish with incorrect rev" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      contexts.publish(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent referring to a deprecated context" in new Context {
      val existing = ContextId(domRef.id, genName(), genVersion())
      contexts.create(existing, objectContextJson).success
      contexts.publish(existing, 1L).success
      contexts.deprecate(existing, 2L).success

      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, arrayContextJson(existing)).failure.exception shouldEqual CommandRejected(
        IllegalImportsViolation(Set(s"Referenced context '$baseUri/contexts/${existing.show}' is deprecated")))
    }

    "prevent referring to a non-published context" in new Context {
      val existing = ContextId(domRef.id, genName(), genVersion())
      contexts.create(existing, objectContextJson).success

      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, arrayContextJson(existing)).failure.exception shouldEqual CommandRejected(
        IllegalImportsViolation(Set(s"Referenced context '$baseUri/contexts/${existing.show}' is not published")))
    }

    "prevent referring to a non existing context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      val ctxs = List(
        ContextId(DomainId(OrgId(genId()), genId()), genName(), genVersion()),
        ContextId(DomainId(orgRef.id, genId()), genName(), genVersion()),
        ContextId(domRef.id, genName(), genVersion()),
      )
      forAll(ctxs) { c =>
        contexts.create(id, arrayContextJson(c)).failure.exception shouldEqual CommandRejected(
          IllegalImportsViolation(Set(s"Referenced context '$baseUri/contexts/${c.show}' does not exist")))
      }
    }

    "prevent referring to context in a deprecated domain" in new Context {
      val deprDom  = doms.create(DomainId(orgRef.id, genId()), "domain").success.value
      val existing = ContextId(deprDom.id, genName(), genVersion())
      contexts.create(existing, objectContextJson).success
      doms.deprecate(deprDom.id, 1L).success.value

      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, arrayContextJson(existing)).failure.exception shouldEqual CommandRejected(
        IllegalImportsViolation(Set(
          s"Referenced context '$baseUri/contexts/${existing.show}' cannot be imported due to its domain deprecation status")))
    }

    "return None for a context that doesn't exist" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.fetch(id).success.value shouldEqual None
    }

    val invalidValues = List(
      Json.obj("@context" -> Json.fromString("bubu")),
      Json.obj("@context" -> Json.fromString("http://some.domain.com/context")),
      Json.obj("@context" -> Json.arr(Json.fromString("bubu"))),
      Json.obj("@context" -> Json.arr(Json.fromString("http://some.domain.com/context"))),
    )

    "prevent creation with invalid context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      forAll(invalidValues) { v =>
        contexts.create(id, v).failure.exception match {
          case CommandRejected(IllegalImportsViolation(vs)) =>
            vs should not be empty
          case _ => fail("context creation with invalid context was not rejected")
        }
      }
      contexts.create(id, Json.obj()).failure.exception match {
        case CommandRejected(ShapeConstraintViolations(vs)) =>
          vs should not be empty
        case _ => fail("context creation with invalid context was not rejected")
      }
    }

    "prevent context update with invalid context" in new Context {
      val id = ContextId(domRef.id, genName(), genVersion())
      contexts.create(id, objectContextJson).success.value shouldEqual ContextRef(id, 1L)
      forAll(invalidValues) { v =>
        contexts.update(id, 1L, v).failure.exception match {
          case CommandRejected(IllegalImportsViolation(vs)) =>
            vs should not be empty
          case _ => fail("context creation with invalid context was not rejected")
        }
      }
      contexts.update(id, 1L, Json.obj()).failure.exception match {
        case CommandRejected(ShapeConstraintViolations(vs)) =>
          vs should not be empty
        case _ => fail("context creation with invalid context was not rejected")
      }
    }
  }

  trait DomainDeprecatedContext extends Context {
    val lockedId = ContextId(domRef.id, genName(), genVersion())
    val locked   = contexts.create(lockedId, objectContextJson).success.value
    val _        = doms.deprecate(domRef.id, domRef.rev).success.value
  }

  trait OrgDeprecatedContext extends Context {
    val lockedId = ContextId(domRef.id, genName(), genVersion())
    val locked   = contexts.create(lockedId, objectContextJson).success.value
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
          Context(lockedId, 2L, objectContextJson, deprecated = true, published = false))
      }

      "prevent new context creation" in new DomainDeprecatedContext {
        val id = ContextId(domRef.id, genName(), genVersion())
        contexts
          .create(id, objectContextJson)
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
          Context(lockedId, 2L, objectContextJson, deprecated = true, published = false))
      }

      "prevent new context creation" in new OrgDeprecatedContext {
        val id = ContextId(domRef.id, genName(), genVersion())
        contexts
          .create(id, objectContextJson)
          .failure
          .exception shouldEqual CommandRejected(OrgIsDeprecated)
      }
    }
  }
}
