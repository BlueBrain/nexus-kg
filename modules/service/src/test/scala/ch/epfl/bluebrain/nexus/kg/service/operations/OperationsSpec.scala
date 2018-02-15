package ch.epfl.bluebrain.nexus.kg.service.operations

import java.time.Clock

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test._
import ch.epfl.bluebrain.nexus.kg.service.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.service.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.service.config.AppConfig.OperationsConfig
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.Agg
import ch.epfl.bluebrain.nexus.kg.service.operations.Operations.ResourceRejection._
import ch.epfl.bluebrain.nexus.kg.service.operations.OperationsSpec._
import ch.epfl.bluebrain.nexus.kg.service.refs.RevisionedRef
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import journal.Logger
import org.scalatest.{Inspectors, Matchers, TryValues, WordSpecLike}

import scala.util.Try

class OperationsSpec extends WordSpecLike with Matchers with Inspectors with TryValues with Randomness with Resources {

  private def genJson(): Json =
    Json.obj("key" -> Json.fromString(genString()))

  private def genName(): String =
    genString(length = 8, Vector.range('a', 'z') ++ Vector.range('0', '9'))

  private implicit val caller = AnonymousCaller(Anonymous())
  private implicit val clock  = Clock.systemUTC

  trait Context {
    implicit val opConfig = OperationsConfig(10)
    val agg               = MemoryAggregate("operations")(ResourceState.Initial, TestOperations.next, TestOperations.eval).toF[Try]
    val operations        = TestOperations(agg)
  }

  "A Resource instance" should {

    "create a new resource" in new Context {
      val id   = Id(genName())
      val json = genJson()
      operations.create(id, json).success.value shouldEqual RevisionedRef(id, 1L)
      operations.fetch(id).success.value shouldEqual Some(Res(id, 1L, json, deprecated = false))
    }

    "reject creating a new resource with longer than allowed length" in new Context {
      val id   = Id(genName() + genName())
      val json = genJson()
      operations.create(id, json).failure.exception shouldEqual CommandRejected(InvalidId(id))
    }

    "update a resource" in new Context {
      val id          = Id(genName())
      val json        = genJson()
      val jsonUpdated = genJson()
      operations.create(id, json).success.value shouldEqual RevisionedRef(id, 1L)
      operations.update(id, 1L, jsonUpdated).success.value shouldEqual RevisionedRef(id, 2L)
      operations.fetch(id).success.value shouldEqual Some(Res(id, 2L, jsonUpdated, deprecated = false))
    }

    "deprecate a resource" in new Context {
      val id   = Id(genName())
      val json = genJson()
      operations.create(id, json).success.value shouldEqual RevisionedRef(id, 1L)
      operations.deprecate(id, 1L).success.value shouldEqual RevisionedRef(id, 2L)
      operations.fetch(id).success.value shouldEqual Some(Res(id, 2L, json, deprecated = true))
    }

    "fetch old revision of a resource" in new Context {
      val id          = Id(genName())
      val json        = genJson()
      val jsonUpdated = genJson()
      operations.create(id, json).success
      operations.update(id, 1L, jsonUpdated).success
      operations.fetch(id, 2L).success.value shouldEqual Some(Res(id, 2L, jsonUpdated, deprecated = false))
      operations.fetch(id, 1L).success.value shouldEqual Some(Res(id, 1L, json, deprecated = false))
    }

    "return None when fetching a revision that does not exist" in new Context {
      val id = Id(genName())
      operations.create(id, genJson()).success
      operations.fetch(id, 10L).success.value shouldEqual None
    }

    "prevent double deprecations" in new Context {
      val id   = Id(genName())
      val json = genJson()
      operations.create(id, json).success.value shouldEqual RevisionedRef(id, 1L)
      operations.deprecate(id, 1L).success.value shouldEqual RevisionedRef(id, 2L)
      operations.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(ResourceIsDeprecated)
    }

    "prevent update when deprecated" in new Context {
      val id = Id(genName())
      operations.create(id, genJson()).success.value shouldEqual RevisionedRef(id, 1L)
      operations.deprecate(id, 1L).success.value shouldEqual RevisionedRef(id, 2L)
      operations.update(id, 2L, genJson()).failure.exception shouldEqual CommandRejected(ResourceIsDeprecated)
    }

    "prevent update with incorrect rev" in new Context {
      val id = Id(genName())
      operations.create(id, genJson()).success.value shouldEqual RevisionedRef(id, 1L)
      operations.update(id, 2L, genJson()).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "prevent deprecate with incorrect rev" in new Context {
      val id = Id(genName())
      operations.create(id, genJson()).success.value shouldEqual RevisionedRef(id, 1L)
      operations.deprecate(id, 2L).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "return None for a schema that doesn't exist" in new Context {
      val id = Id(genName())
      operations.fetch(id).success.value shouldEqual None
    }

  }
}

object OperationsSpec {
  import TestOperations._
  case class Res(id: Id, rev: Long, value: Json, deprecated: Boolean)

  class TestOperations(agg: Agg[Try, Id, ResourceTestEvent])(implicit config: OperationsConfig)
      extends Operations[Try, Id, Json, ResourceTestEvent](agg, logger) {

    override type Resource = Res

    override implicit def buildResource(c: ResourceState.Current[Id, Json]): Resource =
      Res(c.id, c.rev, c.value, c.deprecated)
  }

  object TestOperations {

    final def apply(agg: Agg[Try, Id, ResourceTestEvent])(implicit config: OperationsConfig): TestOperations =
      new TestOperations(agg)

    private[operations] val logger = Logger[this.type]

    private[operations] def next(state: ResourceState, event: ResourceTestEvent): ResourceState =
      ResourceState.next[Id, Json, ResourceTestEvent](state, event)

    private[operations] def eval(
        state: ResourceState,
        cmd: Operations.ResourceCommand[Id]): Either[Operations.ResourceRejection, ResourceTestEvent] =
      ResourceState.eval[Id, Json, ResourceTestEvent](state, cmd)

  }
}
