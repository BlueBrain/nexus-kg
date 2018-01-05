package ch.epfl.bluebrain.nexus.kg.core.queries

import java.time.Clock
import java.util.UUID
import java.util.regex.Pattern

import cats.instances.try_._
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Caller.AnonymousCaller
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.Anonymous
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.kg.core.CallerCtx._
import ch.epfl.bluebrain.nexus.kg.core.Fault.CommandRejected
import ch.epfl.bluebrain.nexus.kg.core.queries.Queries._
import ch.epfl.bluebrain.nexus.kg.core.queries.QueryRejection._
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate
import ch.epfl.bluebrain.nexus.sourcing.mem.MemoryAggregate._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, TryValues, WordSpecLike}

import scala.util.{Random, Try}

class QueriesSpec extends WordSpecLike with Matchers with Inspectors with TryValues with Randomness with Resources {

  private def genId(): QueryId = QueryId(UUID.randomUUID().toString.toLowerCase())

  private def genJson(resource: String): Json = {
    val queries = List("/queries/query-simple.json", "/queries/query-filter.json", "/queries/query-sort.json")
    jsonContentOf(Random.shuffle(queries).head,
                  Map(Pattern.quote("{{resource}}")   -> resource,
                      Pattern.quote(""""{{from}}"""") -> "0",
                      Pattern.quote(""""{{size}}"""") -> "10"))
  }

  private implicit val clock: Clock   = Clock.systemUTC
  private implicit val caller: Caller = AnonymousCaller(Anonymous())

  "A Queries instance" should {
    val agg     = MemoryAggregate("queries")(initial, next, eval).toF[Try]
    val queries = Queries(agg)

    "create a new query" in {
      val json = genJson("instances")
      queries.create(json).success.value.rev shouldEqual 1L
    }

    "update a new query" in {
      val json  = genJson("contexts")
      val json2 = genJson("schemas")
      val ref   = queries.create(json).success.value
      queries.update(ref.id, 1, json2).success.value shouldEqual QueryRef(ref.id, 2L)
      queries.fetch(ref.id).success.value shouldEqual Some(Query(ref.id, 2L, json2))
    }

    "fetch old revision of a query" in {
      val json  = genJson("domains")
      val json2 = genJson("organizations")
      val ref   = queries.create(json).success.value
      queries.update(ref.id, 1, json2).success.value shouldEqual QueryRef(ref.id, 2L)
      queries.fetch(ref.id).success.value shouldEqual Some(Query(ref.id, 2L, json2))
      queries.fetch(ref.id, 1L).success.value shouldEqual Some(Query(ref.id, 1L, json))
    }

    "return None when fetching a revision that does not exist" in {
      val json = genJson("instances")
      val ref  = queries.create(json).success.value
      queries.fetch(ref.id, 4L).success.value shouldEqual None
    }

    "prevent update on missing query" in {
      val json = genJson("instances")
      queries.update(genId(), 0L, json).failure.exception shouldEqual CommandRejected(QueryDoesNotExist)
    }

    "prevent update with incorrect rev" in {
      val json = genJson("instances")
      val ref  = queries.create(json).success.value
      queries.update(ref.id, 2L, json).failure.exception shouldEqual CommandRejected(IncorrectRevisionProvided)
    }

    "return a None when fetching a query that doesn't exists" in {
      queries.fetch(genId()).success.value shouldEqual None
    }
  }
}
