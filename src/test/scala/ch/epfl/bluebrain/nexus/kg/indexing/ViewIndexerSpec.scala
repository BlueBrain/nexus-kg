package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import akka.pattern.AskTimeoutException
import cats.data.{EitherT, OptionT}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.KgError.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.async.ViewCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.{KgError, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import org.mockito.Mockito._
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest._

import scala.concurrent.duration._

class ViewIndexerSpec
    extends ActorSystemFixture("ViewIndexerSpec")
    with WordSpecLike
    with Matchers
    with IOEitherValues
    with IOOptionValues
    with BeforeAndAfter
    with test.Resources
    with IdiomaticMockito
    with TestHelper {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 15 milliseconds)

  private val resources = mock[Resources[IO]]
  private val viewCache = mock[ViewCache[IO]]
  private val indexer   = new ViewIndexer(resources, viewCache)

  before {
    Mockito.reset(resources)
    Mockito.reset(viewCache)
  }

  "A ViewIndexer" should {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val iri                   = Iri.absolute("http://example.com/id").right.value
    val projectRef            = ProjectRef(genUUID)
    val id                    = Id(projectRef, iri)
    //TODO: Change to view SHACL schema when we have one
    val schema = Ref(Schemas.resolverSchemaUri)

    val types = Set[AbsoluteIri](nxv.View, nxv.SparqlView)

    val json       = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
    val resource   = ResourceF.simpleF(id, json, rev = 2, schema = schema, types = types)
    val resourceV  = simpleV(id, json, rev = 2, schema = schema, types = types)
    val view       = View(resourceV).right.value
    val ev         = Created(id, schema, types, json, clock.instant(), Anonymous)
    val unit: Unit = ()

    "index a view" in {
      resources.fetch(id, None) shouldReturn OptionT.some(resource)
      resources.materialize(resource) shouldReturn EitherT.rightT[IO, Rejection](resourceV)
      viewCache.put(view) shouldReturn IO.unit

      indexer(ev).ioValue shouldEqual unit
      verify(viewCache, times(1)).put(view)
    }

    "skip indexing a resolver when the resource cannot be found" in {
      resources.fetch(id, None) shouldReturn OptionT.none[IO, Resource]
      indexer(ev).ioValue shouldEqual unit
      verify(viewCache, times(0)).put(view)
    }

    "skip indexing a resolver when the resource cannot be materialized" in {
      resources.fetch(id, None) shouldReturn OptionT.some(resource)
      val err = IO.raiseError[Either[Rejection, ResourceV]](KgError.InternalError(""))
      resources.materialize(resource) shouldReturn EitherT(err)
      indexer(ev).failed[KgError.InternalError]
      verify(viewCache, times(0)).put(view)
    }

    "raise RetriableError when cache fails due to an AskTimeoutException" in {
      resources.fetch(id, None) shouldReturn OptionT.some(resource)
      resources.materialize(resource) shouldReturn EitherT.rightT[IO, Rejection](resourceV)
      viewCache.put(view) shouldReturn IO.raiseError(new AskTimeoutException("error"))
      indexer(ev).failed[RetriableErr]
      verify(viewCache, times(1)).put(view)
    }

    "raise RetriableError when cache fails due to an OperationTimedOut" in {
      resources.fetch(id, None) shouldReturn OptionT.some(resource)
      resources.materialize(resource) shouldReturn EitherT.rightT[IO, Rejection](resourceV)
      viewCache.put(view) shouldReturn IO.raiseError(OperationTimedOut("error"))
      indexer(ev).failed[RetriableErr]
      verify(viewCache, times(1)).put(view)
    }
  }

}
