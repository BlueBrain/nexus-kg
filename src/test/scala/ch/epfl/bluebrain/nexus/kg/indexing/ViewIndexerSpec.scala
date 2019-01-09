package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.pattern.AskTimeoutException
import akka.testkit.TestKit
import cats.data.{EitherT, OptionT}
import cats.instances.future._
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.Unexpected
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future
import scala.concurrent.duration._

class ViewIndexerSpec
    extends TestKit(ActorSystem("ViewIndexerSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with ScalaFutures
    with BeforeAndAfter
    with test.Resources
    with TestHelper
    with EitherValues
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 0.3 seconds)

  import system.dispatcher

  private val resources = mock[Resources[Future]]
  private val cache     = mock[DistributedCache[Future]]
  private val indexer   = new ViewIndexer(resources, cache)

  before {
    Mockito.reset(resources)
    Mockito.reset(cache)
  }

  "A ViewIndexer" should {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val iri                   = Iri.absolute("http://example.com/id").right.value
    val projectRef            = ProjectRef(genUUID)
    val id                    = Id(projectRef, iri)
    //TODO: Change to view SHACL schema when we have one
    val schema = Ref(Schemas.resolverSchemaUri)

    val types = Set[AbsoluteIri](nxv.View, nxv.SparqlView)

    val json      = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
    val resource  = ResourceF.simpleF(id, json, rev = 2, schema = schema, types = types)
    val resourceV = simpleV(id, json, rev = 2, schema = schema, types = types)
    val view      = View(resourceV).right.value
    val ev        = Created(id, schema, types, json, clock.instant(), Anonymous)

    "index a view" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some(resource))
      when(resources.materialize(resource)).thenReturn(EitherT.rightT[Future, Rejection](resourceV))
      when(cache.applyView(projectRef, view)).thenReturn(Future.successful(()))

      indexer(ev).futureValue shouldEqual (())
      verify(cache, times(1)).applyView(projectRef, view)
    }

    "skip indexing a resolver when the resource cannot be found" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.none[Future, Resource])
      indexer(ev).futureValue shouldEqual (())
      verify(cache, times(0)).applyView(projectRef, view)
    }

    "skip indexing a resolver when the resource cannot be materialized" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some(resource))
      when(resources.materialize(resource)).thenReturn(EitherT.leftT[Future, ResourceV](Unexpected("error"): Rejection))
      indexer(ev).futureValue shouldEqual (())
      verify(cache, times(0)).applyView(projectRef, view)
    }

    "raise RetriableError when cache fails due to an AskTimeoutException" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some(resource))
      when(resources.materialize(resource)).thenReturn(EitherT.rightT[Future, Rejection](resourceV))
      when(cache.applyView(projectRef, view))
        .thenReturn(Future.failed(new AskTimeoutException("error")))
      whenReady(indexer(ev).failed)(_ shouldBe a[RetriableErr])
      verify(cache, times(1)).applyView(projectRef, view)
    }

    "raise RetriableError when cache fails due to an OperationTimedOut" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some(resource))
      when(resources.materialize(resource)).thenReturn(EitherT.rightT[Future, Rejection](resourceV))
      when(cache.applyView(projectRef, view)).thenReturn(Future.failed(new OperationTimedOut("error")))
      whenReady(indexer(ev).failed)(_ shouldBe a[RetriableErr])
      verify(cache, times(1)).applyView(projectRef, view)
    }
  }

}
