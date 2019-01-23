package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.pattern.AskTimeoutException
import akka.testkit.TestKit
import cats.data.{EitherT, OptionT}
import cats.effect.IO
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.IOEitherValues
import ch.epfl.bluebrain.nexus.commons.types.RetriableErr
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.RuntimeErr.OperationTimedOut
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.Unexpected
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.duration._

class ViewIndexerSpec
    extends TestKit(ActorSystem("ViewIndexerSpec"))
    with WordSpecLike
    with Matchers
    with MockitoSugar
    with IOEitherValues
    with BeforeAndAfter
    with test.Resources
    with TestHelper
    with OptionValues {

  override implicit val patienceConfig: PatienceConfig = PatienceConfig(3 seconds, 15 milliseconds)

  private implicit val ioTimer = IO.timer(system.dispatcher)

  private val resources    = mock[Resources[IO]]
  private val viewCache    = mock[ViewCache[IO]]
  private val projectCache = mock[ProjectCache[IO]]
  private val indexer      = new ViewIndexer(resources, viewCache, projectCache)

  before {
    Mockito.reset(resources)
    Mockito.reset(viewCache)
    Mockito.reset(projectCache)
  }

  "A ViewIndexer" should {
    implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
    val base                  = Iri.absolute("http://example.com").right.value
    val voc                   = base + "voc"
    val iri                   = base + "id"
    val subject               = base + "anonymous"
    val projectRef            = ProjectRef(genUUID)
    val id                    = Id(projectRef, iri)
    val project = Project(id.value,
                          "proj",
                          "org",
                          None,
                          base,
                          voc,
                          Map.empty,
                          projectRef.id,
                          genUUID,
                          1L,
                          deprecated = false,
                          Instant.now(clock),
                          subject,
                          Instant.now(clock),
                          subject)
    //TODO: Change to view SHACL schema when we have one
    val schema = Ref(Schemas.resolverSchemaUri)

    val types = Set[AbsoluteIri](nxv.View, nxv.SparqlView)

    val json      = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
    val resource  = ResourceF.simpleF(id, json, rev = 2, schema = schema, types = types)
    val resourceV = simpleV(id, json, rev = 2, schema = schema, types = types)
    val view      = View(resourceV).right.value
    val ev        = Created(id, schema, types, json, clock.instant(), Anonymous)

    "index a view" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some[IO](resource))
      when(resources.materialize(resource)(project)).thenReturn(EitherT.rightT[IO, Rejection](resourceV))
      when(projectCache.get(projectRef)).thenReturn(IO.pure(Some(project)))
      when(viewCache.put(view)).thenReturn(IO.pure(()))

      indexer(ev).ioValue shouldEqual (())
      verify(viewCache, times(1)).put(view)
    }

    "skip indexing a resolver when the resource cannot be found" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.none[IO, Resource])
      indexer(ev).ioValue shouldEqual (())
      verify(viewCache, times(0)).put(view)
    }

    "skip indexing a resolver when the resource cannot be materialized" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some[IO](resource))
      when(resources.materialize(resource)(project))
        .thenReturn(EitherT.leftT[IO, ResourceV](Unexpected("error"): Rejection))
      when(projectCache.get(projectRef)).thenReturn(IO.pure(Some(project)))
      indexer(ev).ioValue shouldEqual (())
      verify(viewCache, times(0)).put(view)
    }

    "raise RetriableError when cache fails due to an AskTimeoutException" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some[IO](resource))
      when(resources.materialize(resource)(project)).thenReturn(EitherT.rightT[IO, Rejection](resourceV))
      when(projectCache.get(projectRef)).thenReturn(IO.pure(Some(project)))
      when(viewCache.put(view))
        .thenReturn(IO.raiseError(new AskTimeoutException("error")))
      indexer(ev).failed[RetriableErr]
      verify(viewCache, times(1)).put(view)
    }

    "raise RetriableError when cache fails due to an OperationTimedOut" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some[IO](resource))
      when(resources.materialize(resource)(project)).thenReturn(EitherT.rightT[IO, Rejection](resourceV))
      when(projectCache.get(projectRef)).thenReturn(IO.pure(Some(project)))
      when(viewCache.put(view)).thenReturn(IO.raiseError(new OperationTimedOut("error")))
      indexer(ev).failed[RetriableErr]
      verify(viewCache, times(1)).put(view)
    }
  }

}
