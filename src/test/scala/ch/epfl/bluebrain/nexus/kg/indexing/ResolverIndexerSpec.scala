package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import akka.actor.ActorSystem
import akka.testkit.TestKit
import cats.data.{EitherT, OptionT}
import cats.instances.future._
import ch.epfl.bluebrain.nexus.admin.client.types.Project
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ResolverCache}
import ch.epfl.bluebrain.nexus.kg.config.Contexts.resolverCtx
import ch.epfl.bluebrain.nexus.kg.config.Schemas
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resources.Event.Created
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.Unexpected
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.mockito.Mockito
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar

import scala.concurrent.Future
import scala.concurrent.duration._

class ResolverIndexerSpec
    extends TestKit(ActorSystem("ResolverIndexerSpec"))
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

  private val resources     = mock[Resources[Future]]
  private val resolverCache = mock[ResolverCache[Future]]
  private val projectCache  = mock[ProjectCache[Future]]
  private val indexer       = new ResolverIndexer(resources, resolverCache, projectCache)

  before {
    Mockito.reset(resources)
    Mockito.reset(resolverCache)
    Mockito.reset(projectCache)
  }

  "A ResolverIndexer" should {
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
    val schema = Ref(Schemas.resolverSchemaUri)

    val types = Set[AbsoluteIri](nxv.Resolver, nxv.CrossProject)

    val json      = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val resource  = ResourceF.simpleF(id, json, rev = 2, schema = schema, types = types)
    val resourceV = simpleV(id, json, rev = 2, schema = schema, types = types)
    val resolver  = Resolver(resourceV).value
    val ev        = Created(id, schema, types, json, clock.instant(), Anonymous)

    "index a resolver" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some(resource))
      when(resources.materialize(resource)(project)).thenReturn(EitherT.rightT[Future, Rejection](resourceV))
      when(resolverCache.put(resolver)).thenReturn(Future.successful(()))
      when(projectCache.get(projectRef)).thenReturn(Future.successful(Some(project)))

      indexer(ev).futureValue shouldEqual (())
      verify(resolverCache, times(1)).put(resolver)
    }

    "skip indexing a resolver when the resource cannot be found" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.none[Future, Resource])
      indexer(ev).futureValue shouldEqual (())
      verify(resolverCache, times(0)).put(resolver)
    }

    "skip indexing a resolver when the resource cannot be materialized" in {
      when(resources.fetch(id, None)).thenReturn(OptionT.some(resource))
      when(resources.materialize(resource)(project))
        .thenReturn(EitherT.leftT[Future, ResourceV](Unexpected("error"): Rejection))
      when(projectCache.get(projectRef)).thenReturn(Future.successful(Some(project)))
      indexer(ev).futureValue shouldEqual (())
      verify(resolverCache, times(0)).put(resolver)
    }
  }

}
