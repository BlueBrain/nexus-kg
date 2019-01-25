package ch.epfl.bluebrain.nexus.kg.async

import java.time.Clock

import akka.testkit._
import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig._
import ch.epfl.bluebrain.nexus.kg.config.Settings
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticSearchView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.resources.ProjectRef
import ch.epfl.bluebrain.nexus.service.test.ActorSystemFixture
import io.circe.Json
import monix.eval.Task
import monix.execution.Scheduler.Implicits.global
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Inspectors, Matchers}

import scala.concurrent.duration._

class ViewCacheSpec
    extends ActorSystemFixture("ViewCacheSpec", true)
    with Randomness
    with Matchers
    with Inspectors
    with ScalaFutures
    with TestHelper {

  override implicit def patienceConfig: PatienceConfig = PatienceConfig(3.seconds.dilated, 5.milliseconds)

  private def genJson: Json = Json.obj("key" -> Json.fromString(genString()))

  private implicit val clock = Clock.systemUTC

  private implicit val appConfig = Settings(system).appConfig

  val ref1 = ProjectRef(genUUID)
  val ref2 = ProjectRef(genUUID)

  val esView = ElasticSearchView(Json.obj(), Set.empty, None, false, true, ref1, genIri, genUUID, 1L, false)

  val sparqlView = SparqlView(ref1, nxv.defaultSparqlIndex.value, genUUID, 1L, false)

  val esViewsProj1     = List.fill(5)(esView.copy(mapping = genJson, id = genIri + "elasticSearch1", uuid = genUUID)).toSet
  val sparqlViewsProj1 = List.fill(5)(sparqlView.copy(id = genIri + "sparql1", uuid = genUUID)).toSet
  val esViewsProj2 =
    List.fill(5)(esView.copy(mapping = genJson, id = genIri + "elasticSearch2", uuid = genUUID, ref = ref2)).toSet
  val sparqlViewsProj2 = List.fill(5)(sparqlView.copy(id = genIri + "sparql2", uuid = genUUID, ref = ref2)).toSet

  private val cache = ViewCache[Task]

  "ViewCache" should {

    "index views" in {
      forAll(esViewsProj1 ++ sparqlViewsProj1 ++ esViewsProj2 ++ sparqlViewsProj2) { view =>
        cache.put(view).runToFuture.futureValue
        cache.getBy[View](view.ref, view.id).runToFuture.futureValue shouldEqual Some(view)
      }
    }

    "get views" in {
      forAll(esViewsProj1) { view =>
        cache.put(view).runToFuture.futureValue
        cache.getBy[ElasticSearchView](view.ref, view.id).runToFuture.futureValue shouldEqual Some(view)
        cache.getBy[SparqlView](view.ref, view.id).runToFuture.futureValue shouldEqual None
      }
    }

    "list views" in {
      cache.get(ref1).runToFuture.futureValue shouldEqual (esViewsProj1 ++ sparqlViewsProj1)
      cache.get(ref2).runToFuture.futureValue shouldEqual (esViewsProj2 ++ sparqlViewsProj2)
    }

    "list filtering by type" in {
      cache.getBy[ElasticSearchView](ref1).runToFuture.futureValue shouldEqual esViewsProj1
      cache.getBy[SparqlView](ref2).runToFuture.futureValue shouldEqual sparqlViewsProj2
    }

    "deprecate view" in {
      val view = esViewsProj1.head
      cache.put(view.copy(deprecated = true, rev = 2L)).runToFuture.futureValue
      cache.getBy[View](view.ref, view.id).runToFuture.futureValue shouldEqual None
      val expectedSet = esViewsProj1.filterNot(_ == view) ++ sparqlViewsProj1
      cache.get(ref1).runToFuture.futureValue shouldEqual expectedSet
    }
  }
}
