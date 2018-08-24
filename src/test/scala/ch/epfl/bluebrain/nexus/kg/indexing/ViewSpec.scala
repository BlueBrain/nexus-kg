package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}

import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{ElasticView, SparqlView}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidPayload
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectRef}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.syntax._
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class ViewSpec extends WordSpecLike with Matchers with OptionValues with Resources with TestHelper with Inspectors {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "A View" when {
    val mapping    = jsonContentOf("/elastic/mapping.json")
    val iri        = url"http://example.com/id".value
    val projectRef = ProjectRef("ref")
    val id         = Id(projectRef, iri)
    "constructing" should {
      val sparqlview  = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
      val elasticview = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)

      "return an ElasticView" in {
        val resource = simpleV(id, elasticview, types = Set(nxv.View, nxv.ElasticView, nxv.Alpha))
        View(resource).right.value shouldEqual ElasticView(mapping,
                                                           Set(nxv.Schema, nxv.Resource),
                                                           Some("one"),
                                                           false,
                                                           true,
                                                           projectRef,
                                                           iri,
                                                           "3aa14a1a-81e7-4147-8306-136d8270bb01",
                                                           resource.rev,
                                                           resource.deprecated)
      }

      "return an SparqlView" in {
        val resource = simpleV(id, sparqlview, types = Set(nxv.View, nxv.SparqlView))
        View(resource).right.value shouldEqual SparqlView(projectRef,
                                                          iri,
                                                          "247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1",
                                                          resource.rev,
                                                          resource.deprecated)

      }

      "fail on ElasticView when types are wrong" in {
        val resource = simpleV(id, elasticview, types = Set(nxv.View))
        View(resource).left.value shouldBe a[InvalidPayload]
      }

      "fail on ElasticView when invalid payload" in {
        val wrong = List(jsonContentOf("/view/elasticview-wrong.json").appendContextOf(viewCtx),
                         jsonContentOf("/view/elasticview-wrong-2.json").appendContextOf(viewCtx))
        forAll(wrong) { json =>
          val resource = simpleV(id, json, types = Set(nxv.View, nxv.ElasticView, nxv.Alpha))
          View(resource).left.value shouldBe a[InvalidPayload]
        }
      }

      "fail on SparqlView when types are wrong" in {
        val resource = simpleV(id, sparqlview, types = Set(nxv.Schema))
        View(resource).left.value shouldBe a[InvalidPayload]
      }

      "fail on SparqlView when invalid payload" in {
        val resource =
          simpleV(id,
                  jsonContentOf("/view/sparqlview-wrong.json").appendContextOf(viewCtx),
                  types = Set(nxv.View, nxv.ElasticView, nxv.Alpha))
        View(resource).left.value shouldBe a[InvalidPayload]
      }
    }

    "converting into json (from Graph)" should {
      "return the json representation for a queryresults list with ElasticView" in {
        val elastic: View = ElasticView(mapping,
                                        Set(nxv.Schema, nxv.Resource),
                                        Some("one"),
                                        false,
                                        true,
                                        projectRef,
                                        iri,
                                        "3aa14a1a-81e7-4147-8306-136d8270bb01",
                                        1L,
                                        false)
        val views: QueryResults[View] = QueryResults(1L, List(UnscoredQueryResult(elastic)))
        views.asJson should equalIgnoreArrayOrder(jsonContentOf("/view/view-list-resp-elastic.json"))
      }

      "return the json representation for a queryresults list with SparqlView" in {
        val sparql: View              = SparqlView(projectRef, iri, "247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1", 1L, false)
        val views: QueryResults[View] = QueryResults(1L, List(UnscoredQueryResult(sparql)))
        views.asJson should equalIgnoreArrayOrder(jsonContentOf("/view/view-list-resp-sparql.json"))
      }
    }
  }

}
