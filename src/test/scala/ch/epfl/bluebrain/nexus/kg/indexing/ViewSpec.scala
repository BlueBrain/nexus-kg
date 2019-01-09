package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import ch.epfl.bluebrain.nexus.commons.test.Resources
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View.{AggregateElasticView, ElasticView, SparqlView, ViewRef}
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidPayload
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectLabel, ProjectRef}
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
    val projectRef = ProjectRef(genUUID)
    val id         = Id(projectRef, iri)

    "constructing" should {
      val sparqlview     = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
      val elasticview    = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)
      val aggElasticView = jsonContentOf("/view/aggelasticview.json").appendContextOf(viewCtx)

      "return an ElasticView" in {
        val resource = simpleV(id, elasticview, types = Set(nxv.View, nxv.ElasticView, nxv.Alpha))
        View(resource).right.value shouldEqual ElasticView(
          mapping,
          Set(nxv.Schema, nxv.Resource),
          Some("one"),
          false,
          true,
          projectRef,
          iri,
          UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
          resource.rev,
          resource.deprecated
        )
      }

      "return an SparqlView" in {
        val resource = simpleV(id, sparqlview, types = Set(nxv.View, nxv.SparqlView))
        View(resource).right.value shouldEqual SparqlView(projectRef,
                                                          iri,
                                                          UUID.fromString("247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1"),
                                                          resource.rev,
                                                          resource.deprecated)

      }

      "return an AggregateElasticView from ProjectLabel ViewRef" in {
        val resource = simpleV(id, aggElasticView, types = Set(nxv.View, nxv.AggregateElasticView, nxv.Alpha))
        val views = Set(
          ViewRef(ProjectLabel("account1", "project1"), url"http://example.com/id2".value),
          ViewRef(ProjectLabel("account1", "project2"), url"http://example.com/id3".value)
        )
        View(resource).right.value shouldEqual AggregateElasticView(
          views,
          projectRef,
          UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
          iri,
          resource.rev,
          resource.deprecated)
      }

      "return an AggregateElasticView from ProjectRef ViewRef" in {
        val aggElasticViewRefs = jsonContentOf("/view/aggelasticviewrefs.json").appendContextOf(viewCtx)

        val resource = simpleV(id, aggElasticViewRefs, types = Set(nxv.View, nxv.AggregateElasticView, nxv.Alpha))
        val views = Set(
          ViewRef(ProjectRef(UUID.fromString("64b202b4-1060-42b5-9b4f-8d6a9d0d9113")),
                  url"http://example.com/id2".value),
          ViewRef(ProjectRef(UUID.fromString("d23d9578-255b-4e46-9e65-5c254bc9ad0a")),
                  url"http://example.com/id3".value)
        )
        View(resource).right.value shouldEqual AggregateElasticView(
          views,
          projectRef,
          UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
          iri,
          resource.rev,
          resource.deprecated)
      }

      "fail on AggregateElasticView when types are wrong" in {
        val resource = simpleV(id, aggElasticView, types = Set(nxv.View))
        View(resource).left.value shouldBe a[InvalidPayload]
      }

      "fail on AggregateElasticView when ViewRef collection are wrong" in {
        val wrongAggElasticView = jsonContentOf("/view/aggelasticviewwrong.json").appendContextOf(viewCtx)

        val resource = simpleV(id, wrongAggElasticView, types = Set(nxv.View, nxv.AggregateElasticView, nxv.Alpha))
        View(resource).left.value shouldBe a[InvalidPayload]
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
                                        UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
                                        1L,
                                        false)
        val views: QueryResults[View] = QueryResults(1L, List(UnscoredQueryResult(elastic)))
        views.asJson should equalIgnoreArrayOrder(jsonContentOf("/view/view-list-resp-elastic.json"))
      }

      "return the json representation for a queryresults list with SparqlView" in {
        val sparql: View =
          SparqlView(projectRef, iri, UUID.fromString("247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1"), 1L, false)
        val views: QueryResults[View] = QueryResults(1L, List(UnscoredQueryResult(sparql)))
        views.asJson should equalIgnoreArrayOrder(jsonContentOf("/view/view-list-resp-sparql.json"))
      }
    }
  }

}
