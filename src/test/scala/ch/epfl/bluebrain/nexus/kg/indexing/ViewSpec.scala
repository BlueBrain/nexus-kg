package ch.epfl.bluebrain.nexus.kg.indexing

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID

import cats.implicits._
import ch.epfl.bluebrain.nexus.commons.search.QueryResult.UnscoredQueryResult
import ch.epfl.bluebrain.nexus.commons.search.QueryResults
import ch.epfl.bluebrain.nexus.commons.test.{CirceEq, Resources}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View._
import ch.epfl.bluebrain.nexus.kg.indexing.ViewEncoder._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.InvalidResourceFormat
import ch.epfl.bluebrain.nexus.kg.resources.{Id, ProjectLabel, ProjectRef}
import ch.epfl.bluebrain.nexus.kg.search.QueryResultEncoder._
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.scalatest.{Inspectors, Matchers, OptionValues, WordSpecLike}

class ViewSpec
    extends WordSpecLike
    with Matchers
    with OptionValues
    with Resources
    with TestHelper
    with Inspectors
    with CirceEq {
  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "A View" when {
    val mapping              = jsonContentOf("/elasticsearch/mapping.json")
    val iri                  = url"http://example.com/id".value
    val projectRef           = ProjectRef(genUUID)
    val id                   = Id(projectRef, iri)
    val sparqlview           = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
    val sparqlview2          = jsonContentOf("/view/sparqlview-tag-schema.json").appendContextOf(viewCtx)
    val elasticSearchview    = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)
    val aggElasticSearchView = jsonContentOf("/view/aggelasticview.json").appendContextOf(viewCtx)
    val aggSparqlView        = jsonContentOf("/view/aggsparql.json").appendContextOf(viewCtx)

    "constructing" should {

      "return an ElasticSearchView" in {
        val resource = simpleV(id, elasticSearchview, types = Set(nxv.View, nxv.ElasticSearchView))
        View(resource).right.value shouldEqual ElasticSearchView(
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
        View(resource).right.value shouldEqual SparqlView(Set.empty,
                                                          None,
                                                          false,
                                                          projectRef,
                                                          iri,
                                                          UUID.fromString("247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1"),
                                                          resource.rev,
                                                          resource.deprecated)

      }

      "return an SparqlView with tag and schema" in {
        val resource = simpleV(id, sparqlview2, types = Set(nxv.View, nxv.SparqlView))
        View(resource).right.value shouldEqual SparqlView(Set(nxv.Schema, nxv.Resource),
                                                          Some("one"),
                                                          true,
                                                          projectRef,
                                                          iri,
                                                          UUID.fromString("247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1"),
                                                          resource.rev,
                                                          resource.deprecated)

      }

      "return an AggregateElasticSearchView from ProjectLabel ViewRef" in {
        val resource =
          simpleV(id, aggElasticSearchView, types = Set(nxv.View, nxv.AggregateElasticSearchView))
        val views = Set(
          ViewRef(ProjectLabel("account1", "project1"), url"http://example.com/id2".value),
          ViewRef(ProjectLabel("account1", "project2"), url"http://example.com/id3".value)
        )
        View(resource).right.value shouldEqual AggregateElasticSearchView(
          views,
          projectRef,
          UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
          iri,
          resource.rev,
          resource.deprecated)
      }

      "return an AggregateSparqlView from ProjectLabel ViewRef" in {
        val resource =
          simpleV(id, aggSparqlView, types = Set(nxv.View, nxv.AggregateSparqlView))
        val views = Set(
          ViewRef(ProjectLabel("account1", "project1"), url"http://example.com/id2".value),
          ViewRef(ProjectLabel("account1", "project2"), url"http://example.com/id3".value)
        )
        View(resource).right.value shouldEqual AggregateSparqlView(
          views,
          projectRef,
          UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
          iri,
          resource.rev,
          resource.deprecated)
      }

      "return an AggregateElasticSearchView from ProjectRef ViewRef" in {
        val aggElasticSearchViewRefs = jsonContentOf("/view/aggelasticviewrefs.json").appendContextOf(viewCtx)

        val resource =
          simpleV(id, aggElasticSearchViewRefs, types = Set(nxv.View, nxv.AggregateElasticSearchView))
        val views = Set(
          ViewRef(ProjectRef(UUID.fromString("64b202b4-1060-42b5-9b4f-8d6a9d0d9113")),
                  url"http://example.com/id2".value),
          ViewRef(ProjectRef(UUID.fromString("d23d9578-255b-4e46-9e65-5c254bc9ad0a")),
                  url"http://example.com/id3".value)
        )
        View(resource).right.value shouldEqual AggregateElasticSearchView(
          views,
          projectRef,
          UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
          iri,
          resource.rev,
          resource.deprecated)
      }

      "fail on AggregateElasticSearchView when types are wrong" in {
        val resource = simpleV(id, aggElasticSearchView, types = Set(nxv.View))
        View(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on AggregateSparqlView when types are wrong" in {
        val resource = simpleV(id, aggSparqlView, types = Set(nxv.View))
        View(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on AggregateElasticSearchView when ViewRef collection are wrong" in {
        val wrongAggElasticSearchView = jsonContentOf("/view/aggelasticviewwrong.json").appendContextOf(viewCtx)

        val resource =
          simpleV(id, wrongAggElasticSearchView, types = Set(nxv.View, nxv.AggregateElasticSearchView))
        View(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on ElasticSearchView when types are wrong" in {
        val resource = simpleV(id, elasticSearchview, types = Set(nxv.View))
        View(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on ElasticSearchView when invalid payload" in {
        val wrong = List(
          jsonContentOf("/view/elasticview-wrong.json").appendContextOf(viewCtx),
          jsonContentOf("/view/elasticview-wrong-2.json").appendContextOf(viewCtx),
          jsonContentOf("/view/elasticview-wrong-3.json").appendContextOf(viewCtx)
        )
        forAll(wrong) { json =>
          val resource = simpleV(id, json, types = Set(nxv.View, nxv.ElasticSearchView))
          View(resource).left.value shouldBe a[InvalidResourceFormat]
        }
      }

      "fail on SparqlView when types are wrong" in {
        val resource = simpleV(id, sparqlview, types = Set(nxv.Schema))
        View(resource).left.value shouldBe a[InvalidResourceFormat]
      }

      "fail on SparqlView when invalid payload" in {
        val resource =
          simpleV(id,
                  jsonContentOf("/view/sparqlview-wrong.json").appendContextOf(viewCtx),
                  types = Set(nxv.View, nxv.ElasticSearchView))
        View(resource).left.value shouldBe a[InvalidResourceFormat]
      }
    }

    "converting into json (from Graph)" should {
      val views = Set(
        ViewRef(ProjectLabel("account1", "project1"), url"http://example.com/id2".value),
        ViewRef(ProjectLabel("account1", "project2"), url"http://example.com/id3".value)
      )

      "return the json representation for an AggregateElasticSearchView" in {
        val agg: View = AggregateElasticSearchView(views,
                                                   projectRef,
                                                   UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
                                                   iri,
                                                   1L,
                                                   deprecated = false)
        agg
          .as[Json](viewCtx.appendContextOf(resourceCtx))
          .right
          .value
          .removeKeys("@context", "_rev", "_deprecated") should equalIgnoreArrayOrder(
          aggElasticSearchView.removeKeys("@context"))
      }

      "return the json representation for an AggregateSparqlView" in {
        val agg: View = AggregateSparqlView(views,
                                            projectRef,
                                            UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
                                            iri,
                                            1L,
                                            deprecated = false)
        agg
          .as[Json](viewCtx.appendContextOf(resourceCtx))
          .right
          .value
          .removeKeys("@context", "_rev", "_deprecated") should equalIgnoreArrayOrder(
          aggSparqlView.removeKeys("@context"))
      }

      "return the json representation for a queryresults list with ElasticSearchView" in {
        val elasticSearch: View = ElasticSearchView(mapping,
                                                    Set(nxv.Schema, nxv.Resource),
                                                    Some("one"),
                                                    false,
                                                    true,
                                                    projectRef,
                                                    iri,
                                                    UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
                                                    1L,
                                                    false)
        val views: QueryResults[View] = QueryResults(1L, List(UnscoredQueryResult(elasticSearch)))
        ViewEncoder.json(views).right.value should equalIgnoreArrayOrder(
          jsonContentOf("/view/view-list-resp-elastic.json"))
      }

      "return the json representation for a queryresults list with SparqlView" in {
        val sparql: View =
          SparqlView(Set.empty,
                     None,
                     true,
                     projectRef,
                     iri,
                     UUID.fromString("247d223b-1d38-4c6e-8fed-f9a8c2ccb4a1"),
                     1L,
                     false)
        val views: QueryResults[View] = QueryResults(1L, List(UnscoredQueryResult(sparql)))
        ViewEncoder.json(views).right.value should equalIgnoreArrayOrder(
          jsonContentOf("/view/view-list-resp-sparql.json"))
      }
    }
  }

}
