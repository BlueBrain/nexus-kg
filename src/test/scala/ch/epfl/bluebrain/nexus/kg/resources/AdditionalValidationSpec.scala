package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}
import java.util.UUID
import java.util.regex.Pattern.quote

import akka.http.scaladsl.model.StatusCodes
import cats.effect.IO
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticServerError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.commons.test.io.{IOEitherValues, IOOptionValues}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity._
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.{ProjectCache, ViewCache}
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticSearchConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticSearchView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.kg.{KgError, TestHelper}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.Path._
import ch.epfl.bluebrain.nexus.rdf.Iri.{AbsoluteIri, Path}
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._
import io.circe.Json
import io.circe.parser.parse
import org.mockito.matchers.MacroBasedMatchers
import org.mockito.{IdiomaticMockito, Mockito}
import org.scalatest._

//noinspection NameBooleanParameters
class AdditionalValidationSpec
    extends WordSpecLike
    with Matchers
    with IOEitherValues
    with IOOptionValues
    with test.Resources
    with IdiomaticMockito
    with MacroBasedMatchers
    with TestHelper
    with BeforeAndAfter
    with Inspectors {

  private implicit val clock: Clock                     = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val elasticSearch: ElasticClient[IO] = mock[ElasticClient[IO]]
  private implicit val projectCache: ProjectCache[IO]   = mock[ProjectCache[IO]]
  private implicit val viewCache: ViewCache[IO]         = mock[ViewCache[IO]]

  before {
    Mockito.reset(elasticSearch)
    Mockito.reset(projectCache)
    Mockito.reset(viewCache)
  }

  "An AdditionalValidation" when {
    implicit val config: ElasticSearchConfig = ElasticSearchConfig("http://localhost", "kg", "doc", "default")
    val iri                                  = Iri.absolute("http://example.com/id").right.value
    val projectRef                           = ProjectRef(genUUID)
    val id                                   = Id(projectRef, iri)
    val user                                 = User("dmontero", "ldap")
    val matchingCaller: Caller =
      Caller(user, Set[Identity](user, User("dmontero2", "ldap"), Group("bbp-ou-neuroinformatics", "ldap2")))

    val label1 = ProjectLabel("account1", "project1")
    val label2 = ProjectLabel("account1", "project2")

    val labels = Set(label1, label2)
    val ref1   = ProjectRef(UUID.fromString("64b202b4-1060-42b5-9b4f-8d6a9d0d9113"))
    val ref2   = ProjectRef(UUID.fromString("d23d9578-255b-4e46-9e65-5c254bc9ad0a"))

    val path = Path(s"/${label1.organization}").right.value
    val acls =
      AccessControlLists(path -> resourceAcls(AccessControlList(user -> (View.query ++ View.write))))
    "applied to generic resources" should {

      "pass always" in {
        val validation = AdditionalValidation.pass[IO]
        val resource   = simpleV(id, Json.obj(), types = Set(nxv.Resource.value) + nxv.InProject)
        validation(id, Ref(unconstrainedSchemaUri), Set(nxv.Resource.value), resource.value, 1L).value.accepted shouldEqual resource.value
      }
    }

    "applied to resolvers" should {
      val schema       = Ref(resolverSchemaUri)
      val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
      val types        = Set[AbsoluteIri](nxv.CrossProject, nxv.Resolver)

      "fail when identities in acls are different from identities on resolver" in {
        val caller: Caller =
          Caller(User("dmontero2", "ldap"),
                 Set[Identity](Group("bbp-ou-neuroinformatics", "ldap2"), User("dmontero2", "ldap")))
        val validation = AdditionalValidation.resolver[IO](caller)
        val resource   = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value, 1L).value.rejected[InvalidIdentity]
      }

      "fail when the payload cannot be serialized" in {
        val caller: Caller = Caller.anonymous
        val validation     = AdditionalValidation.resolver[IO](caller)
        val resource       = simpleV(id, crossProject, types = Set(nxv.Resolver))
        validation(id, schema, Set(nxv.Resolver), resource.value, 1L).value.rejected[InvalidResourceFormat]
      }

      "fail when project not found in cache" in {
        val labels = Set(label2, label1)
        projectCache.getProjectRefs(labels) shouldReturn IO.pure(
          Map[ProjectLabel, Option[ProjectRef]](label1 -> None, label2 -> None))

        val validation = AdditionalValidation.resolver[IO](matchingCaller)
        val resource   = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value, 1L).value.rejected[ProjectsNotFound]
      }

      "pass when identities in acls are the same as the identities on resolver" in {
        val labels      = Set(label2, label1)
        val projectRef1 = ProjectRef(genUUID)
        val projectRef2 = ProjectRef(genUUID)
        projectCache.getProjectRefs(labels) shouldReturn IO.pure(
          Map(label1 -> Option(projectRef1), label2 -> Option(projectRef2)))
        val validation = AdditionalValidation.resolver[IO](matchingCaller)
        val resource   = simpleV(id, crossProject, types = types)
        val expected = jsonContentOf(
          "/resolve/cross-project-modified.json",
          Map(quote("{account1-project2-uuid}") -> projectRef2.id.toString,
              quote("{account1-project1-uuid}") -> projectRef1.id.toString)
        )
        validation(id, schema, types, resource.value, 1L).value.accepted.source should equalIgnoreArrayOrder(expected)
      }
    }

    "applied to views" should {
      val schema               = Ref(viewSchemaUri)
      val elasticSearchView    = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)
      val aggElasticSearchView = jsonContentOf("/view/aggelasticview.json").appendContextOf(viewCtx)
      val sparqlView           = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
      val types                = Set[AbsoluteIri](nxv.View, nxv.ElasticSearchView, nxv.Alpha)
      val mappings             = elasticSearchView.hcursor.get[String]("mapping").flatMap(parse).right.value
      def index(rev: Long)     = s"kg_${projectRef.id}_3aa14a1a-81e7-4147-8306-136d8270bb01_$rev"

      val es = ElasticSearchView(Json.obj(),
                                 Set.empty,
                                 Some("one"),
                                 false,
                                 true,
                                 projectRef,
                                 iri,
                                 UUID.fromString("3aa14a1a-81e7-4147-8306-136d8270bb01"),
                                 1L,
                                 false)

      "fail when the index throws an error for an ElasticSearchView on creation" in {
        val idx        = index(1L)
        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, elasticSearchView, types = types)
        elasticSearch.createIndex(idx, any[Json]) shouldReturn IO.raiseError(
          ElasticServerError(StatusCodes.BadRequest, "Error on creation..."))
        validation(id, schema, types, resource.value, 1L).value.failed[ElasticServerError]
      }

      "fail when the mappings are wrong for an ElasticSearchView" in {
        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, elasticSearchView, types = types)
        elasticSearch.createIndex(any[String], any[Json]) shouldReturn IO.pure(true)
        elasticSearch.updateMapping(any[String], any[String], any[Json]) shouldReturn IO.raiseError(
          ElasticServerError(StatusCodes.BadRequest, "Error on mappings..."))

        validation(id, schema, types, resource.value, 1L).value.failed[ElasticServerError]
      }

      "fail when the elasticSearch mappings cannot be applied because the index does not exists for an ElasticSearchView" in {
        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, elasticSearchView, types = types)
        val idx        = index(3L)
        elasticSearch.createIndex(idx, any[Json]) shouldReturn IO.pure(true)
        elasticSearch.updateMapping(idx, config.docType, mappings) shouldReturn IO.pure(false)
        validation(id, schema, types, resource.value, 3L).value.failed[KgError.InternalError]
      }

      "pass when the mappings are correct for an ElasticSearchView" in {
        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, elasticSearchView, types = types, rev = 2L)
        val idx        = index(2L)
        elasticSearch.createIndex(idx, any[Json]) shouldReturn IO.pure(true)
        elasticSearch.updateMapping(idx, config.docType, mappings) shouldReturn IO.pure(true)
        validation(id, schema, types, resource.value, 2L).value.accepted shouldEqual resource.value
      }

      "fail when project not found in cache for a AggregateElasticSearchView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticSearchView, nxv.Alpha)

        projectCache.getProjectRefs(labels) shouldReturn IO.pure(Map[ProjectLabel, Option[ProjectRef]](label1 -> None))

        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, aggElasticSearchView, types = types)
        validation(id, schema, types, resource.value, 1L).value.rejected[ProjectsNotFound]
      }

      "fail when view cannot be found on cache using AggregateElasticSearchView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticSearchView, nxv.Alpha)

        val id2 = url"http://example.com/id3"
        val id3 = url"http://example.com/other"
        projectCache.getProjectRefs(labels) shouldReturn IO.pure(Map(label1 -> Option(ref1), label2 -> Option(ref2)))
        viewCache.get(ref1) shouldReturn IO.pure(Set[View](es, es.copy(id = id3)))
        viewCache.get(ref2) shouldReturn IO.pure(Set[View](es.copy(id = id2), es.copy(id = id3)))

        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, aggElasticSearchView, types = types)
        validation(id, schema, types, resource.value, 1L).value.rejected[NotFound]
      }

      "fail no permissions found on project referenced on AggregateElasticSearchView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticSearchView, nxv.Alpha)

        val aclsWrongPerms =
          List(
            AccessControlLists(
              path -> resourceAcls(AccessControlList(user -> Set(Permission.unsafe("schemas/manage"))))),
            AccessControlLists(
              path -> resourceAcls(AccessControlList(Anonymous -> Set(Permission.unsafe("views/manage"))))),
            AccessControlLists(
              "other" / "project" -> resourceAcls(AccessControlList(user -> Set(Permission.unsafe("views/manage")))))
          )

        val resource = simpleV(id, aggElasticSearchView, types = types)
        forAll(aclsWrongPerms) { a =>
          val validation = AdditionalValidation.view[IO](matchingCaller, a)
          validation(id, schema, types, resource.value, 1L).value
            .rejected[ProjectsNotFound] shouldEqual ProjectsNotFound(Set(label1, label2))
        }
      }

      "pass when correct AggregateElasticSearchView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticSearchView, nxv.Alpha)

        val id1 = url"http://example.com/id2"
        val id2 = url"http://example.com/id3"
        val id3 = url"http://example.com/other"
        projectCache.getProjectRefs(labels) shouldReturn IO.pure(Map(label1 -> Option(ref1), label2 -> Option(ref2)))
        viewCache.get(ref1) shouldReturn IO.pure(Set[View](es.copy(id = id1), es.copy(id = id3)))
        viewCache.get(ref2) shouldReturn IO.pure(Set[View](es.copy(id = id2), es.copy(id = id3)))

        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val resource   = simpleV(id, aggElasticSearchView, types = types)
        val expected   = jsonContentOf("/view/aggelasticviewrefs.json").addContext(viewCtxUri)
        val result     = validation(id, schema, types, resource.value, 1L).value.accepted
        result.ctx shouldEqual resource.value.ctx
        result.source should equalIgnoreArrayOrder(expected)
      }

      "pass when it is an SparqlView" in {
        val validation = AdditionalValidation.view[IO](matchingCaller, acls)
        val types      = Set[AbsoluteIri](nxv.SparqlView.value, nxv.View, nxv.Alpha)
        val resource   = simpleV(id, sparqlView, types = types)

        validation(id, schema, types, resource.value, 1L).value.accepted shouldEqual resource.value
      }
    }
  }

}
