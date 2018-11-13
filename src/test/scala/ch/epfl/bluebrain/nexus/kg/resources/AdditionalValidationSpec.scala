package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.StatusCodes
import cats.MonadError
import cats.data.EitherT
import cats.instances.try_
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticServerError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.Caller.{AnonymousCaller, AuthenticatedCaller}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.indexing.View
import ch.epfl.bluebrain.nexus.kg.indexing.View.ElasticView
import ch.epfl.bluebrain.nexus.kg.resources.Rejection._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.Json
import io.circe.parser.parse
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest._
import org.scalatest.mockito.MockitoSugar
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

import scala.util.Try

class AdditionalValidationSpec
    extends WordSpecLike
    with Matchers
    with test.Resources
    with EitherValues
    with TestHelper
    with MockitoSugar
    with TryValues
    with BeforeAndAfter
    with Inspectors {

  private implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val elastic      = mock[ElasticClient[Try]]
  private implicit val cache        = mock[DistributedCache[Try]]

  before {
    Mockito.reset(elastic)
    Mockito.reset(cache)
  }

  "An AdditionalValidation" when {
    implicit val config: ElasticConfig         = ElasticConfig("http://localhost", "kg", "doc", "default")
    val iri                                    = Iri.absolute("http://example.com/id").right.value
    val projectRef                             = ProjectRef("ref")
    val id                                     = Id(projectRef, iri)
    val accountRef                             = AccountRef("accountRef")
    implicit val F: MonadError[Try, Throwable] = try_.catsStdInstancesForTry
    val user                                   = UserRef("ldap", "dmontero")
    val matchingCaller: Caller = AuthenticatedCaller(
      AuthToken("some"),
      user,
      Set[Identity](user, UserRef("ldap", "dmontero2"), GroupRef("ldap2", "bbp-ou-neuroinformatics"))
    )

    val label1 = ProjectLabel("account1", "project1")
    val label2 = ProjectLabel("account1", "project2")
    val acls   = FullAccessControlList((user: Identity, Address(label1.account), Permissions(Permission("views/manage"))))

    val labels = Set(label1, label2)
    val ref1   = ProjectRef("64b202b4-1060-42b5-9b4f-8d6a9d0d9113")
    val ref2   = ProjectRef("d23d9578-255b-4e46-9e65-5c254bc9ad0a")

    "applied to generic resources" should {

      "pass always" in {
        val validation = AdditionalValidation.pass[Try]
        val resource   = simpleV(id, Json.obj(), types = Set(nxv.Resource.value) + nxv.InProject)
        validation(id, Ref(resourceSchemaUri), Set(nxv.Resource.value), resource.value, 1L).value.success.value.right.value shouldEqual resource.value
      }
    }

    "applied to resolvers" should {
      val schema       = Ref(resolverSchemaUri)
      val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
      val types        = Set[AbsoluteIri](nxv.CrossProject, nxv.Resolver)

      "fail when identities in acls are different from identities on resolver" in {
        val caller: Caller =
          AuthenticatedCaller(AuthToken("sone"),
                              UserRef("ldap", "dmontero2"),
                              Set[Identity](GroupRef("ldap2", "bbp-ou-neuroinformatics"), UserRef("ldap", "dmontero2")))
        val validation = AdditionalValidation.resolver[Try](caller, accountRef)
        val resource   = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[InvalidIdentity]
      }

      "fail when the payload cannot be serialized" in {
        val caller: Caller = AnonymousCaller
        val validation     = AdditionalValidation.resolver[Try](caller, accountRef)
        val resource       = simpleV(id, crossProject, types = Set(nxv.Resolver))
        validation(id, schema, Set(nxv.Resolver), resource.value, 1L).value.success.value.left.value shouldBe a[
          InvalidPayload]
      }

      "fail when project not found in cache" in {
        val labels = Set(label2, label1)
        when(cache.projectRefs(labels))
          .thenReturn(EitherT.leftT[Try, Map[ProjectLabel, ProjectRef]](ProjectsNotFound(labels): Rejection))

        val validation = AdditionalValidation.resolver[Try](matchingCaller, accountRef)
        val resource   = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[ProjectsNotFound]
      }

      "pass when identities in acls are the same as the identities on resolver" in {
        val labels = Set(label2, label1)
        when(cache.projectRefs(labels))
          .thenReturn(EitherT.rightT[Try, Rejection](
            Map(label1 -> ProjectRef("account1-project1-uuid"), label2 -> ProjectRef("account1-project2-uuid"))))
        val validation = AdditionalValidation.resolver[Try](matchingCaller, accountRef)
        val resource   = simpleV(id, crossProject, types = types)
        val expected   = jsonContentOf("/resolve/cross-project-modified.json")
        validation(id, schema, types, resource.value, 1L).value.success.value.right.value.source should equalIgnoreArrayOrder(
          expected)
      }
    }

    "applied to views" should {
      val schema           = Ref(viewSchemaUri)
      val elasticView      = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)
      val aggElasticView   = jsonContentOf("/view/aggelasticview.json").appendContextOf(viewCtx)
      val sparqlView       = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
      val types            = Set[AbsoluteIri](nxv.View, nxv.ElasticView, nxv.Alpha)
      val mappings         = elasticView.hcursor.get[String]("mapping").flatMap(parse).right.value
      def index(rev: Long) = s"kg_${projectRef.id}_3aa14a1a-81e7-4147-8306-136d8270bb01_$rev"

      val es = ElasticView(Json.obj(),
                           Set.empty,
                           Some("one"),
                           false,
                           true,
                           projectRef,
                           iri,
                           "3aa14a1a-81e7-4147-8306-136d8270bb01",
                           1L,
                           false)

      "fail when the index throws an error for an ElasticView on creation" in {
        val idx        = index(1L)
        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, elasticView, types = types)
        when(elastic.createIndex(idx))
          .thenReturn(F.raiseError(ElasticServerError(StatusCodes.BadRequest, "Error on creation...")))
        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[InvalidPayload]
      }

      "fail when the mappings are wrong for an ElasticView" in {
        val idx        = index(1L)
        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, elasticView, types = types)
        when(elastic.createIndex(idx)).thenReturn(Try(true))
        when(elastic.updateMapping(idx, config.docType, mappings))
          .thenReturn(F.raiseError(ElasticServerError(StatusCodes.BadRequest, "Error on mappings...")))

        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[InvalidPayload]
      }

      "fail when the elasticSearch mappings cannot be applied because the index does not exists for an ElasticView" in {
        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, elasticView, types = types)
        val idx        = index(3L)
        when(elastic.createIndex(idx)).thenReturn(Try(true))
        when(elastic.updateMapping(idx, config.docType, mappings)).thenReturn(Try(false))
        validation(id, schema, types, resource.value, 3L).value.success.value.left.value shouldBe a[Unexpected]
      }

      "pass when the mappings are correct for an ElasticView" in {
        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, elasticView, types = types, rev = 2L)
        val idx        = index(2L)
        when(elastic.createIndex(idx)).thenReturn(Try(true))
        when(elastic.updateMapping(idx, config.docType, mappings)).thenReturn(Try(true))
        validation(id, schema, types, resource.value, 2L).value.success.value.right.value shouldEqual resource.value
      }

      "fail when project not found in cache for a AggregateElasticView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticView, nxv.Alpha)

        when(cache.projectRefs(labels)(F))
          .thenReturn(EitherT.leftT[Try, Map[ProjectLabel, ProjectRef]](ProjectsNotFound(labels): Rejection))

        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, aggElasticView, types = types)
        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[ProjectsNotFound]
      }

      "fail when view cannot be found on cache using AggregateElasticView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticView, nxv.Alpha)

        val id2 = url"http://example.com/id3"
        val id3 = url"http://example.com/other"
        when(cache.projectRefs(labels)(F))
          .thenReturn(EitherT.rightT[Try, Rejection](Map(label1 -> ref1, label2 -> ref2)))
        when(cache.views(ref1)).thenReturn(Try(Set[View](es, es.copy(id = id3))))
        when(cache.views(ref2)).thenReturn(Try(Set[View](es.copy(id = id2), es.copy(id = id3))))

        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, aggElasticView, types = types)
        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[NotFound]
      }

      "fail no permissions found on project referenced on AggregateElasticView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticView, nxv.Alpha)

        val aclsWrongPerms = List(
          FullAccessControlList((user: Identity, Address(label1.account), Permissions(Permission("schemas/manage")))),
          FullAccessControlList(
            (Anonymous: Identity, Address(label1.account), Permissions(Permission("views/manage")))),
          FullAccessControlList((user: Identity, Address("other/project"), Permissions(Permission("views/manage"))))
        )

        val resource = simpleV(id, aggElasticView, types = types)
        forAll(aclsWrongPerms) { a =>
          val validation = AdditionalValidation.view[Try](matchingCaller, a)
          validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldEqual ProjectsNotFound(
            Set(label1, label2))
        }
      }

      "pass when correct AggregateElasticView" in {
        val types = Set[AbsoluteIri](nxv.View, nxv.AggregateElasticView, nxv.Alpha)

        val id1 = url"http://example.com/id2"
        val id2 = url"http://example.com/id3"
        val id3 = url"http://example.com/other"
        when(cache.projectRefs(labels)(F))
          .thenReturn(EitherT.rightT[Try, Rejection](Map(label1 -> ref1, label2 -> ref2)))
        when(cache.views(ref1)).thenReturn(Try(Set[View](es.copy(id = id1), es.copy(id = id3))))
        when(cache.views(ref2)).thenReturn(Try(Set[View](es.copy(id = id2), es.copy(id = id3))))

        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val resource   = simpleV(id, aggElasticView, types = types)
        val expected   = jsonContentOf("/view/aggelasticviewrefs.json").addContext(viewCtxUri)
        val result     = validation(id, schema, types, resource.value, 1L).value.success.value.right.value
        result.ctx shouldEqual resource.value.ctx
        result.source should equalIgnoreArrayOrder(expected)
      }

      "pass when it is an SparqlView" in {
        val validation = AdditionalValidation.view[Try](matchingCaller, acls)
        val types      = Set[AbsoluteIri](nxv.SparqlView.value, nxv.View, nxv.Alpha)
        val resource   = simpleV(id, sparqlView, types = types)

        validation(id, schema, types, resource.value, 1L).value.success.value.right.value shouldEqual resource.value
      }
    }
  }

}
