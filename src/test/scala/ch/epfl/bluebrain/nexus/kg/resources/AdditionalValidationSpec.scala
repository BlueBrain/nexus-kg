package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.StatusCodes
import cats.instances.try_._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticServerError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.Caller.{AnonymousCaller, AuthenticatedCaller}
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types.{AuthToken, Identity}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.AppConfig.ElasticConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
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

import scala.util.Try

class AdditionalValidationSpec
    extends WordSpecLike
    with Matchers
    with test.Resources
    with EitherValues
    with TestHelper
    with MockitoSugar
    with TryValues
    with BeforeAndAfter {

  private implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())
  private implicit val elastic      = mock[ElasticClient[Try]]

  before {
    Mockito.reset(elastic)
  }

  "An AdditionalValidation" when {
    implicit val config: ElasticConfig                      = ElasticConfig("http://localhost", "kg", "doc", "default")
    val iri                                                 = Iri.absolute("http://example.com/id").right.value
    val projectRef                                          = ProjectRef("ref")
    val id                                                  = Id(projectRef, iri)
    val accountRef                                          = AccountRef("accountRef")
    val labelResol: ProjectLabel => CId[Option[ProjectRef]] = lb => Some(ProjectRef(s"${lb.account}-${lb.value}-uuid"))
    val matchingCaller: Caller = AuthenticatedCaller(
      AuthToken("some"),
      UserRef("ldap", "dmontero"),
      Set[Identity](UserRef("ldap", "dmontero"),
                    UserRef("ldap", "dmontero2"),
                    GroupRef("ldap2", "bbp-ou-neuroinformatics"))
    )

    "applied to generic resources" should {

      "pass always" in {
        val validation = AdditionalValidation.pass[CId]
        val resource   = simpleV(id, Json.obj(), types = Set(nxv.Resource.value) + nxv.InProject)
        validation(id, Ref(resourceSchemaUri), Set(nxv.Resource.value), resource.value, 1L).value.right.value shouldEqual resource.value
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
        val validation = AdditionalValidation.resolver[CId](caller, accountRef, labelResol)
        val resource   = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value, 1L).value.left.value shouldBe a[InvalidIdentity]
      }

      "fail when the payload cannot be serialized" in {
        val caller: Caller = AnonymousCaller
        val validation     = AdditionalValidation.resolver[CId](caller, accountRef, labelResol)
        val resource       = simpleV(id, crossProject, types = Set(nxv.Resolver))
        validation(id, schema, Set(nxv.Resolver), resource.value, 1L).value.left.value shouldBe a[InvalidPayload]
      }

      "fail when projects cannot be converted to account and project label" in {
        val crossProjectNoLabel = jsonContentOf("/resolve/cross-project-no-label.json").appendContextOf(resolverCtx)
        val validation          = AdditionalValidation.resolver[CId](matchingCaller, accountRef, labelResol)
        val resource            = simpleV(id, crossProjectNoLabel, types = types)
        validation(id, schema, types, resource.value, 1L).value.left.value shouldBe a[InvalidPayload]
      }

      "fail when project not found in cache" in {
        val notFoundResol: ProjectLabel => CId[Option[ProjectRef]] = _ => None
        val validation                                             = AdditionalValidation.resolver[CId](matchingCaller, accountRef, notFoundResol)
        val resource                                               = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value, 1L).value.left.value shouldBe a[ProjectNotFound]

      }

      "pass when identities in acls are the same as the identities on resolver" in {
        val validation = AdditionalValidation.resolver[CId](matchingCaller, accountRef, labelResol)
        val resource   = simpleV(id, crossProject, types = types)
        val expected   = jsonContentOf("/resolve/cross-project-modified.json")
        validation(id, schema, types, resource.value, 1L).value.right.value.source should equalIgnoreArrayOrder(
          expected)
      }
    }

    "applied to views" should {
      val schema           = Ref(viewSchemaUri)
      val elasticView      = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)
      val sparqlView       = jsonContentOf("/view/sparqlview.json").appendContextOf(viewCtx)
      val types            = Set[AbsoluteIri](nxv.View, nxv.ElasticView, nxv.Alpha)
      val mappings         = elasticView.hcursor.get[String]("mapping").flatMap(parse).right.value
      val F                = catsStdInstancesForTry
      def index(rev: Long) = s"kg_${projectRef.id}_3aa14a1a-81e7-4147-8306-136d8270bb01_$rev"

      "fail when the index cerating throws an error for an ElasticView" in {
        val idx        = index(1L)
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, elasticView, types = types)
        when(elastic.createIndex(idx))
          .thenReturn(F.raiseError(ElasticServerError(StatusCodes.BadRequest, "Error on creation...")))
        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[InvalidPayload]
      }

      "fail when the mappings are wrong for an ElasticView" in {
        val idx        = index(1L)
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, elasticView, types = types)
        when(elastic.createIndex(idx)).thenReturn(Try(true))
        when(elastic.updateMapping(idx, config.docType, mappings))
          .thenReturn(F.raiseError(ElasticServerError(StatusCodes.BadRequest, "Error on mappings...")))

        validation(id, schema, types, resource.value, 1L).value.success.value.left.value shouldBe a[InvalidPayload]
      }

      "fail when the elasticSearch mappings cannot be applied because the index does not exists for an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, elasticView, types = types)
        val idx        = index(3L)
        when(elastic.createIndex(idx)).thenReturn(Try(true))
        when(elastic.updateMapping(idx, config.docType, mappings)).thenReturn(Try(false))
        validation(id, schema, types, resource.value, 3L).value.success.value.left.value shouldBe a[Unexpected]
      }

      "pass when the mappings are correct for an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, elasticView, types = types, rev = 2L)
        val idx        = index(2L)
        when(elastic.createIndex(idx)).thenReturn(Try(true))
        when(elastic.updateMapping(idx, config.docType, mappings)).thenReturn(Try(true))
        validation(id, schema, types, resource.value, 2L).value.success.value.right.value shouldEqual resource.value
      }

      "pass when it is an SparqlView" in {
        val validation = AdditionalValidation.view[Try]
        val types      = Set[AbsoluteIri](nxv.SparqlView.value, nxv.View, nxv.Alpha)
        val resource   = simpleV(id, sparqlView, types = types)

        validation(id, schema, types, resource.value, 1L).value.success.value.right.value shouldEqual resource.value
      }
    }
  }

}
