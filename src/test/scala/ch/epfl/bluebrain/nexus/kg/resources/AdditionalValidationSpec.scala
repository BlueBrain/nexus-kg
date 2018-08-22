package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import akka.http.scaladsl.model.StatusCodes
import cats.instances.try_._
import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticFailure.ElasticServerError
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.iam.client.types.Address._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types.Permission._
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Permissions}
import ch.epfl.bluebrain.nexus.kg.TestHelper
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
import org.mockito.ArgumentMatchers.{eq => mEq}
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatest._

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

    val iri                                                 = Iri.absolute("http://example.com/id").right.value
    val projectRef                                          = ProjectRef("ref")
    val id                                                  = Id(projectRef, iri)
    val perms                                               = Permissions(Read, Write)
    val accountRef                                          = AccountRef("accountRef")
    val labelResol: ProjectLabel => CId[Option[ProjectRef]] = lb => Some(ProjectRef(s"${lb.account}-${lb.value}-uuid"))

    val matchingAcls: Option[FullAccessControlList] = Some(
      FullAccessControlList((UserRef("ldap", "dmontero"), "a" / "b", perms),
                            (UserRef("ldap", "dmontero2"), "a" / "b", perms),
                            (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))

    "applied to generic resources" should {

      "pass always" in {
        val validation = AdditionalValidation.pass[CId]
        val resource   = simpleV(id, Json.obj(), types = Set(nxv.Resource.value) + nxv.InProject)
        validation(id, Ref(resourceSchemaUri), Set(nxv.Resource.value), resource.value).value.right.value shouldEqual resource.value
      }
    }

    "applied to resolvers" should {
      val schema       = Ref(resolverSchemaUri)
      val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
      val types        = Set[AbsoluteIri](nxv.CrossProject, nxv.Resolver)

      "fail when identities in acls are different from identities on resolver" in {
        val acls: Option[FullAccessControlList] = Some(
          FullAccessControlList((UserRef("ldap", "dmontero2"), "a" / "b", perms),
                                (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))
        val validation = AdditionalValidation.resolver[CId](acls, accountRef, labelResol)
        val resource   = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value).value.left.value shouldBe a[InvalidIdentity]
      }

      "fail when the payload cannot be serialized" in {
        val acls: Option[FullAccessControlList] = Some(FullAccessControlList((Anonymous, "a" / "b", perms)))
        val validation                          = AdditionalValidation.resolver[CId](acls, accountRef, labelResol)
        val resource                            = simpleV(id, crossProject, types = Set(nxv.Resolver))
        validation(id, schema, Set(nxv.Resolver), resource.value).value.left.value shouldBe a[InvalidPayload]
      }

      "fail when projects cannot be converted to account and project label" in {
        val crossProjectNoLabel = jsonContentOf("/resolve/cross-project-no-label.json").appendContextOf(resolverCtx)
        val validation          = AdditionalValidation.resolver[CId](matchingAcls, accountRef, labelResol)
        val resource            = simpleV(id, crossProjectNoLabel, types = types)
        validation(id, schema, types, resource.value).value.left.value shouldBe a[InvalidPayload]
      }

      "fail when project not found in cache" in {
        val notFoundResol: ProjectLabel => CId[Option[ProjectRef]] = _ => None
        val validation                                             = AdditionalValidation.resolver[CId](matchingAcls, accountRef, notFoundResol)
        val resource                                               = simpleV(id, crossProject, types = types)
        validation(id, schema, types, resource.value).value.left.value shouldBe a[ProjectNotFound]

      }

      "pass when identities in acls are the same as the identities on resolver" in {
        val validation = AdditionalValidation.resolver[CId](matchingAcls, accountRef, labelResol)
        val resource   = simpleV(id, crossProject, types = types)
        val expected   = jsonContentOf("/resolve/cross-project-modified.json")
        validation(id, schema, types, resource.value).value.right.value.source should equalIgnoreArrayOrder(expected)
      }
    }

    "applied to views" should {
      val schema   = Ref(viewSchemaUri)
      val view     = jsonContentOf("/view/elasticview.json").appendContextOf(viewCtx)
      val types    = Set[AbsoluteIri](nxv.View, nxv.ElasticView, nxv.Alpha)
      val mappings = view.hcursor.get[String]("mapping").flatMap(parse).right.value
      val F        = catsStdInstancesForTry

      "fail when the mappings are wrong for an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, view, types = types)
        when(elastic.createIndex(isA[String], mEq(mappings)))
          .thenReturn(F.raiseError(new ElasticServerError(StatusCodes.BadRequest, "Failed to parse mapping...")))

        validation(id, schema, types, resource.value).value.success.value.left.value shouldBe a[InvalidPayload]
      }

      "fail when the creation of the elasticsearch index throws an unexpected error for an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, view, types = types)
        when(elastic.createIndex(isA[String], mEq(mappings)))
          .thenReturn(F.raiseError(new RuntimeException("Failed to parse mapping...")))

        validation(id, schema, types, resource.value).value.success.value.left.value shouldBe a[Unexpected]
      }

      "fail when the elasticSearch index already exists for an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, view, types = types)
        when(elastic.createIndex(isA[String], mEq(mappings))).thenReturn(Try(false))

        validation(id, schema, types, resource.value).value.success.value.left.value shouldBe a[Unexpected]
      }

      "pass when the mappings are correct for an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, view, types = types)
        when(elastic.createIndex(isA[String], mEq(mappings))).thenReturn(Try(true))
        when(elastic.deleteIndex(isA[String])).thenReturn(Try(true))

        validation(id, schema, types, resource.value).value.success.value.right.value shouldEqual resource.value
      }

      "pass when it is not an ElasticView" in {
        val validation = AdditionalValidation.view[Try]
        val resource   = simpleV(id, view, types = Set(nxv.SparqlView.value))

        validation(id, schema, Set(nxv.SparqlView.value), resource.value).value.success.value.right.value shouldEqual resource.value
      }
    }
  }

}
