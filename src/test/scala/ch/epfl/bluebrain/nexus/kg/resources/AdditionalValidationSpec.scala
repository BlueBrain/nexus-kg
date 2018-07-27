package ch.epfl.bluebrain.nexus.kg.resources

import java.time.{Clock, Instant, ZoneId}

import cats.{Id => CId}
import ch.epfl.bluebrain.nexus.commons.test
import ch.epfl.bluebrain.nexus.iam.client.types.Address._
import ch.epfl.bluebrain.nexus.iam.client.types.Identity.{Anonymous, GroupRef, UserRef}
import ch.epfl.bluebrain.nexus.iam.client.types.Permission._
import ch.epfl.bluebrain.nexus.iam.client.types.{FullAccessControlList, Permissions}
import ch.epfl.bluebrain.nexus.kg.TestHelper
import ch.epfl.bluebrain.nexus.kg.config.Contexts.resolverCtx
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary._
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload, ProjectNotFound}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class AdditionalValidationSpec
    extends WordSpecLike
    with Matchers
    with test.Resources
    with EitherValues
    with TestHelper {

  private implicit val clock: Clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "An AdditionalValidation" should {

    val crossProject                                        = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val iri                                                 = Iri.absolute("http://example.com/id").right.value
    val projectRef                                          = ProjectRef("ref")
    val id                                                  = Id(projectRef, iri)
    val schema                                              = Ref(resolverSchemaUri)
    val perms                                               = Permissions(Read, Write)
    val accountRef                                          = AccountRef("accountRef")
    val labelResol: ProjectLabel => CId[Option[ProjectRef]] = lb => Some(ProjectRef(s"${lb.account}-${lb.value}-uuid"))

    val matchingAcls: Option[FullAccessControlList] = Some(
      FullAccessControlList((UserRef("ldap", "dmontero"), "a" / "b", perms),
                            (UserRef("ldap", "dmontero2"), "a" / "b", perms),
                            (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))
    val crossProjectTypes = Set[AbsoluteIri](nxv.CrossProject, nxv.Resolver)

    "pass always" in {
      val validation = AdditionalValidation.pass[CId]
      val resource   = simpleV(id, crossProject, types = crossProjectTypes + nxv.InProject)
      validation(id, schema, crossProjectTypes, resource.value).value.right.value shouldEqual resource.value
    }

    "fail when identities in acls are different from identities on resolver" in {
      val acls: Option[FullAccessControlList] = Some(
        FullAccessControlList((UserRef("ldap", "dmontero2"), "a" / "b", perms),
                              (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))
      val validation = AdditionalValidation.resolver[CId](acls, accountRef, labelResol)
      val resource   = simpleV(id, crossProject, types = crossProjectTypes)
      validation(id, schema, crossProjectTypes, resource.value).value.left.value shouldBe a[InvalidIdentity]
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
      val resource            = simpleV(id, crossProjectNoLabel, types = crossProjectTypes)
      validation(id, schema, crossProjectTypes, resource.value).value.left.value shouldBe a[InvalidPayload]
    }

    "fail when project not found in cache" in {
      val notFoundResol: ProjectLabel => CId[Option[ProjectRef]] = _ => None
      val validation                                             = AdditionalValidation.resolver[CId](matchingAcls, accountRef, notFoundResol)
      val resource                                               = simpleV(id, crossProject, types = crossProjectTypes)
      validation(id, schema, crossProjectTypes, resource.value).value.left.value shouldBe a[ProjectNotFound]

    }

    "pass when identities in acls are the same as the identities on resolver" in {
      val validation = AdditionalValidation.resolver[CId](matchingAcls, accountRef, labelResol)
      val resource   = simpleV(id, crossProject, types = crossProjectTypes)
      val expected   = jsonContentOf("/resolve/cross-project-modified.json")
      validation(id, schema, crossProjectTypes, resource.value).value.right.value.source should equalIgnoreArrayOrder(
        expected)
    }
  }

}
