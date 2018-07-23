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
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{InvalidIdentity, InvalidPayload}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.scalatest.{EitherValues, Matchers, WordSpecLike}

class AdditionalValidationSpec
    extends WordSpecLike
    with Matchers
    with test.Resources
    with EitherValues
    with TestHelper {

  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "An AdditionalValidation" should {

    val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val iri          = Iri.absolute("http://example.com/id").right.value
    val projectRef   = ProjectRef("ref")
    val id           = Id(projectRef, iri)
    val schema       = Ref(crossResolverSchemaUri)
    val perms        = Permissions(Read, Write)

    "pass always" in {
      val validation = AdditionalValidation.pass[CId]
      val resource   = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.InProject, nxv.Resource))
      validation(id, schema, Set(nxv.CrossProject, nxv.Resolver), resource.value).value.right.value shouldEqual (())
    }

    "fail when identities in acls are different from identities on resolver" in {
      val acls: Option[FullAccessControlList] = Some(
        FullAccessControlList((UserRef("ldap", "dmontero2"), "a" / "b", perms),
                              (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))
      val validation = AdditionalValidation.resolver[CId](acls)
      val resource   = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
      validation(id, schema, Set(nxv.CrossProject, nxv.Resolver), resource.value).value.left.value shouldBe a[
        InvalidIdentity]
    }

    "fail when the payload cannot be serialized" in {
      val acls: Option[FullAccessControlList] = Some(FullAccessControlList((Anonymous, "a" / "b", perms)))
      val validation                          = AdditionalValidation.resolver[CId](acls)
      val resource                            = simpleV(id, crossProject, types = Set(nxv.Resolver))
      validation(id, schema, Set(nxv.Resolver), resource.value).value.left.value shouldBe a[InvalidPayload]
    }

    "pass when identities in acls are the same as the identities on resolver" in {
      val acls: Option[FullAccessControlList] = Some(
        FullAccessControlList((UserRef("ldap", "dmontero"), "a" / "b", perms),
                              (UserRef("ldap", "dmontero2"), "a" / "b", perms),
                              (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))
      val validation = AdditionalValidation.resolver[CId](acls)
      val resource   = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
      validation(id, schema, Set(nxv.Resolver, nxv.CrossProject), resource.value).value.right.value shouldEqual (())
    }
  }

}
