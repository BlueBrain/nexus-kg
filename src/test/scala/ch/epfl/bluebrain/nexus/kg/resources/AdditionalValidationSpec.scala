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
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver
import ch.epfl.bluebrain.nexus.kg.resolve.Resolver.{CrossProjectResolver, InAccountResolver, InProjectResolver}
import ch.epfl.bluebrain.nexus.kg.resources.Rejection.{AlreadyExistsType, InvalidIdentity, InvalidPayload}
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import org.scalatest.{EitherValues, Inspectors, Matchers, WordSpecLike}

class AdditionalValidationSpec
    extends WordSpecLike
    with Matchers
    with test.Resources
    with EitherValues
    with TestHelper
    with Inspectors {

  private implicit val clock = Clock.fixed(Instant.ofEpochSecond(3600), ZoneId.systemDefault())

  "An AdditionalValidation" when {
    val crossProject = jsonContentOf("/resolve/cross-project.json").appendContextOf(resolverCtx)
    val inAccount    = jsonContentOf("/resolve/in-account.json").appendContextOf(resolverCtx)
    val inProject    = jsonContentOf("/resolve/in-project.json").appendContextOf(resolverCtx)
    val iri          = Iri.absolute("http://example.com/id").right.value
    val projectRef   = ProjectRef("ref")
    val id           = Id(projectRef, iri)
    val schema       = Ref(resolverSchemaUri)

    "dealing with regular resources" should {
      "pass always" in {
        val validation = AdditionalValidation.pass[CId]
        val resource   = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.InProject, nxv.Resource))
        validation(id, schema, Set(nxv.CrossProject, nxv.Resolver), resource.value).value.right.value shouldEqual (())
      }
    }

    "dealing with resolvers" should {
      val perms                                            = Permissions(Read, Write)
      val accountRef                                       = AccountRef("accountRef")
      val emptyResolvers: ProjectRef => CId[Set[Resolver]] = _ => Set.empty
      val passedAcls: Option[FullAccessControlList] = Some(
        FullAccessControlList((UserRef("ldap", "dmontero"), "a" / "b", perms),
                              (UserRef("ldap", "dmontero2"), "a" / "b", perms),
                              (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))

      "pass when having correct payload and identities in acls are the same as the identities on resolver" in {
        val validation = AdditionalValidation.resolver[CId](passedAcls, accountRef, emptyResolvers)
        val list = List(crossProject -> nxv.CrossProject.value,
                        inProject -> nxv.InProject.value,
                        inAccount -> nxv.InAccount.value)
        forAll(list) {
          case (json, tpe) =>
            val resource = simpleV(id, json, types = Set(nxv.Resolver, tpe))
            validation(id, schema, Set(nxv.Resolver, tpe), resource.value).value.right.value shouldEqual (())
        }
      }

      "pass when the CrossProjectResolver already exists" in {
        val resolvers: ProjectRef => CId[Set[Resolver]] =
          ref => Set(CrossProjectResolver(Set.empty, Set.empty, List.empty, ref, iri, 1L, false, 10))
        val validation = AdditionalValidation.resolver[CId](passedAcls, accountRef, resolvers)
        val resource   = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        validation(id, schema, Set(nxv.Resolver, nxv.CrossProject), resource.value).value.right.value shouldEqual (())
      }

      "fail when identities in acls are different from identities on resolver" in {
        val acls: Option[FullAccessControlList] = Some(
          FullAccessControlList((UserRef("ldap", "dmontero2"), "a" / "b", perms),
                                (GroupRef("ldap2", "bbp-ou-neuroinformatics"), "c" / "d", perms)))
        val validation = AdditionalValidation.resolver[CId](acls, accountRef, emptyResolvers)
        val resource   = simpleV(id, crossProject, types = Set(nxv.Resolver, nxv.CrossProject))
        validation(id, schema, Set(nxv.CrossProject, nxv.Resolver), resource.value).value.left.value shouldBe a[
          InvalidIdentity]
      }

      "fail when the payload cannot be serialized" in {
        val acls: Option[FullAccessControlList] = Some(FullAccessControlList((Anonymous, "a" / "b", perms)))
        val validation                          = AdditionalValidation.resolver[CId](acls, accountRef, emptyResolvers)
        val resource                            = simpleV(id, crossProject, types = Set(nxv.Resolver))
        validation(id, schema, Set(nxv.Resolver), resource.value).value.left.value shouldBe a[InvalidPayload]
      }

      "fail when the InAccountResolver already exists" in {
        val resolvers: ProjectRef => CId[Set[Resolver]] =
          ref => Set(InAccountResolver(Set.empty, List.empty, accountRef, ref, iri, 1L, false, 10))
        val validation = AdditionalValidation.resolver[CId](passedAcls, accountRef, resolvers)
        val resource   = simpleV(id, inAccount, types = Set(nxv.Resolver, nxv.InAccount))
        validation(id, schema, Set(nxv.Resolver, nxv.InAccount), resource.value).value.left.value shouldBe a[
          AlreadyExistsType]
      }

      "fail when the InProjectResolver already exists" in {
        val resolvers: ProjectRef => CId[Set[Resolver]] = ref => Set(InProjectResolver(ref, iri, 1L, false, 10))
        val validation                                  = AdditionalValidation.resolver[CId](passedAcls, accountRef, resolvers)
        val resource                                    = simpleV(id, inProject, types = Set(nxv.Resolver, nxv.InProject))
        validation(id, schema, Set(nxv.Resolver, nxv.InProject), resource.value).value.left.value shouldBe a[
          AlreadyExistsType]
      }
    }
  }
}
