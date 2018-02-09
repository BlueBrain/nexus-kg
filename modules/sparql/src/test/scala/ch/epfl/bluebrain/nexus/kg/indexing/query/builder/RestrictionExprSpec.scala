package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.{Own, Read, Write}
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity.GroupRef
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr.{ComparisonExpr, LogicalExpr, NoopExpr}
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import org.scalatest.{Matchers, WordSpecLike}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.UriTerm
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op._
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._

class RestrictionExprSpec extends WordSpecLike with Matchers {

  private val base                                                     = s"http://localhost/v0"
  private val nexusVocBaseDomains                                      = s"$base/voc/nexus/core"
  private implicit val domainsQualifier: ConfiguredQualifier[DomainId] = Qualifier.configured[DomainId](base)
  private implicit val orgsQualifier: ConfiguredQualifier[OrgId]       = Qualifier.configured[OrgId](base)
  private implicit val StringQualifier: ConfiguredQualifier[String]    = Qualifier.configured[String](nexusVocBaseDomains)

  "A RestrictionExpr" should {

    "create noopExpr expression when root ACL present" in {
      val aclAuth = FullAccessControlList(
        (GroupRef("BBP", "group1"), Path("/kg/org"), Permissions(Own, Read, Write)),
        (GroupRef("BBP", "group3"), Path("/kg"), Permissions(Own, Read, Write)),
        (GroupRef("BBP", "group1"), Path("/kg/org/dom1/a/b/c"), Permissions(Own, Read, Write))
      )
      RestrictionExpr(aclAuth) shouldEqual NoopExpr
    }

    "create org expression when org ACL present" in {
      val aclAuth = FullAccessControlList(
        (GroupRef("BBP", "group1"), Path("/kg"), Permissions(Write)),
        (GroupRef("BBP", "group1"), Path("/kg/org/dom"), Permissions(Own)),
        (GroupRef("BBP", "group1"), Path("/kg/org"), Permissions(Own, Read, Write))
      )
      RestrictionExpr(aclAuth) shouldEqual ComparisonExpr(Eq,
                                                          UriPath("organization".qualify),
                                                          UriTerm(OrgId("org").qualify))
    }

    "create org and domain expression when org ACL present" in {
      val aclAuth = FullAccessControlList(
        (GroupRef("BBP", "group1"), Path("/kg"), Permissions(Write)),
        (GroupRef("BBP", "group1"), Path("/kg/org/dom"), Permissions(Read)),
        (GroupRef("BBP", "group1"), Path("/kg/org"), Permissions(Own, Read, Write))
      )
      val orgExpr = ComparisonExpr(Eq, UriPath("organization".qualify), UriTerm(OrgId("org").qualify))
      val domExpr = ComparisonExpr(Eq, UriPath("domain".qualify), UriTerm(DomainId(OrgId("org"), "dom").qualify))

      RestrictionExpr(aclAuth) shouldEqual LogicalExpr(Or, List(domExpr, orgExpr))
    }

    "create noopExpr expression when no ACLS present on the root, org or domain level" in {
      val aclAuth = FullAccessControlList(
        (GroupRef("BBP", "group1"), Path("/kg/org/dom/schemaName"), Permissions(Read)),
        (GroupRef("BBP", "group1"), Path("/kg/org/dom/schemaName/v1.0.0"), Permissions(Read))
      )
      RestrictionExpr(aclAuth) shouldEqual NoopExpr
    }
  }

}
