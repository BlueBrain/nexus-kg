package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path, Permissions}
import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.Or
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.UriPath
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.UriTerm
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Expr, Op}
import shapeless.{:+:, CNil, Coproduct, Poly1}

private class RestrictionExpr()(implicit Q: ConfiguredQualifier[String],
                                orgQ: ConfiguredQualifier[OrgId],
                                domQ: ConfiguredQualifier[DomainId]) {

  private val orgKey = "organization".qualify
  private val domKey = "domain".qualify

  private[builder] case object Root

  private type Id = Root.type :+: OrgId :+: DomainId :+: CNil

  private implicit def toIds(path: Path): Option[Id] = path.segments match {
    case _ :: org :: dom :: Nil => Some(Coproduct[Id](DomainId(OrgId(org), dom)))
    case _ :: org :: Nil        => Some(Coproduct[Id](OrgId(org)))
    case _ :: Nil               => Some(Coproduct[Id](Root))
    case _                      => None
  }

  object expr extends Poly1 {
    implicit def caseRoot: Case.Aux[Root.type, Expr] = at(_ => NoopExpr)
    implicit def caseOrgId: Case.Aux[OrgId, Expr] =
      at(org => ComparisonExpr(Op.Eq, UriPath(orgKey), UriTerm(org.qualify)))
    implicit def caseDomId: Case.Aux[DomainId, Expr] =
      at(dom => ComparisonExpr(Op.Eq, UriPath(domKey), UriTerm(dom.qualify)))
  }

  def apply(acls: FullAccessControlList): Expr =
    acls.toPathMap.collect {
      case (path, perms) if perms.containsAny(Permissions(Read)) => (path: Option[Id]).map(_.fold(expr))
    }.flatten match {
      case Nil                                  => NoopExpr
      case expr :: Nil                          => expr
      case exprs if exprs.exists(_ == NoopExpr) => NoopExpr
      case exprs                                => LogicalExpr(Or, exprs.toList)
    }

}

object RestrictionExpr {
  final def apply(acls: FullAccessControlList)(implicit Q: ConfiguredQualifier[String],
                                               orgQ: ConfiguredQualifier[OrgId],
                                               domQ: ConfiguredQualifier[DomainId]): Expr = {
    val restriction = new RestrictionExpr()
    restriction(acls)
  }
}
