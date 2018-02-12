package ch.epfl.bluebrain.nexus.kg.indexing.query.builder

import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path, Permissions}
import ch.epfl.bluebrain.nexus.kg.core.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextId
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Expr._
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Op.Or
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.PropPath.{SubjectPath, UriPath}
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.Term.UriTerm
import ch.epfl.bluebrain.nexus.kg.core.queries.filtering.{Expr, Op}
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import shapeless.{:+:, CNil, Coproduct, Poly1}

/**
  * Defines the expression filter to add for different resurce's Id
  *
  * @tparam Id the identity type of the resource
  */
sealed trait ResourceRestrictionExpr[Id] {

  protected case object Root

  protected type Ids = Root.type :+: OrgId :+: DomainId :+: CNil

  private implicit def toIds(path: Path): Option[Ids] = path.segments match {
    case _ :: org :: dom :: Nil => Some(Coproduct[Ids](DomainId(OrgId(org), dom)))
    case _ :: org :: Nil        => Some(Coproduct[Ids](OrgId(org)))
    case _ :: Nil               => Some(Coproduct[Ids](Root))
    case _                      => None
  }

  /**
    * A method to convert the given ''Ids'' of a [[Coproduct]] to the expression for that Id
    *
    * @param id the value of the generic type ''Ids''
    */
  def mapToExpr(id: Ids)(implicit Q: ConfiguredQualifier[String],
                         orgQ: ConfiguredQualifier[OrgId],
                         domQ: ConfiguredQualifier[DomainId]): Option[Expr]

  /**
    * Constructs an expression filter that restrict resources based on provided ''acls''
    *
    * @param acls the acls used to created the expression
    */
  final def apply(acls: FullAccessControlList)(implicit Q: ConfiguredQualifier[String],
                                               orgQ: ConfiguredQualifier[OrgId],
                                               domQ: ConfiguredQualifier[DomainId]): Expr =
    acls.toPathMap.collect {
      case (path, perms) if perms.containsAny(Permissions(Read)) => (path: Option[Ids]).flatMap(mapToExpr)
    }.flatten match {
      case Nil                                  => NoopExpr
      case expr :: Nil                          => expr
      case exprs if exprs.exists(_ == NoopExpr) => NoopExpr
      case exprs                                => LogicalExpr(Or, exprs.toList)
    }
}

/**
  * Implementation of an expression filter on organization and domains which are linked to the resource's Id through a relationship
  *
  * @tparam Id the identity type of the resource
  */
private class RestrictionExprAncestor[Id] extends ResourceRestrictionExpr[Id] {

  override def mapToExpr(id: Ids)(implicit Q: ConfiguredQualifier[String],
                                  orgQ: ConfiguredQualifier[OrgId],
                                  domQ: ConfiguredQualifier[DomainId]): Option[Expr] = {
    object expr extends Poly1 {
      implicit def caseRoot: Case.Aux[Root.type, Option[Expr]] = at(_ => Some(NoopExpr))
      implicit def caseOrgId: Case.Aux[OrgId, Option[Expr]] =
        at(org => Some(ComparisonExpr(Op.Eq, UriPath("organization".qualify), UriTerm(org.qualify))))
      implicit def caseDomId: Case.Aux[DomainId, Option[Expr]] =
        at(dom => Some(ComparisonExpr(Op.Eq, UriPath("domain".qualify), UriTerm(dom.qualify))))
    }

    id.fold(expr)
  }
}

object ResourceRestrictionExpr {
  final implicit val restrictionExprInstance: ResourceRestrictionExpr[InstanceId] = new RestrictionExprAncestor()

  final implicit val restrictionExprSchemaId: ResourceRestrictionExpr[SchemaId] = new RestrictionExprAncestor()

  final implicit val restrictionExprContextId: ResourceRestrictionExpr[ContextId] = new RestrictionExprAncestor()

  final implicit val restrictionExprDomainId: ResourceRestrictionExpr[DomainId] =
    /**
      * Implementation of an expression filter where the domain exists directly on the resource
      * and the organization is linked to the resource through a relationship
      */
    new ResourceRestrictionExpr[DomainId] {
      override def mapToExpr(id: Ids)(implicit Q: ConfiguredQualifier[String],
                                      orgQ: ConfiguredQualifier[OrgId],
                                      domQ: ConfiguredQualifier[DomainId]): Option[Expr] = {
        object expr extends Poly1 {
          implicit def caseRoot: Case.Aux[Root.type, Option[Expr]] = at(_ => Some(NoopExpr))
          implicit def caseOrgId: Case.Aux[OrgId, Option[Expr]] =
            at(org => Some(ComparisonExpr(Op.Eq, UriPath("organization".qualify), UriTerm(org.qualify))))
          implicit def caseDomId: Case.Aux[DomainId, Option[Expr]] =
            at(dom => Some(ComparisonExpr(Op.Eq, SubjectPath, UriTerm(dom.qualify))))
        }

        id.fold(expr)
      }
    }

  final implicit val restrictionExprOrgId: ResourceRestrictionExpr[OrgId] =
    /**
      * Implementation of an expression filter where the organization exists directly on the resource
      */
    new ResourceRestrictionExpr[OrgId] {

      override def mapToExpr(id: Ids)(implicit Q: ConfiguredQualifier[String],
                                      orgQ: ConfiguredQualifier[OrgId],
                                      domQ: ConfiguredQualifier[DomainId]): Option[Expr] = {
        object expr extends Poly1 {
          implicit def caseRoot: Case.Aux[Root.type, Option[Expr]] = at(_ => Some(NoopExpr))
          implicit def caseOrgId: Case.Aux[OrgId, Option[Expr]] =
            at(org => Some(ComparisonExpr(Op.Eq, SubjectPath, UriTerm(org.qualify))))
          implicit def caseDomId: Case.Aux[DomainId, Option[Expr]] = at(_ => None)
        }

        id.fold(expr)
      }
    }
}
