package ch.epfl.bluebrain.nexus.kg.query

import cats.MonadError
import cats.instances.string._
import ch.epfl.bluebrain.nexus.commons.es.client.ElasticClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults.UnscoredQueryResults
import ch.epfl.bluebrain.nexus.commons.types.search._
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping.rdfTypeKey
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIndexingSettings
import io.circe.Json

/**
  * Base class implementing common Elastic Search queries
  *
  * @param elasticClient  Elastic Search client
  * @param settings       Elastic Search settings
  * @param rs             HTTP client
  * @tparam F             the monadic effect type
  * @tparam Id            type of the ID that's returned
  */
abstract class BaseElasticQueries[F[_], Id](elasticClient: ElasticClient[F], settings: ElasticIndexingSettings)(
    implicit
    rs: HttpClient[F, QueryResults[Id]],
    F: MonadError[F, Throwable]) {

  val ElasticIndexingSettings(prefix, _, base, baseVoc) = settings
  protected val defaultSort                             = SortList(List(Sort(Sort.OrderType.Asc, "@id")))

  implicit val orgIdQualifier: ConfiguredQualifier[OrgId]           = Qualifier.configured[OrgId](base)
  implicit val domainIdQualifier: ConfiguredQualifier[DomainId]     = Qualifier.configured[DomainId](base)
  implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]     = Qualifier.configured[SchemaId](base)
  implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
  implicit val stringQualifier: ConfiguredQualifier[String]         = Qualifier.configured[String](baseVoc)
  val deprecatedKey: String                                         = "deprecated".qualifyAsString
  val publishedKey: String                                          = "published".qualifyAsString

  /**
    * Index used for searching
    */
  protected val index: String

  /**
    * RDF type used to filter entities
    */
  protected val rdfType: String

  /**
    * Created filter terms from `deprecated` and `published` parmeters
    * @param deprecated deprecated filter value
    * @param published  published filter value
    * @return `Json` object representing the filter
    */
  protected def termsFrom(deprecated: Option[Boolean], published: Option[Boolean]): Seq[Json] = {
    val deprecatedTerm = deprecated.map(d => term(deprecatedKey, d))
    val publishedTerm  = published.map(p => term(publishedKey, p))

    Seq(deprecatedTerm, publishedTerm).flatten
  }

  /**
    * Embed given `terms` inside a Elasitc Search Query and add RDF type term
    * @param acls   list of access controls to restrict the query
    * @param terms  filter terms
    * @return `Json` object representing the query
    */
  protected def query(acls: FullAccessControlList, terms: Json*): Json = {
    matchAllQuery(terms :+ term(rdfTypeKey, rdfType), aclsToFilter(acls))
  }

  /**
    * Create a boolean term for Elastic Search filter
    * @param key    field on which to apply the filter
    * @param value  filter value
    * @return `Json` object representing the term
    */
  protected def term(key: String, value: Boolean): Json =
    Json.obj(
      "term" -> Json.obj(
        key -> Json.fromBoolean(value)
      )
    )

  /**
    ** Create a string term for Elastic Search filter
    * @param key    field on which to apply the filter
    * @param value  filter value
    * @return `Json` object representing the term
    */
  protected def term(key: String, value: String): Json =
    Json.obj(
      "term" -> Json.obj(
        key -> Json.fromString(value)
      )
    )

  /**
    * Create a term for SchemaGroup filter
    * @param schemaName name of the schema to filter by
    * @return `Json` object representing the term
    */
  protected def schemaGroupTerm(schemaName: SchemaName): Json =
    term("schemaGroup".qualifyAsString, schemaName.qualifyAsString)

  private def matchAllQuery(filterTerms: Seq[Json], aclTerms: Option[Json]) =
    Json.obj(
      "query" -> Json.obj(
        "bool" -> Json.obj(
          "filter" -> Json.obj(
            "bool" -> Json.obj(
              "must" -> Json.arr(filterTerms ++ aclTerms: _*)
            )
          )
        )
      )
    )

  private val kgRoot         = "kg"
  private val rootPath       = Path(s"/$kgRoot")
  private val readPermission = Read

  /**
    * Generate filter term for organization
    * @param orgId  organization ID
    * @return JSON object representing the filter
    */
  protected def orgTerm(orgId: OrgId): Json = term("organization".qualifyAsString, orgId.qualifyAsString)

  /**
    * Generate filter term for domain
    * @param domainId domain ID
    * @return JSON object representing the filter
    */
  protected def domainTerm(domainId: DomainId): Json = term("domain".qualifyAsString, domainId.qualifyAsString)

  private def aclsToFilter(acls: FullAccessControlList): Option[Json] = {

    if (acls.toPathMap.getOrElse(rootPath, Permissions.empty).contains(readPermission)) {
      None
    } else {
      val terms = acls.toPathMap
        .filter { case (_, permissions) => permissions.contains(readPermission) }
        .flatMap {
          case (path, _) =>
            path.segments match {
              case `kgRoot` :: orgId :: domId :: Nil => Some(domainTerm(DomainId(OrgId(orgId), domId)))
              case `kgRoot` :: orgId :: Nil          => Some(orgTerm(OrgId(orgId)))
              case _                                 => None
            }
        }
        .toSeq
      Some(
        Json.obj(
          "bool" -> Json.obj(
            "should" -> Json.arr(terms: _*)
          )
        )
      )
    }
  }

  protected def hasReadPermissionsFor(domainId: DomainId, acls: FullAccessControlList): Boolean = {
    acls.toPathMap
      .filter { case (_, permissions) => permissions.contains(readPermission) }
      .map {
        case (path, _) =>
          path.segments match {
            case `kgRoot` :: Nil                                     => true
            case `kgRoot` :: domainId.orgId.id :: Nil                => true
            case `kgRoot` :: domainId.orgId.id :: domainId.id :: Nil => true
            case _                                                   => false
          }
      }
      .exists(b => b)

  }

  /**
    * List all objects of a given type
    * @param pagination   pagination object
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @param acls         list of access controls to restrict the query
    * @return query results
    */
  def list(pagination: Pagination,
           deprecated: Option[Boolean],
           published: Option[Boolean],
           acls: FullAccessControlList): F[QueryResults[Id]] = {
    if (acls.hasAnyPermission(Permissions(Read))) {
      elasticClient
        .search[Id](query(acls, termsFrom(deprecated, published): _*), Set(index))(pagination, sort = defaultSort)
    } else {
      F.pure(UnscoredQueryResults(0L, List.empty[QueryResult[Id]]))
    }
  }

  /**
    * List all objects of a given type within an organization
    * @param pagination   pagination object
    * @param orgId        organization ID
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @param acls         list of access controls to restrict the query
    * @return query results
    *
    */
  def list(pagination: Pagination,
           orgId: OrgId,
           deprecated: Option[Boolean],
           published: Option[Boolean],
           acls: FullAccessControlList): F[QueryResults[Id]] = {
    if (acls.hasAnyPermission(Permissions(Read))) {
      elasticClient.search[Id](query(acls, termsFrom(deprecated, published) :+ orgTerm(orgId): _*), Set(index))(
        pagination,
        sort = defaultSort)
    } else {
      F.pure(UnscoredQueryResults(0L, List.empty[QueryResult[Id]]))
    }
  }

  /**
    * List all objects of a given type within a domain
    *
    * @param pagination   pagination object
    * @param domainId     domain ID
    * @param deprecated   boolean to decide whether to filter deprecated objects
    * @param published    boolean to decide whether to filter published objects
    * @param acls         list of access controls to restrict the query
    * @return query results
    *
    */
  def list(pagination: Pagination,
           domainId: DomainId,
           deprecated: Option[Boolean],
           published: Option[Boolean],
           acls: FullAccessControlList): F[QueryResults[Id]] = {
    if (hasReadPermissionsFor(domainId, acls)) {
      elasticClient.search[Id](query(acls, termsFrom(deprecated, published) :+ domainTerm(domainId): _*), Set(index))(
        pagination,
        sort = defaultSort)
    } else {
      F.pure(UnscoredQueryResults(0L, List.empty[QueryResult[Id]]))
    }
  }

}
