package ch.epfl.bluebrain.nexus.kg.query

import java.util.regex.Pattern.quote

import cats.instances.string._
import cats.instances.future._
import akka.http.scaladsl.model.Uri.Path
import ch.epfl.bluebrain.nexus.commons.es.client.{ElasticClient, ElasticDecoder, ElasticQueryClient}
import ch.epfl.bluebrain.nexus.commons.es.server.embed.ElasticServer
import ch.epfl.bluebrain.nexus.commons.http.HttpClient
import ch.epfl.bluebrain.nexus.commons.http.HttpClient.{UntypedHttpClient, akkaHttpClient, withAkkaUnmarshaller}
import ch.epfl.bluebrain.nexus.commons.test.{Randomness, Resources}
import ch.epfl.bluebrain.nexus.commons.types.search.QueryResults
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.IndexingVocab.PrefixMapping.rdfTypeKey
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.commons.http.JsonLdCirceSupport._
import ch.epfl.bluebrain.nexus.commons.iam.acls.{FullAccessControlList, Path => IAMPath, Permissions}
import ch.epfl.bluebrain.nexus.commons.iam.acls.Permission.Read
import ch.epfl.bluebrain.nexus.commons.iam.identity.Identity
import ch.epfl.bluebrain.nexus.kg.core.{ConfiguredQualifier, Qualifier}
import ch.epfl.bluebrain.nexus.kg.core.Qualifier._
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.{ElasticIndexingSettings, IndexerFixture}
import io.circe.Decoder
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}

import scala.concurrent.Future
import scala.concurrent.duration._

trait QueryFixture[Id]
    extends ElasticServer
    with IndexerFixture
    with WordSpecLike
    with Matchers
    with ScalaFutures
    with Randomness
    with Resources
    with Inspectors
    with BeforeAndAfterAll
    with Eventually
    with CancelAfterFailure
    with Assertions {
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(6 seconds, 300 milliseconds)

  val base = s"http://$localhost:8080/v0"
  val settings @ ElasticIndexingSettings(_, _, _, nexusVocBase) =
    ElasticIndexingSettings(genString(length = 6),
                            genString(length = 6),
                            base,
                            s"http://$localhost:8080/vocabs/nexus/core/terms/v0.1.0/")
  implicit val orgIdQualifier: ConfiguredQualifier[OrgId]             = Qualifier.configured[OrgId](base)
  implicit val domainIdQualifier: ConfiguredQualifier[DomainId]       = Qualifier.configured[DomainId](base)
  implicit val contextNameQualifier: ConfiguredQualifier[ContextName] = Qualifier.configured[ContextName](base)
  implicit val contextIdQualifier: ConfiguredQualifier[ContextId]     = Qualifier.configured[ContextId](base)
  implicit val schemaNameQualifier: ConfiguredQualifier[SchemaName]   = Qualifier.configured[SchemaName](base)
  implicit val schemaIdQualifier: ConfiguredQualifier[SchemaId]       = Qualifier.configured[SchemaId](base)
  implicit val instanceIdQualifier: ConfiguredQualifier[InstanceId]   = Qualifier.configured[InstanceId](base)
  implicit val stringQualifier: ConfiguredQualifier[String]           = Qualifier.configured[String](nexusVocBase)

  val revKey: String        = "rev".qualifyAsString
  val deprecatedKey: String = "deprecated".qualifyAsString
  val publishedKey: String  = "published".qualifyAsString
  val nameKey: String       = "name".qualifyAsString
  val orgKey: String        = "organization".qualifyAsString
  val domainKey: String     = "domain".qualifyAsString
  val schemaKey: String     = "schema".qualifyAsString
  val pageSize: Int         = 3
  val objectCount: Int      = (pageSize * 2) + 1

  implicit val untypedHttpClient: UntypedHttpClient[Future] = akkaHttpClient
  implicit val idDecoder: Decoder[Id]
  implicit val D: Decoder[QueryResults[Id]]                   = ElasticDecoder[Id]
  implicit val rsSearch: HttpClient[Future, QueryResults[Id]] = withAkkaUnmarshaller[QueryResults[Id]]
  val elasticQueryClient                                      = ElasticQueryClient[Future](esUri)

  val elasticClient: ElasticClient[Future] = ElasticClient[Future](esUri, elasticQueryClient)
  val refreshUri                           = esUri.withPath(Path.SingleSlash + "_refresh")

  val mapping = jsonContentOf(
    "/es-index.json",
    Map(
      quote("{{type}}")         -> "doc",
      quote("{{rdfType}}")      -> rdfTypeKey,
      quote("{{contextGroup}}") -> "contextGroup".qualifyAsString,
      quote("{{organization}}") -> orgKey,
      quote("{{domain}}")       -> domainKey,
      quote("{{schema}}")       -> schemaKey,
      quote("{{schemaGroup}}")  -> "schemaGroup".qualifyAsString,
      quote("{{deprecated}}")   -> deprecatedKey,
      quote("{{published}}")    -> publishedKey
    )
  )

  val defaultAcls = FullAccessControlList((Identity.Anonymous(), IAMPath./("kg"), Permissions(Read)))
}
