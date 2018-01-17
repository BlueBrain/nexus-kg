package ch.epfl.bluebrain.nexus.kg.indexing

import java.net.URLEncoder

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.kg.core.contexts.ContextId
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import shapeless.Typeable

/**
  * Trait which defines the signature for converting ids to the ElasticSearch index
  *
  * @tparam A the generic type of the id
  */
trait ElasticIndexerId[A] {

  /**
    * The signature for converting an ''id'' with a ''prefix'' into a uniquely valid ElasticSearch index
    *
    * @param id     the id to convert
    * @param prefix the prefix to apply
    */
  def apply(id: A, prefix: String): String
}

object ElasticIds {

  implicit val orgElasticIndexer = new ElasticIndexerId[OrgId] {
    override def apply(id: OrgId, prefix: String): String = elasticId(id, prefix)
  }

  implicit val domainElasticIndexer = new ElasticIndexerId[DomainId] {
    override def apply(id: DomainId, prefix: String): String = elasticId(id, prefix)
  }

  implicit val schemaElasticIndexer = new ElasticIndexerId[SchemaId] {
    override def apply(id: SchemaId, prefix: String): String = domainElasticIndexer(id.domainId, prefix)
  }

  implicit val contextElasticIndexer = new ElasticIndexerId[ContextId] {
    override def apply(id: ContextId, prefix: String): String = domainElasticIndexer(id.domainId, prefix)
  }

  implicit val instanceElasticIndexer = new ElasticIndexerId[InstanceId] {
    override def apply(id: InstanceId, prefix: String): String = domainElasticIndexer(id.schemaId.domainId, prefix)
  }

  private def elasticId[A: Show](id: A, prefix: String)(implicit T: Typeable[A]): String =
    prefix + "_" + T.describe.toLowerCase + "_" + URLEncoder.encode(id.show, "UTF-8")

  /**
    * Interface syntax to expose new functionality into the generic type A
    *
    * @param id        the instance of ''A''
    * @param converter the implicitly available indexer converter
    * @tparam A the generic type
    */
  implicit class ElasticIndexerIdSyntax[A](id: A)(implicit converter: ElasticIndexerId[A]) {
    def toIndex(prefix: String): String = converter(id, prefix)
  }

  /**
    * Interface syntax to expose new functionality into the generic type A
    *
    * @param id   the instance of ''A''
    * @param ev$1 the implicitly available ''Show[A]''
    * @param T    the implicitly available ''Typeable[A]''
    * @tparam A the generic type
    */
  implicit class ElasticIdSyntax[A: Show](id: A)(implicit T: Typeable[A]) {

    /**
      * Converts this ''id'' to a uniquely valid ElasticSearch id
      */
    def elasticId: String = T.describe.toLowerCase + "_" + URLEncoder.encode(id.show, "UTF-8")
  }

}
