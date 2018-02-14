package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID

import ch.epfl.bluebrain.nexus.commons.test.Randomness
import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.SchemaId
import ch.epfl.bluebrain.nexus.kg.indexing.ElasticIds._
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class ElasticIdsSpec extends WordSpecLike with Matchers with Inspectors with Randomness {

  "An ElasticIds" should {

    "convert ids into ElasticSearch ids" in {
      OrgId("one").elasticId shouldEqual "orgid_one"
      DomainId(OrgId("one"), "two").elasticId shouldEqual s"domainid_one/two"
      SchemaId(DomainId(OrgId("one"), "two"), "name", Version(1, 0, 0)).elasticId shouldEqual s"schemaid_one/two/name/v1.0.0"
    }

    "convert ids into ElasticSearch index ids" in {
      val prefix = "some"
      OrgId("one").toIndex(prefix) shouldEqual s"${prefix}_organizations"
      DomainId(OrgId("one"), "two").toIndex(prefix) shouldEqual s"${prefix}_domains"
      SchemaId(DomainId(OrgId("one"), "two"), "name", Version(1, 0, 0))
        .toIndex(prefix) shouldEqual s"${prefix}_schemas"
      InstanceId(SchemaId(DomainId(OrgId("one"), "two"), "name", Version(1, 0, 0)), UUID.randomUUID().toString)
        .toIndex(prefix) shouldEqual s"${prefix}_instances_domainid_one_two"
    }
  }
}
