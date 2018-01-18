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
  private val Slash = "%2F"

  "An ElasticIds" should {

    "convert ids into ElasticSearch ids" in {
      OrgId("one").elasticId shouldEqual "orgid_one"
      DomainId(OrgId("one"), "two").elasticId shouldEqual s"domainid_one${Slash}two"
      SchemaId(DomainId(OrgId("one"), "two"), "name", Version(1, 0, 0)).elasticId shouldEqual s"schemaid_one${Slash}two${Slash}name${Slash}v1.0.0"
    }

    "convert ids into ElasticSearch index ids" in {
      val prefix = "some"
      OrgId("one").toIndex(prefix) shouldEqual "some_orgid_one"
      DomainId(OrgId("one"), "two").toIndex(prefix) shouldEqual s"some_domainid_one${Slash}two"
      SchemaId(DomainId(OrgId("one"), "two"), "name", Version(1, 0, 0))
        .toIndex(prefix) shouldEqual s"some_domainid_one${Slash}two"
      InstanceId(SchemaId(DomainId(OrgId("one"), "two"), "name", Version(1, 0, 0)), UUID.randomUUID().toString)
        .toIndex(prefix) shouldEqual s"some_domainid_one${Slash}two"
    }
  }
}
