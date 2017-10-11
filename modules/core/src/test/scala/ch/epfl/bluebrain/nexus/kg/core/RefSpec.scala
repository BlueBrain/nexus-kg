package ch.epfl.bluebrain.nexus.kg.core

import java.util.UUID

import ch.epfl.bluebrain.nexus.commons.types.Version
import ch.epfl.bluebrain.nexus.kg.core.domains.{DomainId, DomainRef}
import ch.epfl.bluebrain.nexus.kg.core.instances.{InstanceId, InstanceRef}
import ch.epfl.bluebrain.nexus.kg.core.organizations.{OrgId, OrgRef}
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaRef}
import org.scalatest.{Inspectors, Matchers, WordSpecLike}

class RefSpec extends WordSpecLike with Matchers with Inspectors {
  "A Ref" should {

    def anyId[Id](ref: Ref[Id]): Id = ref.id
    def anyRev[Id](ref: Ref[Id]): Long = ref.rev

    "be able to convert any ref into it's id and it's revision" in {
      val orgRef = OrgRef(OrgId("org"), 1L)
      anyId(orgRef) shouldEqual orgRef.id
      anyRev(orgRef) shouldEqual orgRef.rev
      val domainRef = DomainRef(DomainId(orgRef.id, "domain"), 2L)
      anyId(domainRef) shouldEqual domainRef.id
      anyRev(domainRef) shouldEqual domainRef.rev

      val schemaRef =
        SchemaRef(SchemaId(domainRef.id, "name", Version(1, 0, 0)), 3L)
      anyId(schemaRef) shouldEqual schemaRef.id
      anyRev(schemaRef) shouldEqual schemaRef.rev

      val instanceRef =
        InstanceRef(InstanceId(schemaRef.id, UUID.randomUUID().toString), 3L)
      anyId(instanceRef) shouldEqual instanceRef.id
      anyRev(instanceRef) shouldEqual instanceRef.rev

    }

    def anyWithImplicit[A, Id](ref: A)(implicit C: A => Ref[Id]): Id = ref.id

    "be able to convert any ref into it's id using implicit" in {
      val orgRef = OrgRef(OrgId("org"), 1L)
      anyWithImplicit(orgRef) shouldEqual orgRef.id
    }
  }

}
