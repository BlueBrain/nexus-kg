package ch.epfl.bluebrain.nexus.kg.indexing

import java.util.UUID

import akka.http.scaladsl.model.Uri
import cats.Show
import ch.epfl.bluebrain.nexus.kg.core.Randomness
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import org.scalatest.{Matchers, WordSpecLike}
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.common.types.Version
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.ShapeId

class QualifierSpec extends WordSpecLike with Matchers with Randomness {

  def genUUID(): String = UUID.randomUUID().toString.toLowerCase
  val base = Uri("http://localhost/base")

  "A OrgId" should {
    val id = OrgId("org")

    "be mapped into a qualified uri using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[OrgId] = Qualifier.configured[OrgId](base)
      id.qualify shouldEqual Uri("http://localhost/base/organizations/org")
    }

    "be mapped into a qualified uri in string format using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[OrgId] = Qualifier.configured[OrgId](base)
      id.qualifyAsString shouldEqual "http://localhost/base/organizations/org"
    }

    "be mapped into a qualified uri using an explicit base uri" in {
      id.qualifyWith("http://localhost/explicit") shouldEqual Uri("http://localhost/explicit/organizations/org")
    }

    "be mapped into a qualified uri in string format" in {
      id.qualifyAsStringWith("http://localhost/explicit") shouldEqual "http://localhost/explicit/organizations/org"
    }
  }

  "A DomainId" should {
    val id = DomainId(OrgId("org"), "dom")

    "be mapped into a qualified uri using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[DomainId] = Qualifier.configured[DomainId](base)
      id.qualify shouldEqual Uri("http://localhost/base/organizations/org/domains/dom")
    }

    "be mapped into a qualified uri in string format using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[DomainId] = Qualifier.configured[DomainId](base)
      id.qualifyAsString shouldEqual "http://localhost/base/organizations/org/domains/dom"
    }

    "be mapped into a qualified uri using an explicit base uri" in {
      id.qualifyWith("http://localhost/explicit") shouldEqual Uri("http://localhost/explicit/organizations/org/domains/dom")
    }

    "be mapped into a qualified uri in string format" in {
      id.qualifyAsStringWith("http://localhost/explicit") shouldEqual "http://localhost/explicit/organizations/org/domains/dom"
    }
  }

  "A SchemaName" should {
    val id = SchemaName(DomainId(OrgId("org"), "dom"),"name")

    "be mapped into a qualified uri using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
      id.qualify shouldEqual Uri("http://localhost/base/schemas/org/dom/name")
    }

    "be mapped into a qualified uri in string format using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
      id.qualifyAsString shouldEqual "http://localhost/base/schemas/org/dom/name"
    }

    "be mapped into a qualified uri using an explicit base uri" in {
      id.qualifyWith("http://localhost/explicit") shouldEqual Uri("http://localhost/explicit/schemas/org/dom/name")
    }

    "be mapped into a qualified uri in string format" in {
      id.qualifyAsStringWith("http://localhost/explicit") shouldEqual "http://localhost/explicit/schemas/org/dom/name"
    }
  }

  "A SchemaId" should {
    val id = SchemaId(DomainId(OrgId("org"), "dom"),"name", Version(1,0,0))

    "be mapped into a qualified uri using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)
      id.qualify shouldEqual Uri("http://localhost/base/schemas/org/dom/name/v1.0.0")
    }

    "be mapped into a qualified uri in string format using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)
      id.qualifyAsString shouldEqual "http://localhost/base/schemas/org/dom/name/v1.0.0"
    }

    "be mapped into a qualified uri using an explicit base uri" in {
      id.qualifyWith("http://localhost/explicit") shouldEqual Uri("http://localhost/explicit/schemas/org/dom/name/v1.0.0")
    }

    "be mapped into a qualified uri in string format" in {
      id.qualifyAsStringWith("http://localhost/explicit") shouldEqual "http://localhost/explicit/schemas/org/dom/name/v1.0.0"
    }
  }

  "A ShapeId" should {
    val id = ShapeId(SchemaId(DomainId(OrgId("org"), "dom"),"name", Version(1,0,0)), "fragment")

    "be mapped into a qualified uri using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[ShapeId] = Qualifier.configured[ShapeId](base)
      id.qualify shouldEqual Uri("http://localhost/base/schemas/org/dom/name/v1.0.0/shapes/fragment")
    }

    "be mapped into a qualified uri in string format using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[ShapeId] = Qualifier.configured[ShapeId](base)
      id.qualifyAsString shouldEqual "http://localhost/base/schemas/org/dom/name/v1.0.0/shapes/fragment"
    }

    "be mapped into a qualified uri using an explicit base uri" in {
      id.qualifyWith("http://localhost/explicit") shouldEqual Uri("http://localhost/explicit/schemas/org/dom/name/v1.0.0/shapes/fragment")
    }

    "be mapped into a qualified uri in string format" in {
      id.qualifyAsStringWith("http://localhost/explicit") shouldEqual "http://localhost/explicit/schemas/org/dom/name/v1.0.0/shapes/fragment"
    }
  }

  "An InstanceId" should {
    val id = InstanceId(SchemaId(DomainId(OrgId("org"), "dom"),"name", Version(1,0,0)), genUUID())

    "be mapped into a qualified uri using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[InstanceId] = Qualifier.configured[InstanceId](base)
      id.qualify shouldEqual Uri(s"http://localhost/base/data/org/dom/name/v1.0.0/${id.id}")
    }

    "be mapped into a qualified uri in string format using a configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[InstanceId] = Qualifier.configured[InstanceId](base)
      id.qualifyAsString shouldEqual s"http://localhost/base/data/org/dom/name/v1.0.0/${id.id}"
    }

    "be mapped into a qualified uri using an explicit base uri" in {
      id.qualifyWith("http://localhost/explicit") shouldEqual Uri(s"http://localhost/explicit/data/org/dom/name/v1.0.0/${id.id}")
    }

    "be mapped into a qualified uri in string format" in {
      id.qualifyAsStringWith("http://localhost/explicit") shouldEqual s"http://localhost/explicit/data/org/dom/name/v1.0.0/${id.id}"
    }
  }

  "A qualifier uri" should {
    val orgId = OrgId(genString(length = 4))
    val domainId = DomainId(orgId, genString(length = 8))
    val schemaName = SchemaName(domainId, genString(length = 4))
    val schemaId = schemaName.versioned(genVersion())

    "unqualify the uri into an InstanceId using an explicit base uri" in {
      val id = InstanceId(schemaId, genUUID())
      s"$base/data/${id.show}".unqualifyWith[InstanceId](base) shouldEqual Some(id)
    }

    "unqualify the uri into an InstanceId using an configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[InstanceId] = Qualifier.configured[InstanceId](base)
      val id = InstanceId(schemaId, genUUID())
      s"$base/data/${id.show}".unqualify[InstanceId] shouldEqual Some(id)
    }

    "unqualify the uri into an SchemaId using an explicit base uri" in {
      s"$base/schemas/${schemaId.show}".unqualifyWith[SchemaId](base) shouldEqual Some(schemaId)
    }

    "unqualify the uri into an SchemaId using an configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[SchemaId] = Qualifier.configured[SchemaId](base)
      s"$base/schemas/${schemaId.show}".unqualify[SchemaId] shouldEqual Some(schemaId)
    }

    "unqualify the uri into an SchemaName using an explicit base uri" in {
      s"$base/schemas/${schemaName.show}".unqualifyWith[SchemaName](base) shouldEqual Some(schemaName)
    }

    "unqualify the uri into an SchemaName using an configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[SchemaName] = Qualifier.configured[SchemaName](base)
      s"$base/schemas/${schemaName.show}".unqualify[SchemaName] shouldEqual Some(schemaName)
    }

    "unqualify the uri into an ShapeId using an explicit base uri" in {
      val shapeId = ShapeId(schemaId, "fragment")
      s"$base/schemas/${shapeId.show}".unqualifyWith[ShapeId](base) shouldEqual Some(shapeId)
    }

    "unqualify the uri into an ShapeId using an configured base uri" in {
      implicit val qualifier: ConfiguredQualifier[ShapeId] = Qualifier.configured[ShapeId](base)
      val shapeId = ShapeId(schemaId, "fragment")
      s"$base/schemas/${shapeId.show}".unqualify[ShapeId] shouldEqual Some(shapeId)

    }

    "unqualify the uri into an DomainId using an explicit base uri" in {
      implicit val showDomainId: Show[DomainId] = Show.show(domain => s"organizations/${orgId.id}/domains/${domainId.id}")
      s"$base/${domainId.show}".unqualifyWith[DomainId](base) shouldEqual Some(domainId)
    }

    "unqualify the uri into an DomainId using an configured base uri" in {
      implicit val showDomainId: Show[DomainId] = Show.show(domain => s"organizations/${orgId.id}/domains/${domainId.id}")
      implicit val qualifier: ConfiguredQualifier[DomainId] = Qualifier.configured[DomainId](base)
      s"$base/${domainId.show}".unqualify[DomainId] shouldEqual Some(domainId)
    }

    "unqualify the uri into an OrgId using an explicit base uri" in {
      implicit val showDomainId: Show[OrgId] = Show.show(org => s"organizations/${org.id}")
      s"$base/${orgId.show}".unqualifyWith[OrgId](base) shouldEqual Some(orgId)
    }

    "unqualify the uri into an OrgId using an configured base uri" in {
      implicit val showDomainId: Show[OrgId] = Show.show(org => s"organizations/${org.id}")
      implicit val qualifier: ConfiguredQualifier[OrgId] = Qualifier.configured[OrgId](base)
      s"$base/${orgId.show}".unqualify[OrgId] shouldEqual Some(orgId)
    }


  }
}