package ch.epfl.bluebrain.nexus.kg

import java.time.Clock
import java.util.UUID

import ch.epfl.bluebrain.nexus.iam.client.types.Identity.Anonymous
import ch.epfl.bluebrain.nexus.iam.client.types.{AccessControlList, Identity, Permission, ResourceAccessControlList}
import ch.epfl.bluebrain.nexus.kg.config.Schemas.unconstrainedSchemaUri
import ch.epfl.bluebrain.nexus.kg.resources.ResourceF.Value
import ch.epfl.bluebrain.nexus.kg.resources.{Ref, ResId, ResourceF}
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax._
import io.circe.Json
import org.mockito.ArgumentMatchers.{argThat, isA => mockIsA}
import org.scalatest.EitherValues

import scala.reflect.ClassTag

trait TestHelper extends MockitoMatchers with EitherValues {

  private val clock = Clock.systemUTC()
  val read          = Permission.unsafe("resources/read")
  val write         = Permission.unsafe("files/write")

  def resourceAcls(acl: AccessControlList): ResourceAccessControlList =
    ResourceAccessControlList(url"http://example.com/id".value,
                              1L,
                              Set.empty,
                              clock.instant(),
                              Anonymous,
                              clock.instant(),
                              Anonymous,
                              acl)

  def simpleV(id: ResId,
              value: Json,
              rev: Long = 1L,
              types: Set[AbsoluteIri] = Set.empty,
              deprecated: Boolean = false,
              schema: Ref = Ref(unconstrainedSchemaUri),
              created: Identity = Anonymous,
              updated: Identity = Anonymous)(implicit clock: Clock): ResourceF[Value] =
    ResourceF(
      id,
      rev,
      types,
      deprecated,
      Map.empty,
      None,
      clock.instant(),
      clock.instant(),
      created,
      updated,
      schema,
      Value(value, value.contextValue, value.asGraph(id.value).right.value)
    )
  def simpleV(res: ResourceF[Json])(implicit clock: Clock) = ResourceF(
    res.id,
    res.rev,
    res.types,
    res.deprecated,
    Map.empty,
    None,
    clock.instant(),
    clock.instant(),
    res.createdBy,
    res.updatedBy,
    res.schema,
    Value(res.value, res.value.contextValue, res.value.asGraph(res.id.value).right.value)
  )

  def genUUID: UUID = UUID.randomUUID()

  def genIri: AbsoluteIri = url"http://example.com/".value + genUUID.toString
}

trait MockitoMatchers {
  def isA[T: ClassTag]: T =
    mockIsA(implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]])

  def matches[A](f: A => Boolean): A = {
    argThat((argument: A) => f(argument))
  }
}
