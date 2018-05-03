package ch.epfl.bluebrain.nexus.kg.core.access

import ch.epfl.bluebrain.nexus.kg.core.access.Access._
import ch.epfl.bluebrain.nexus.kg.core.resources.ResourceType
import com.github.ghik.silencer.silent

import scala.annotation.implicitNotFound

/**
  * Trait that provides evidence that a certain type ''T'' has a certain access ''A''
  *
  * @tparam T a subtype of [[ResourceType]]
  * @tparam A a subtype of [[Access]]
  */
@implicitNotFound("'${T}' type does not have access to perform '${A}'.")
@SuppressWarnings(Array("UnusedMethodParameter"))
trait HasAccess[T <: ResourceType, A <: Access]

@SuppressWarnings(Array("UnusedMethodParameter"))
object HasAccess {

  def apply[T <: ResourceType, A <: Access](): HasAccess[T, A] = new HasAccess[T, A] {}

  implicit def readFromManage[T <: ResourceType](implicit @silent m: T HasAccess Manage): T HasAccess Read     = null
  implicit def writeFromManage[T <: ResourceType](implicit @silent m: T HasAccess Manage): T HasAccess Write   = null
  implicit def createFromManage[T <: ResourceType](implicit @silent m: T HasAccess Manage): T HasAccess Create = null
  implicit def attachFromManage[T <: ResourceType](implicit @silent m: T HasAccess Manage): T HasAccess Attach = null
}
