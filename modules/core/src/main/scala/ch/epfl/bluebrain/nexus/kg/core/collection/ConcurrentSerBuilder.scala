package ch.epfl.bluebrain.nexus.kg.core.collection

import java.lang
import java.util.Collections.newSetFromMap
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.collection.mutable.Set

object ConcurrentSerBuilder {

  /**
    * Constructs a concurrent set which uses an underlying [[ConcurrentHashMap]] implementation
    *
    * @param elems the initial elements to add to the set
    * @tparam A the generic type of the set
    */
  final def apply[A](elems: A*): Set[A] = {
    val set = newSetFromMap(new ConcurrentHashMap[A, lang.Boolean])
    set.addAll(elems.asJava)
    set.asScala
  }

}
