package ch.epfl.bluebrain.nexus.kg.indexing

import java.lang

import scala.collection.JavaConverters._
import java.util.Collections.newSetFromMap
import java.util.concurrent.ConcurrentHashMap
import collection.mutable.Set

/**
  * Concurrent set which uses an underlying [[ConcurrentHashMap]] implementation
  *
  * @param elems the initial elements to add to the set
  * @tparam A the generic type of the set
  */
case class ConcurrentSet[A](private val elems: A*) {

  /** Tests if some element is contained in this set.
    *
    * This method is equivalent to `contains`. It allows sets to be interpreted as predicates.
    *
    * @param elem the element to test for membership.
    * @return `true` if `elem` is contained in this set, `false` otherwise.
    */
  def apply(elem: A): Boolean = set(elem)

  /** Tests if some element is contained in this set.
    *
    * @param elem the element to test for membership.
    * @return `true` if `elem` is contained in this set, `false` otherwise.
    */
  def contains(elem: A): Boolean = apply(elem)

  /**
    * Adds a single element to the set.
    *
    * @param elem the element
    */
  def +=(elem: A): Set[A] = set += elem

  /**
    * Removes a single element from the set.
    *
    * @param elem the element
    */
  def -=(elem: A): Set[A] = set -= elem

  private val set: Set[A] =
    newSetFromMap(new ConcurrentHashMap[A, lang.Boolean](elems.map(k => k -> lang.Boolean.TRUE).toMap.asJava)).asScala

}
