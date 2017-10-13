package ch.epfl.bluebrain.nexus.kg.indexing.query

import ch.epfl.bluebrain.nexus.kg.indexing.ConfiguredQualifier
import ch.epfl.bluebrain.nexus.kg.indexing.Qualifier._
import org.apache.jena.arq.querybuilder.ConstructBuilder
import org.apache.jena.graph.NodeFactory

/**
  * A builder of queries for patch graph operations.
  */
object PatchQuery {

  /**
    * Builds a SPARQL query to select the triples to be removed during a patch graph operation.
    *
    * @param id         the identifier
    * @param predicates the collection of predicates
    * @tparam A         the identifier type
    * @return a SPARQL query to select the triples to be removed
    */
  def apply[A: ConfiguredQualifier](id: A, predicates: String*): String = {
    val subj = NodeFactory.createURI(id.qualifyAsString)
    predicates.zipWithIndex
      .foldLeft(new ConstructBuilder) {
        case (builder, (elem, idx)) =>
          val predicate = NodeFactory.createURI(elem)
          val objVar    = NodeFactory.createVariable(s"var$idx")
          builder
            .addConstruct(subj, predicate, objVar)
            .addWhere(subj, predicate, objVar)
      }
      .build
      .serialize()
  }
}
