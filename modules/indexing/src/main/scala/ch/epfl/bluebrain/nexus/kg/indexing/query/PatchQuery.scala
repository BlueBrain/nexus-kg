package ch.epfl.bluebrain.nexus.kg.indexing.query

import akka.http.scaladsl.model.Uri
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
    * @param graph      the ''graph'' to be targeted
    * @param predicates the collection of predicates
    * @tparam A the identifier type
    * @return a SPARQL query to select the triples to be removed
    */
  def apply[A](id: A, graph: Uri, predicates: String*)(implicit C: ConfiguredQualifier[A]): String = {
    val subj     = NodeFactory.createURI(id.qualifyAsString)
    val graphUri = NodeFactory.createURI(graph.toString())
    val (construct, where) = predicates.zipWithIndex
      .foldLeft((new ConstructBuilder, new ConstructBuilder)) {
        case ((builderConstruct, builderWhere), (elem, idx)) =>
          val predicate = NodeFactory.createURI(elem)
          val objVar    = NodeFactory.createVariable(s"var$idx")
          (builderConstruct
             .addConstruct(subj, predicate, objVar),
           builderWhere.addOptional(subj, predicate, objVar))
      }
    construct
      .addGraph(graphUri, where)
      .build
      .serialize()
  }

  /**
    * Builds a SPARQL query to select all the triples of a graph except the ones with the provided ''predicates''.
    *
    * @param graph      the ''graph'' to be targeted
    * @param predicates the collection of predicates to be kept
    * @return a SPARQL query to select the triples to be removed
    */
  def inverse(graph: Uri, predicates: String*): String = {
    val graphUri = NodeFactory.createURI(graph.toString())
    val filter   = predicates.map(p => s"?p != <${NodeFactory.createURI(p)}>").mkString(" && ")
    new ConstructBuilder()
      .addConstruct("?s", "?p", "?o")
      .addGraph(graphUri, new ConstructBuilder().addWhere("?s", "?p", "?o").addFilter(filter))
      .build
      .serialize()
  }

  /**
    * Builds a SPARQL query to select the triples to be removed during a patch graph operation.
    *
    * @param id          the identifier
    * @param predAndObj the collection of predicates and values
    * @return a SPARQL query to select the triples to be removed
    */
  def exactMatch(id: String, predAndObj: (String, String)*): String = {
    val subj = NodeFactory.createURI(id)
    predAndObj
      .foldLeft(new ConstructBuilder) {
        case (builder, (predicate, obj)) =>
          builder
            .addConstruct(subj, NodeFactory.createURI(predicate), NodeFactory.createURI(obj))
      }
      .build
      .serialize()
  }
}
