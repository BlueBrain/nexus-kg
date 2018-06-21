package ch.epfl.bluebrain.nexus.kg.validation

import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.syntax.jena._
import es.weso.rdf.jena.RDFAsJenaModel
import es.weso.schema._
import es.weso.shapeMaps.{Info, NonConformant}

/**
  * ShaclValidator implementation based on ''es.weso.schema'' validator.
  */
object Validator {

  private val triggerMode  = TargetDeclarations.name
  private val schemaEngine = ShaclexSchema.empty.name

  /**
    * Validates the ''data'' graph against the specified collection of shapes represented as a graph.
    *
    * @param shapes the collection of shapes against which the data is validated
    * @param data   the data to be validated
    * @return a report describing the validation outcome
    */
  def validate(shapes: Graph, data: Graph): ValidationReport = {
    val report = for {
      schema <- Schemas.fromRDF(RDFAsJenaModel(shapes), schemaEngine)
      res = schema.validate(RDFAsJenaModel(data), triggerMode, "", None, None)
      rep = toReport(res)
    } yield rep
    report.fold(err => ValidationReport(List(ValidationResult(err))), identity)
  }

  private def toReport(result: Result): ValidationReport = {
    if (!result.isValid) {
      ValidationReport(result.errors.map(err => ValidationResult(err.msg)).toList)
    } else if (result.shapeMaps.forall(_.noSolutions)) {
      ValidationReport(List(ValidationResult("No data was selected for validation")))
    } else {
      findViolations(result) match {
        case Nil => ValidationReport(Nil)
        case violations =>
          ValidationReport(violations.map {
            _.reason match {
              case None          => ValidationResult("Unknown violation")
              case Some(message) => ValidationResult(message)
            }
          })
      }
    }
  }

  private def findViolations(result: Result): List[Info] = {
    for {
      resultMaps <- result.shapeMaps
      infoMap    <- resultMaps.resultMap.values
      (_, info)  <- infoMap
      if info.status == NonConformant
    } yield info
  }.toList

  /**
    * Data type that represents the outcome of validating data against a shacl schema.  A non empty list of validation
    * results implies that the data does not conform to the schema.
    *
    * @param result a collection of validation results that describe constraint violations
    */
  final case class ValidationReport(result: List[ValidationResult]) {

    /**
      * Whether the data conforms to the schema.
      */
    lazy val conforms: Boolean = result.isEmpty
  }

  /**
    * A ''ValidationResult'' describes a constraint violation resulting from validating data against a shacl schema.
    *
    * @param reason a descriptive reason as to why the violation occurred
    */
  final case class ValidationResult(reason: String)
}
