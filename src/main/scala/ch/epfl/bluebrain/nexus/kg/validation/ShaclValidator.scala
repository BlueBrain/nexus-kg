package ch.epfl.bluebrain.nexus.kg.validation

import cats.MonadError
import cats.syntax.applicativeError._
import cats.syntax.functor._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.ShaclValidatorErr._
import ch.epfl.bluebrain.nexus.commons.shacl.validator.{ValidationReport, ValidationResult}
import ch.epfl.bluebrain.nexus.rdf.Graph
import ch.epfl.bluebrain.nexus.rdf.syntax.jena._
import es.weso.rdf.jena.RDFAsJenaModel
import es.weso.schema._
import es.weso.shapeMaps.{Info, NonConformant}
import journal.Logger

object ShaclValidator {

  private val logger = Logger[this.type]

  private val triggerMode  = TargetDeclarations.name
  private val schemaEngine = ShaclexSchema.empty.name

  /**
    * Validates ''data'' in its Graph model representation against the specified ''schema''.  It produces a
    * ''ValidationReport'' in the ''F[_]'' context.
    *
    * @param schema the shacl schema instance against which data is validated
    * @param data   the data to be validated
    * @return a ''ValidationReport'' in the ''F[_]'' context
    */
  def validate[F[_]](schema: Graph, data: Graph)(implicit F: MonadError[F, Throwable]): F[ValidationReport] =
    Schemas.fromRDF(RDFAsJenaModel(schema), schemaEngine) match {
      case Right(s) =>
        validateShacl(RDFAsJenaModel(data), s)
          .recoverWith {
            case CouldNotFindImports(missing) =>
              F.pure(ValidationReport(missing.toList.map(imp => ValidationResult(s"Could not load import '$imp'"))))
            case IllegalImportDefinition(values) =>
              F.pure(
                ValidationReport(values.toList.map(imp => ValidationResult(s"The provided import '$imp' is invalid"))))
            case _: FailedToLoadData =>
              F.pure(ValidationReport(List(ValidationResult("The data format is invalid"))))
          }
      case Left(errMessage) => F.raiseError(FailedToLoadData(errMessage))
    }

  private def validateShacl[F[_]](model: RDFAsJenaModel, schema: Schema)(
      implicit F: MonadError[F, Throwable]): F[ValidationReport] =
    F.pure {
      logger.debug("Validating data against schema")
      schema.validate(model, triggerMode, "", None, None)
    } map { result =>
      logger.debug(s"Validation result '$result'")
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
                case None          => ValidationResult("Violation found")
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

}
