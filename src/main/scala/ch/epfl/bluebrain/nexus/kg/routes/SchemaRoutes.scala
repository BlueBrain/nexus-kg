package ch.epfl.bluebrain.nexus.kg.routes

import cats.data.EitherT
import ch.epfl.bluebrain.nexus.iam.client.Caller
import ch.epfl.bluebrain.nexus.iam.client.types._
import ch.epfl.bluebrain.nexus.kg.async.DistributedCache
import ch.epfl.bluebrain.nexus.kg.config.AppConfig
import ch.epfl.bluebrain.nexus.kg.config.Contexts._
import ch.epfl.bluebrain.nexus.kg.config.Schemas._
import ch.epfl.bluebrain.nexus.kg.directives.LabeledProject
import ch.epfl.bluebrain.nexus.kg.resources._
import ch.epfl.bluebrain.nexus.kg.routes.ResourceRoutes.Schemed
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.syntax.circe.context._
import io.circe.Json
import monix.eval.Task

class SchemaRoutes private[routes] (resources: Resources[Task], acls: FullAccessControlList, caller: Caller)(
    implicit wrapped: LabeledProject,
    cache: DistributedCache[Task],
    indexers: Clients[Task],
    config: AppConfig)
    extends Schemed(resources, shaclSchemaUri, "schemas", acls, caller) {

  override def transformCreate(j: Json): Json =
    j.addContext(shaclCtxUri)

  override def transformUpdate(id: AbsoluteIri, j: Json): EitherT[Task, Rejection, Json] =
    EitherT.rightT(transformCreate(j))

}
