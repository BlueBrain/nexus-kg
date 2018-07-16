package ch.epfl.bluebrain.nexus.kg.resources

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

import ch.epfl.bluebrain.nexus.iam.client.types.Identity
import ch.epfl.bluebrain.nexus.kg.config.Vocabulary.nxv
import ch.epfl.bluebrain.nexus.rdf.Node
import ch.epfl.bluebrain.nexus.rdf.Node.Literal
import ch.epfl.bluebrain.nexus.rdf.Vocabulary._
import com.github.ghik.silencer.silent

object syntax {

  final implicit class ResourceSyntax(resource: ResourceF[_, _, _]) {
    def isSchema: Boolean = resource.types.contains(nxv.Schema.value)
  }

  final implicit def toNode(instant: Instant): Node =
    Literal(instant.atOffset(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT), xsd.dateTime.value)

  @SuppressWarnings(Array("UnusedMethodParameter"))
  //TODO: This is not implemented. Morover, this does not return a Literal,
  //but a IriNode
  final implicit def toNode(@silent identity: Identity): Node =
    Literal(nxv.Anonymous.value.asUri)
}
