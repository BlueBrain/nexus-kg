package ch.epfl.bluebrain.nexus.kg.core.resources

/**
  * Enumeration type for all resource types that the platform recognized.
  */
sealed trait ResourceType extends Product with Serializable

object ResourceType {

  /**
    * Schema resource type
    */
  final case object Schema extends ResourceType

  /**
    * Instance resource type
    */
  final case object Instance extends ResourceType

  /**
    * Ontology resource type
    */
  final case object Ontology extends ResourceType

  type SchemaType   = Schema.type
  type InstanceType = Instance.type
  type OntologyType = Ontology.type
}
