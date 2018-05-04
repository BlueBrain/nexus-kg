package ch.epfl.bluebrain.nexus.kg.core.resources

/**
  * Enumeration type for all resource types that the platform recognized.
  *
  * @param name the resource type string value
  */
sealed abstract class ResourceType(val name: String) extends Product with Serializable

object ResourceType {

  /**
    * Schema resource type
    */
  final case object Schema extends ResourceType("schema")

  /**
    * Instance resource type
    */
  final case object Instance extends ResourceType("instance")

  /**
    * Ontology resource type
    */
  final case object Ontology extends ResourceType("ontology")

  type SchemaType   = Schema.type
  type InstanceType = Instance.type
  type OntologyType = Ontology.type

}
