package ch.epfl.bluebrain.nexus.kg.config

import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.rdf.Iri
import ch.epfl.bluebrain.nexus.rdf.Iri.AbsoluteIri
import ch.epfl.bluebrain.nexus.rdf.Node.IriNode
import ch.epfl.bluebrain.nexus.rdf.syntax.node.unsafe._

/**
  * Constant vocabulary values
  */
object Vocabulary {

  /**
    * Nexus vocabulary.
    */
  object nxv {
    val base: Iri.AbsoluteIri          = url"https://bluebrain.github.io/nexus/vocabulary/".value
    private[Vocabulary] implicit val _ = IriNode(base)

    /**
      * @param suffix the segment to suffix to the base
      * @return an [[IriNode]] composed by the ''base'' plus the provided ''suffix''
      */
    def withSuffix(suffix: String): IriNode = IriNode(base + suffix)

    // Metadata vocabulary
    val rev           = PrefixMapping.metadata("rev")
    val deprecated    = PrefixMapping.metadata("deprecated")
    val createdAt     = PrefixMapping.metadata("createdAt")
    val updatedAt     = PrefixMapping.metadata("updatedAt")
    val createdBy     = PrefixMapping.metadata("createdBy")
    val updatedBy     = PrefixMapping.metadata("updatedBy")
    val constrainedBy = PrefixMapping.metadata("constrainedBy")
    val self          = PrefixMapping.metadata("self")
    val project       = PrefixMapping.metadata("project")
    val total         = PrefixMapping.metadata("total")
    val results       = PrefixMapping.metadata("results")
    val maxScore      = PrefixMapping.metadata("maxScore")
    val score         = PrefixMapping.metadata("score")
    val uuid          = PrefixMapping.metadata("uuid")
    val instant       = PrefixMapping.metadata("instant")
    val eventSubject  = PrefixMapping.metadata("subject")
    val resourceId    = PrefixMapping.metadata("resourceId")
    val projectUuid   = PrefixMapping.metadata("projectUuid")

    // File metadata vocabulary
    val filename  = PrefixMapping.metadata("filename")
    val digest    = PrefixMapping.metadata("digest")
    val algorithm = PrefixMapping.metadata("algorithm")
    val value     = PrefixMapping.metadata("value")
    val bytes     = PrefixMapping.metadata("bytes")
    val mediaType = PrefixMapping.metadata("mediaType")
    val storageId = PrefixMapping.metadata("storageId")

    // ElasticSearch sourceAsText predicate
    val originalSource = PrefixMapping.metadata("original_source")

    // Tagging resource payload vocabulary
    val tag  = PrefixMapping.prefix("tag")
    val tags = PrefixMapping.prefix("tags")

    // Resolvers payload vocabulary
    val priority      = PrefixMapping.prefix("priority")
    val resourceTypes = PrefixMapping.prefix("resourceTypes")
    val projects      = PrefixMapping.prefix("projects")
    val identities    = PrefixMapping.prefix("identities")
    val realm         = PrefixMapping.prefix("realm")
    val subject       = PrefixMapping.prefix("subject")
    val group         = PrefixMapping.prefix("group")

    // View payload vocabulary
    val resourceSchemas = PrefixMapping.prefix("resourceSchemas")
    val resourceTag     = PrefixMapping.prefix("resourceTag")
    val includeMetadata = PrefixMapping.prefix("includeMetadata")
    val sourceAsText    = PrefixMapping.prefix("sourceAsText")
    val mapping         = PrefixMapping.prefix("mapping")
    val views           = PrefixMapping.prefix("views")
    val viewId          = PrefixMapping.prefix("viewId")

    //Storage payload vocabulary
    val default         = PrefixMapping.prefix("default")
    val volume          = PrefixMapping.prefix("volume")
    val readPermission  = PrefixMapping.prefix("readPermission")
    val writePermission = PrefixMapping.prefix("writePermission")

    // S3 storage payload vocabulary
    val bucket    = PrefixMapping.prefix("bucket")
    val endpoint  = PrefixMapping.prefix("endpoint")
    val region    = PrefixMapping.prefix("region")
    val accessKey = PrefixMapping.prefix("accessKey")
    val secretKey = PrefixMapping.prefix("secretKey")

    // File link payload vocabulary
    val location = PrefixMapping.prefix("location")

    // View default ids
    val defaultElasticSearchIndex = PrefixMapping.prefix("defaultElasticSearchIndex")
    val defaultSparqlIndex        = PrefixMapping.prefix("defaultSparqlIndex")

    //Resolver default id
    val defaultResolver = PrefixMapping.prefix("defaultInProject")

    //Storage default id
    val defaultStorage = PrefixMapping.prefix("diskStorageDefault")

    // @type platform ids
    val Schema                     = PrefixMapping.prefix("Schema")
    val File                       = PrefixMapping.prefix("File")
    val Resource                   = PrefixMapping.prefix("Resource")
    val Ontology                   = PrefixMapping.prefix("Ontology")
    val Resolver                   = PrefixMapping.prefix("Resolver")
    val InProject                  = PrefixMapping.prefix("InProject")
    val CrossProject               = PrefixMapping.prefix("CrossProject")
    val Storage                    = PrefixMapping.prefix("Storage")
    val DiskStorage                = PrefixMapping.prefix("DiskStorage")
    val S3Storage                  = PrefixMapping.prefix("S3Storage")
    val View                       = PrefixMapping.prefix("View")
    val ElasticSearchView          = PrefixMapping.prefix("ElasticSearchView")
    val SparqlView                 = PrefixMapping.prefix("SparqlView")
    val AggregateElasticSearchView = PrefixMapping.prefix("AggregateElasticSearchView")
    val AggregateSparqlView        = PrefixMapping.prefix("AggregateSparqlView")
    val User                       = PrefixMapping.prefix("User")
    val Group                      = PrefixMapping.prefix("Group")
    val Authenticated              = PrefixMapping.prefix("Authenticated")
    val Anonymous                  = PrefixMapping.prefix("Anonymous")
    val AccessControlList          = PrefixMapping.prefix("AccessControlList")
  }

  /**
    * Prefix mapping.
    *
    * @param prefix the prefix associated to this term, used in the Json-LD context
    * @param value  the fully expanded [[AbsoluteIri]] to what the ''prefix'' resolves
    */
  final case class PrefixMapping(prefix: String, value: AbsoluteIri)

  object PrefixMapping {

    /**
      * Constructs a [[PrefixMapping]] vocabulary term from the given ''base'' and the provided ''lastSegment'' with an '_'.
      *
      * @param lastSegment the last segment to append to the ''base'' to build the metadata vocabulary term
      */
    def metadata(lastSegment: String)(implicit base: IriNode): PrefixMapping =
      PrefixMapping("_" + lastSegment, url"${base.value.show + lastSegment}".value)

    /**
      * Constructs a [[PrefixMapping]] vocabulary term from the given ''base'' and the provided ''lastSegment''.
      *
      * @param lastSegment the last segment to append to the ''base'' to build the vocabulary term
      */
    def prefix(lastSegment: String): PrefixMapping = new PrefixMapping(lastSegment, nxv.withSuffix(lastSegment).value)

    implicit def prefixMappingIri(m: PrefixMapping): IriNode               = IriNode(m.value)
    implicit def prefixMappingAbsoluteIri(m: PrefixMapping): AbsoluteIri   = m.value
    implicit def prefixMappingToIriF(p: PrefixMapping): IriNode => Boolean = _ == IriNode(p.value)
    implicit val prefixMappingShow: Show[PrefixMapping]                    = Show.show(_.value.show)
  }
}
