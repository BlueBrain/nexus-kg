package ch.epfl.bluebrain.nexus.kg.core

import java.net.URLEncoder
import java.util.regex.Pattern

import akka.http.scaladsl.model.Uri
import cats.Show
import cats.syntax.show._
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path
import ch.epfl.bluebrain.nexus.kg.core.contexts.{ContextId, ContextName}
import ch.epfl.bluebrain.nexus.kg.core.domains.DomainId
import ch.epfl.bluebrain.nexus.kg.core.instances.InstanceId
import ch.epfl.bluebrain.nexus.kg.core.organizations.OrgId
import ch.epfl.bluebrain.nexus.kg.core.schemas.shapes.ShapeId
import ch.epfl.bluebrain.nexus.kg.core.schemas.{SchemaId, SchemaName}

import scala.util.Try

/**
  * Typeclass definition for values that can be fully qualified using a base uri.
  *
  * @tparam A the value type
  */
trait Qualifier[A] {

  /**
    * Computes the fully qualified ''Uri'' of the value of type ''A'' using the argument ''base'' uri.
    *
    * @param value the value to qualify
    * @param base  the base uri against which the value is qualified
    * @return a fully qualified ''Uri''
    */
  def apply(value: A, base: Uri): Uri

  /**
    * Attempts to compute the value of type ''A'' from a fully qualified ''Uri'' using the argument ''base'' uri.
    *
    * @param uri  the fully qualifier ''Uri''
    * @param base the base uri against which the fully qualified ''Uri'' is unqualified
    * @return an option of the id of type A
    */
  def unapply(uri: Uri, base: Uri): Option[A]

}

/**
  * Typeclass definition for values that can be fully qualified using a preconfigured base uri.
  *
  * @tparam A the value type
  */
trait ConfiguredQualifier[A] {

  /**
    * Computes the fully qualified ''Uri'' of the value of type ''A'' using a preconfigured ''base'' uri.
    *
    * @param value the value to qualify
    * @return a fully qualified ''Uri''
    */
  def apply(value: A): Uri

  /**
    * Attempts to compute the value of type ''A'' from a fully qualified ''Uri''
    *
    * @param uri the fully qualifier ''Uri''
    * @return an option of the id of type A
    */
  def unapply(uri: Uri): Option[A]
}

object Qualifier extends QualifierInstances {

  /**
    * Constructs a ''ConfiguredQualifier[A]'' from an explicit ''base'' uri and an implicitly available
    * ''Qualifier[A]''.
    *
    * @param base the base uri to use when qualifying values of type ''A''
    * @return a ''ConfiguredQualifier[A]''
    */
  final def configured[A](base: Uri)(implicit Q: Qualifier[A]): ConfiguredQualifier[A] =
    new ConfiguredQualifier[A] {
      override def apply(value: A): Uri = Q(value, base)

      override def unapply(uri: Uri): Option[A] = Q.unapply(uri, base)
    }

  implicit class ToQualifierOps[A](value: A)(implicit Q: Qualifier[A]) {

    /**
      * Qualifies the value against the argument ''base'' Uri.
      *
      * @param base the base uri against which the value is qualified
      * @return a fully qualified ''Uri''
      */
    def qualifyWith(base: Uri): Uri =
      Q(value, base)

    /**
      * Qualifies the value against the argument ''base'' Uri.
      *
      * @param base the base uri against which the value is qualified
      * @return a fully qualified ''Uri'' in a string format
      */
    def qualifyAsStringWith(base: Uri): String =
      qualifyWith(base).toString()
  }

  implicit class ToConfiguredQualifierOps[A](value: A)(implicit Q: ConfiguredQualifier[A]) {

    /**
      * Qualifies the value using a preconfigured ''base'' uri.
      *
      * @return a fully qualified ''Uri''
      */
    def qualify: Uri =
      Q(value)

    /**
      * Qualifies the value using a preconfigured ''base'' uri.
      *
      * @return a fully qualified ''Uri'' in a string format
      */
    def qualifyAsString: String =
      qualify.toString()
  }

  implicit class UnqualifierOps(uri: Uri) {

    /**
      * Unqualifies the value against the argument ''base'' Uri.
      *
      * @param base the base uri against which the fully qualified uri is attempted to be unqualified
      * @return an option of the id of type A
      */
    def unqualifyWith[A](base: Uri)(implicit Q: Qualifier[A]): Option[A] =
      Q.unapply(uri, base)
  }

  implicit class ToUnqualifiedStringOps(uriString: String) {

    /**
      * Unqualifies the value against the argument ''base'' Uri.
      *
      * @param base the base uri against which the fully qualified uri is attempted to be unqualified
      * @return an option of the id of type A
      */
    def unqualifyWith[A](base: Uri)(implicit Q: Qualifier[A]): Option[A] =
      Try(Uri(uriString)).toOption.flatMap(Q.unapply(_, base))
  }

  implicit class ToConfiguredUnqualifiedStringOps(uriString: String) {

    /**
      * Unqualifies the fully qualified string uri using a preconfigured ''base'' uri.
      *
      * @return a fully qualified ''Uri''
      */
    def unqualify[A](implicit Q: ConfiguredQualifier[A]): Option[A] = {
      Try(Uri(uriString)).toOption.flatMap(Q.unapply)
    }
  }
}

trait QualifierInstances {

  private def removeBaseUri(uri: Uri, base: Uri, path: Option[String] = None) =
    uri
      .toString()
      .replaceAll(Pattern.quote(s"$base/${path.map(p => s"$p/").getOrElse("")}"), "")

  implicit val domainIdQualifier: Qualifier[DomainId] =
    new Qualifier[DomainId] {
      override def apply(value: DomainId, base: Uri): Uri =
        Uri(s"$base/domains/${encode(value.show)}")

      override def unapply(uri: Uri, base: Uri): Option[DomainId] =
        Try {
          val parts = removeBaseUri(uri, base).split('/')
          DomainId(OrgId(parts(1)), parts(2))
        }.toOption
    }

  implicit val orgIdQualifier: Qualifier[OrgId] = new Qualifier[OrgId] {
    override def apply(value: OrgId, base: Uri): Uri =
      Uri(s"$base/organizations/${encode(value.show)}")

    override def unapply(uri: Uri, base: Uri): Option[OrgId] =
      Try { OrgId(removeBaseUri(uri, base, Some("organizations"))) }.toOption
  }

  implicit val schemaNameQualifier: Qualifier[SchemaName] =
    new Qualifier[SchemaName] {
      override def apply(value: SchemaName, base: Uri): Uri =
        Uri(s"$base/schemas/${encode(value.show)}")
      override def unapply(uri: Uri, base: Uri): Option[SchemaName] =
        SchemaName(removeBaseUri(uri, base, Some("schemas")))
    }

  implicit val schemaIdQualifier: Qualifier[SchemaId] =
    new Qualifier[SchemaId] {
      override def apply(value: SchemaId, base: Uri): Uri =
        Uri(s"$base/schemas/${encode(value.show)}")
      override def unapply(uri: Uri, base: Uri): Option[SchemaId] =
        SchemaId(removeBaseUri(uri, base, Some("schemas"))).toOption
    }

  implicit val shapeIdQualifier: Qualifier[ShapeId] = new Qualifier[ShapeId] {
    override def apply(value: ShapeId, base: Uri): Uri =
      Uri(s"$base/schemas/${encode(value.show)}")

    override def unapply(uri: Uri, base: Uri): Option[ShapeId] = {
      val schemaIdUri = Uri(s"$uri".substring(0, s"$uri".indexOf("/shapes")))
      schemaIdQualifier
        .unapply(schemaIdUri, base)
        .map(ShapeId(_, s"$uri".substring(s"$uri".lastIndexOf('/') + 1)))
    }
  }

  implicit val instanceIdQualifier: Qualifier[InstanceId] =
    new Qualifier[InstanceId] {
      override def apply(value: InstanceId, base: Uri): Uri =
        Uri(s"$base/data/${encode(value.show)}")
      override def unapply(uri: Uri, base: Uri): Option[InstanceId] =
        InstanceId(removeBaseUri(uri, base, Some("data"))).toOption
    }

  implicit val contextNameQualifier: Qualifier[ContextName] =
    new Qualifier[ContextName] {
      override def apply(value: ContextName, base: Uri): Uri =
        Uri(s"$base/contexts/${encode(value.show)}")
      override def unapply(uri: Uri, base: Uri): Option[ContextName] =
        ContextName(removeBaseUri(uri, base, Some("contexts")))
    }

  implicit val contextIdQualifier: Qualifier[ContextId] =
    new Qualifier[ContextId] {
      override def apply(value: ContextId, base: Uri): Uri =
        Uri(s"$base/contexts/${encode(value.show)}")
      override def unapply(uri: Uri, base: Uri): Option[ContextId] =
        ContextId(removeBaseUri(uri, base, Some("contexts"))).toOption
    }

  implicit def stringIdQualifier(implicit S: Show[String]): Qualifier[String] =
    new Qualifier[String] {
      override def apply(value: String, base: Uri): Uri =
        if (base.isEmpty) value.show
        else base.copy(path = (base.path: Path) ++ Path(value.show))

      override def unapply(uri: Uri, base: Uri): None.type = None
    }
  private def encode(value: String): String =
    value.split("/").map(v => Try(URLEncoder.encode(v, "UTF-8")).getOrElse(v)).mkString("/")
}
