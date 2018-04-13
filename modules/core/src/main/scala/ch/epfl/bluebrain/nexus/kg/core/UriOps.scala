package ch.epfl.bluebrain.nexus.kg.core

import akka.http.scaladsl.model.Uri
import ch.epfl.bluebrain.nexus.commons.iam.acls.Path

object UriOps {

  /**
    * Interface syntax to expose new functionality into [[Uri]].
    *
    * @param uri the instance of a [[Uri]]
    */
  implicit class UriSyntax(uri: Uri) {

    /**
      * Append a ''path'' to the ''uri''
      *
      * @param path the path to append
      */
    def append(path: Path): Uri =
      if (uri.path.isEmpty) uri.withPath(path)
      else
        uri.copy(path = (uri.path: Path) ++ path)
  }
}
