package ch.epfl.bluebrain.nexus.kg.indexing.acls

import akka.http.scaladsl.model.Uri

/**
  * Collection of configurable settings specific to ACL indexing.
  *
  * @param index          the name of the index
  * @param aclBase        the application base uri for operating on ACLs
  * @param aclBaseNs      the ACL base context
  * @param nexusVocBase   the nexus core vocabulary base
  */
final case class AclIndexingSettings(index: String, aclBase: Uri, aclBaseNs: Uri, nexusVocBase: Uri)
