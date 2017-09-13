# Instances
**`/data/{orgId}/{domainId}/{name}/{version}`** path describes schemas inside an specific organization and domain. The following resources are allowed:

## Get
###**`GET /data/{orgId}/{domainId}/{name}/{version}/{id}?rev={rev}`**
**Retrieves** the instance specified in **id** from the schema **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |
| id            | String        | The unique identifier for the instance.                                                                                       |
| rev           | Option[Long]| The revision of the instance you want to retrieve. If not specified, the latest revision will be assumed.                     |


### Response JSON-LD Object
| Field             | Type          | Description                                                                           |
| -------------     |-------------  | ---------------------------------------------                                         |
| @id               | String        | The unique identifier for the instance.                                               |
| rev               | Long          | The current revision of the instance.                                                 |
| originalFileName  | Option[String]| The filename instance attachment, if present.                                         |
| contentType       | Option[String]| The MIME type of the instance attachment, if present.                                 |
| size.value        | Option[String]| The size of the instance attachment, if present.                                      |
| size.unit         | Option[String]| The unit if the size.value field, if present.                                         |
| digest.value      | Option[String]| The value of the digest of the attachment instance, if present                        |
| digest.alg        | Option[String]| The algorithm used to calculate the digest of the attachment instance, if present.    |
| *                 | *             | The payload of the instance.                                                          |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested instance does not exist.

### Example request
```bash
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0"
```


## List
###**`GET /data/{orgId}?deprecated={deprecated}`**
###**`GET /data/{orgId}/{domainId}?deprecated={deprecated}`**
###**`GET /data/{orgId}/{domainId}/{name}?deprecated={deprecated}`**
###**`GET /data/{orgId}/{domainId}/{name}/{version}?deprecated={deprecated}`**

**Retrieves a list** of instances with the specific filters defined on the request URI.

### Request path and query parameters
| Field         | Type                  | Description                                                                                                                       |
| ------------- |-------------          | ---------------------------------------------                                                                                     |  
| orgId         | String                | The organization identifier to which the listing instances belong.                                                                | 
| domainId      | Option[String]        | The domain identifier to which the listing instances belong.                                                                      |
| name          | Option[String]        | The schema name to which the listing instances belong.                                                                            |
| version       | Option[String]        | The schema version to which the listing instances belong.                                                                         |
| deprecated    | Option[Boolean]       | A deprecated filter for the instances you want to list. If not set, it will return both deprecated and not deprecated instances.  |
| *             |                       | [Pagination fields](basics.html#pagination-response).                                                                                  |

### Status Codes
* **200 OK** - Request completed successfully.

### Response JSON-LD Object

The response format is the one defined in [Listing and querying response format](basics.html#listing-and-querying-response-format)


### Examples request
```bash
# Filtering by organization nexus and domain core
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core"

# Filtering by organization nexus, domain core and schema name subject and only return deprecated instances
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject?deprecated=true"

# Filtering by organization nexus, domain core, schema name subject and version v1.0.0 and only return deprecated instances. Return maximim 100 results
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0?deprecated=true&from=0&size=100"
```

## Full text search
###**`GET /data/q={term}`**

**Retrieves a list** of instances that match the provided **term**

### Request path and query parameters
| Field         | Type                  | Description                                                                                                                       |
| ------------- |-------------          | ---------------------------------------------                                                                                     |  
| term          | String                | The term used to match against all property values of the instance.                                                                | 
| *             |                       | [Pagination fields](basics.html#pagination-response).                                                                                  |

### Status Codes
* **200 OK** - Request completed successfully.

### Response JSON-LD Object

The response format is the one defined in [Listing and querying response format](basics.html#listing-and-querying-response-format)


### Examples request
```bash
# Filtering by term subject
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/data?q=subject"
```

## Create
###**`POST /data/{orgId}/{domainId}/{name}/{version}`**
**Creates** the instance to validate against the schema **name & version** from the domain **domainId** inside the organization **orgId** with the provided payload.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |

### Request JSON-LD Object
A valid JSON-LD object. This object will be validated against the provided schema.


### Response JSON-LD Object
| Field             | Type          | Description                                                                           |   
| -------------     |-------------  | ---------------------------------------------                                         |
| @id               | String        | The unique identifier for the instance.                                               |
| rev               | Long          | The current revision of the instance.                                                 |
| originalFileName  | Option[String]| The filename instance attachment, if present.                                         |
| contentType       | Option[String]| The MIME type of the instance attachment, if present.                                 |
| size.value        | Option[String]| The size of the instance attachment, if present.                                      |
| size.unit         | Option[String]| The unit if the size.value field, if present.                                         |
| digest.value      | Option[String]| The value of the digest of the attachment instance, if present                        |
| digest.alg        | Option[String]| The algorithm used to calculate the digest of the attachment instance, if present.    |

### Status Codes
* **201 Created** - Request completed successfully.
* **400 Bad Request**
    * The schema, domain or organization are deprecated.
    * The schema is not published.
    * The instance payload provided does not have valid shape constrains.
* **404 Not Found** - The schema, domain or organization do not exist.

### Example request
```bash
curl -v -X POST -H "Content-Type: application/json" "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0" -d '{"@context": {"@vocab": "https://bbp-nexus.epfl.ch/voc/experiment/core/", "@base": "https://bbp-nexus.epfl.ch/v0/data/bbp/experiment/subject/v1.0.0/", "bbpexp": "https://bbp-nexus.epfl.ch/voc/experiment/core/", "description": "http://schema.org/description", "name": "http://schema.org/name", "weight": "http://schema.org/weight", "unitCode": "http://schema.org/unitCode", "value": "http://schema.org/value", "QuantitativeValue": "http://schema.org/QuantitativeValue", "GenderType": "http://schema.org/GenderType", "label": "http://www.w3.org/2000/01/rdf-schema#label", "providerId": "https://bbp-nexus.epfl.ch/voc/experiment/core/providerId", "Subject": "https://bbp-nexus.epfl.ch/voc/experiment/core/Subject", "Age": "https://bbp-nexus.epfl.ch/voc/experiment/core/Age", "period": "https://bbp-nexus.epfl.ch/voc/experiment/core/period", "strain": "https://bbp-nexus.epfl.ch/voc/experiment/core/strain", "species": "https://bbp-nexus.epfl.ch/voc/experiment/core/species"}, "@type": "bbpexp:Subject", "@id": "eb36bc14-5718-11e7-907b-a6006ad3dba0", "providerId": "00826165", "name": "00826165", "description": "Rat used for recording on 20160810 by Jane Yi in LNMC", "species": {"@id": "http://purl.obolibrary.org/obo/NCBITaxon_10116", "label": "Rattus norvegicus"}, "sex": {"@type": "GenderType", "value": "female"}, "weight": {"@type": "QuantitativeValue", "value": 38.1, "unitCode": {"@id": "https://bbp-nexus.epfl.ch/voc/unit/GRM", "label": "g"} }, "strain": {"@id": "http://www.hbp.FIXME.org/hbp_taxonomy_ontology/0000008", "label": "Wistar Han"} }'
```

## Update
###**`PUT /data/{orgId}/{domainId}/{name}/{version}/{id}?rev={rev}`**
**Updates** the instance **id** from the schema **name & version** from the domain **domainId** inside the organization **orgId** overriding the existing payload with the provided one.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |
| id            | String        | The unique identifier for the instance.                                                                                       |
| rev           | Long          | The current revision of the instance.                                                                                         |


### Request JSON-LD Object
A valid JSON-LD object. This object will be validated against the provided schema.


### Response JSON-LD Object
| Field             | Type          | Description                                                                           |   
| -------------     |-------------  | ---------------------------------------------                                         |
| @id               | String        | The unique identifier for the instance.                                               |
| rev               | Long          | The current revision of the instance.                                                 |
| originalFileName  | Option[String]| The filename instance attachment, if present.                                         |
| contentType       | Option[String]| The MIME type of the instance attachment, if present.                                 |
| size.value        | Option[String]| The size of the instance attachment, if present.                                      |
| size.unit         | Option[String]| The unit if the size.value field, if present.                                         |
| digest.value      | Option[String]| The value of the digest of the attachment instance, if present                        |
| digest.alg        | Option[String]| The algorithm used to calculate the digest of the attachment instance, if present.    |

### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request**
    * The instance, schema, domain or organization are deprecated.
    * The instance payload provided does not have valid shape constrains.
* **404 Not Found** - The instance, schema, domain or organization do not exist.
* **409 Conflict** - The provided rev query parameter does not match the current revision.

### Example request
```bash
curl -v -X PUT -H "Content-Type: application/json" "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0/ce97bb62-7336-4449-93e2-3e311e9e960d?rev=1" -d '{"@context": {"@vocab": "https://bbp-nexus.epfl.ch/voc/experiment/core/", "@base": "https://bbp-nexus.epfl.ch/v0/data/bbp/experiment/subject/v1.0.0/", "bbpexp": "https://bbp-nexus.epfl.ch/voc/experiment/core/", "description": "http://schema.org/description", "name": "http://schema.org/name", "weight": "http://schema.org/weight", "unitCode": "http://schema.org/unitCode", "value": "http://schema.org/value", "QuantitativeValue": "http://schema.org/QuantitativeValue", "GenderType": "http://schema.org/GenderType", "label": "http://www.w3.org/2000/01/rdf-schema#label", "providerId": "https://bbp-nexus.epfl.ch/voc/experiment/core/providerId", "Subject": "https://bbp-nexus.epfl.ch/voc/experiment/core/Subject", "Age": "https://bbp-nexus.epfl.ch/voc/experiment/core/Age", "period": "https://bbp-nexus.epfl.ch/voc/experiment/core/period", "strain": "https://bbp-nexus.epfl.ch/voc/experiment/core/strain", "species": "https://bbp-nexus.epfl.ch/voc/experiment/core/species"}, "@type": "bbpexp:Subject", "@id": "eb36bc14-5718-11e7-907b-a6006ad3dba0", "providerId": "00826165", "name": "00826165", "description": "Rat used for recording on 20160810 by Jane Yi in LNMC", "species": {"@id": "http://purl.obolibrary.org/obo/NCBITaxon_10116", "label": "Rattus norvegicus"}, "sex": {"@type": "GenderType", "value": "female"}, "weight": {"@type": "QuantitativeValue", "value": 38.1, "unitCode": {"@id": "https://bbp-nexus.epfl.ch/voc/unit/GRM", "label": "g"} }, "strain": {"@id": "http://www.hbp.FIXME.org/hbp_taxonomy_ontology/0000008", "label": "Another label"} }'
```


## Deprecate
###**`DELETE /data/{orgId}/{domainId}/{name}/{version}/{id}?rev={rev}`**
**Deprecates** the instance **id** from the schema **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |
| id            | String        | The unique identifier for the instance.                                                                                       |
| rev           | Long          | The current revision of the instance.                                                                                         |

### Response JSON-LD Object
| Field             | Type          | Description                                                                           |   
| -------------     |-------------  | ---------------------------------------------                                         |
| @id               | String        | The unique identifier for the instance.                                               |
| rev               | Long          | The current revision of the instance.                                                 |
| originalFileName  | Option[String]| The filename instance attachment, if present.                                         |
| contentType       | Option[String]| The MIME type of the instance attachment, if present.                                 |
| size.value        | Option[String]| The size of the instance attachment, if present.                                      |
| size.unit         | Option[String]| The unit if the size.value field, if present.                                         |
| digest.value      | Option[String]| The value of the digest of the attachment instance, if present                        |
| digest.alg        | Option[String]| The algorithm used to calculate the digest of the attachment instance, if present.    |

### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request** - The instance, schema, domain or organization are deprecated.
* **404 Not Found** - The instance, schema, domain or organization do not exist.
* **409 Conflict** - The provided rev query parameter does not match the current revision.

### Example request
```bash
curl -v -X DELETE "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0/ce97bb62-7336-4449-93e2-3e311e9e960d?rev=2"
```

## Get attachment
###**`GET /data/{orgId}/{domainId}/{name}/{version}/{id}/attachment?rev={rev}`**
**Retrieves** the instance attachment from the instance **id** from the schema **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |
| rev           | Option[Long]| The revision of the instance attachment you want to retrieve. If not specified, the latest revision will be assumed.          |


### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested instance attachment does not exist.

### Example request
```bash
curl "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0/ce97bb62-7336-4449-93e2-3e311e9e960d/attachment?rev=2" -o output.png
```

## Create attachment
###**`PUT /data/{orgId}/{domainId}/{name}/{version}/{id}/attachment?rev={rev}`**
**Creates** the instance attachment from the instance **id** from the schema **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |
| rev           | Long]         | The current revision of the instance.          |

### Multipart request

*Content-Type*: application/json

It should contain one of the multipart types as:
```
Content-Disposition: form-data; name="file"; filename="{FILENAME}"
Content-Type: {VALID_MIME_TYPE}
```

### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request** - The instance, schema, domain or organization are deprecated.
* **404 Not Found** - The instance, schema, domain or organization do not exist.
* **409 Conflict** - The provided rev query parameter does not match the current revision.

### Example request
```bash
curl -v -X PUT -F "filename={filename}" -F "file=@/path/to/file/name" "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0/ce97bb62-7336-4449-93e2-3e311e9e960d?rev=2"
```

## Deprecates attachment
###**`DELETE /data/{orgId}/{domainId}/{name}/{version}/{id}/attachment?rev={rev}`**
**Deprecates** the instance attachment from the instance **id** from the schema **name & version** from the domain **domainId** inside the organization **orgId**.

@@@ note

This does not delete the binary attached to an instance, although it prevents from fetching it from the latest review. The binary attached will be still available fetching an older review.

@@@

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/)  |
| rev           | Long]         | The current revision of the instance.                                                                                         |


### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request** - The instance, schema, domain or organization are deprecated.
* **404 Not Found** - The instance, schema, domain or organization do not exist.
* **409 Conflict** - The provided rev query parameter does not match the current revision.

### Example request
```bash
curl -v -X DELETE "https://bbp-nexus.epfl.ch/{environment}/{version}/data/nexus/core/subject/v1.0.0/ce97bb62-7336-4449-93e2-3e311e9e960d?rev=3"
```