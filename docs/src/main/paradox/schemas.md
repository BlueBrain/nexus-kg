# Schemas
**`/schemas/{orgId}`** path describes schemas inside an specific organization. The following resources are allowed:

## Get
###**`GET /schemas/{orgId}/{domainId}/{name}/{version}`**
**Retrieves** the schema specified in **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization.                                                                                   | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/). |


### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the schema.         |
| rev           | Long          | The current revision of the schema.           |
| deprecated    | Boolean       | whether the schema is deprecated or not.      |
| published     | Boolean       | whether the schema is deprecated or not.      |
| *             | *             | The payload of the schema.                    |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested schema does not exist.

### Example request
```bash
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/organizations/nexus/domains/core"
```

## List
###**`GET /schemas/{orgId}?deprecated={deprecated}&published={published}`**
###**`GET /schemas/{orgId}/{domainId}?deprecated={deprecated}&published={published}`**
###**`GET /schemas/{orgId}/{domainId}/{name}?deprecated={deprecated}&published={published}`**

**Retrieves a list** of schemas with the specific filters defined on the request URI.

### Request path and query parameters
| Field         | Type                  | Description                                                                                                                   |
| ------------- |-------------          | ---------------------------------------------                                                                                 |
| orgId         | String                | The organization identifier to which the listing schemas belong.                                                              | 
| domainId      | Option[String]        | The domain identifier to which the listing schemas belong.                                                                    |
| name          | Option[String]        | The schema name to which the listing schemas belong.                                                                          |
| deprecated    | Option[Boolean]       | A deprecated filter for the schemas you want to list. If not set, it will return both deprecated and not deprecated schemas.  |
| published     | Option[Boolean]       | A published filter for the schemas you want to list. If not set, it will return both published and unpublished schemas.       |
| *             |                       | [Pagination fields](basics.html#pagination-response).                                                                              |

### Status Codes
* **200 OK** - Request completed successfully.

### Response JSON-LD Object

The response format is the one defined in [Listing and querying response format](basics.html#listing-and-querying-response-format)

### Status Codes
* **200 OK** - Request completed successfully.

### Examples request
```bash
# Filtering by organization nexus and domain core
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core"

# Filtering by organization nexus, domain core and schema name subject and only return deprecated schemas
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject?deprecated=true"

# Filtering by organization nexus, domain core and schema name subject and only return deprecated schemas. Return maximim 100 results
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject?deprecated=true&from=0&size=100"
```

## Create
###**`PUT /schemas/{orgId}/{domainId}/{name}/{version}`**
**Creates** the schema specified in **name & version** from the domain **domainId** inside the organization **orgId** with the provided payload.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization.                                                                                   | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain. It has to match the following regex: ([a-zA-Z0-9]+)          |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/). |

### Request JSON-LD Object
A valid JSON-LD object


### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the schema.         |
| rev           | Long          | The current revision of the schema.           |


### Status Codes
* **201 Created** - Request completed successfully.
* **400 Bad Request**
    * The provided id path parameter does not match the regex ([a-zA-Z0-9]+) or the version does not follow the [semantic format](http://semver.org/).
    * The domain or organization are deprecated.
    * The schema payload provided does not have valid shape constrains.
    * The schema payload provided imports a shape or schema which does not exist.
    * The schema payload provided imports a shape or schema which is not allowed to import.
* **409 Conflict** - The requested schema already exists.
* **404 Not Found**
    * The domain or organization belonging to this schema does not exist.

### Example request
```bash
curl -v -X PUT -H "Content-Type: application/json" "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject/v1.0.0" -d '{"@type": "owl:Ontology", "shapes": [{"@id": "bbpexpshape:AgeShape", "@type": "sh:NodeShape", "property": [{"@id": "_:b6", "sh:in": {"@list": ["Pre-natal", "Post-natal"] }, "maxCount": "1", "minCount": "1", "name": "Period", "node": [], "path": "bbpexp:period"}, {"@id": "_:b5", "class": "schema:QuantitativeValue", "maxCount": "1", "minCount": "1", "name": "Age", "node": "bbpexpshape:AgeValueShape", "path": "schema:value"} ] }, {"@id": "bbpexpshape:AgeValueShape", "@type": "sh:NodeShape", "property": [{"@id": "_:b2", "maxCount": "1", "minCount": "1", "name": "Unit Code", "node": [], "nodeKind": "sh:IRI", "path": "schema:unitCode"}, {"@id": "_:b4", "datatype": "xsd:double", "maxCount": "1", "minCount": "1", "name": "Age value", "node": [], "path": "schema:value"} ] }, {"@id": "bbpexpshape:SubjectShape", "@type": "sh:NodeShape", "description": "Subject (used in neuroscience experiment) shape definition. A subject should have an IRI as identifier.", "nodeKind": "sh:IRI", "property": [{"@id": "_:b8", "class": "bbpexp:Age", "description": "The subject age.", "maxCount": "1", "minCount": "0", "name": "Age", "node": "bbpexpshape:AgeShape", "path": "bbpexp:age"}, {"@id": "_:b9", "description": "Subject disease.", "maxCount": "1", "name": "Disease", "node": [], "path": "bbpexp:disease"}, {"@id": "_:b1", "description": "The species of the subject.", "maxCount": "1", "minCount": "1", "name": "Species", "node": [], "nodeKind": "sh:IRI", "path": "bbpexp:species"}, {"description": "Subject Transgenic.", "maxCount": "1", "name": "Transgenic", "node": [], "nodeKind": "sh:IRI", "path": "bbpexp:transgenic"}, {"@id": "_:b11", "description": "Description of the subject.", "name": "Description", "node": [], "path": "schema:description"}, {"@id": "_:b12", "description": "Laboratory/team internal name of the subject.", "name": "Name", "node": [], "path": "schema:name"}, {"@id": "_:b13", "class": "schema:QuantitativeValue", "description": "The weight of the subject.", "maxCount": "1", "name": "Weight", "node": "bbpexpshape:WeightShape", "path": "schema:weight"}, {"@id": "_:b7", "description": "The sex of the subject.", "maxCount": "1", "minCount": "1", "name": "Sex", "node": [], "path": "bbpexp:sex"}, {"@id": "_:b14", "description": "Subject Strain.", "maxCount": "1", "name": "Strain", "node": [], "nodeKind": "sh:IRI", "path": "bbpexp:strain"} ], "targetClass": "bbpexp:Subject"}, {"@id": "bbpexpshape:WeightShape", "@type": "sh:NodeShape", "property": [{"@id": "_:b0", "nodeKind": "sh:IRI", "maxCount": "1", "minCount": "1", "name": "Unit Code", "node": [], "path": "schema:unitCode"}, {"@id": "_:b3", "datatype": "xsd:double", "maxCount": "1", "minCount": "1", "name": "Weight", "node": [], "path": "schema:value"} ], "targetObjectOf": "schema:weight"} ], "@context": {"maxCount": {"@id": "sh:maxCount", "@type": "xsd:integer"}, "minCount": {"@id": "sh:minCount", "@type": "xsd:integer"}, "datatype": {"@id": "sh:datatype", "@type": "@id"}, "name": "sh:name", "path": {"@id": "sh:path", "@type": "@id"}, "nodeKind": {"@id": "node:Kind", "@type": "@id"}, "description": "sh:description", "class": {"@id": "sh:class", "@type": "@id"}, "property": {"@id": "sh:property", "@type": "@id"}, "isDefinedBy": {"@id": "rdfs:isDefinedBy", "@type": "@id"}, "targetClass": {"@id": "sh:targetClass", "@type": "@id"}, "targetObjectOf": {"@id": "sh:targetObjectOf", "@type": "@id"}, "node": {"@id": "sh:node", "@type": "@id"}, "rest": {"@id": "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest", "@type": "@id"}, "first": "http://www.w3.org/1999/02/22-rdf-syntax-ns#first", "in": {"@id": "sh:in", "@type": "@id"}, "schema": "http://schema.org/", "bbpexpshape": "https://bbp-nexus.epfl.ch/schemas/bbp/experiment/subject/v1.0.0/shapes/", "sh": "http://www.w3.org/ns/shacl#", "owl": "http://www.w3.org/2002/07/owl#", "sex": "http://www.hbp.FIXME.org/hbp_sex_ontology/", "xsd": "http://www.w3.org/2001/XMLSchema#", "bbpexp": "https://bbp-nexus.epfl.ch/voc/experiment/core/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "shapes": {"@reverse": "rdfs:isDefinedBy", "@type": "@id"} } }'
```

## Update
###**`PUT /schemas/{orgId}/{domainId}/{name}/{version}?rev={rev}`**
**Updates** the schema specified in **name & version** from the domain **domainId** inside the organization **orgId** overriding the existing payload with the provided one.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization.                                                                                   | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/). |
| rev           | Long          | The current revision of the schema.                                                                                           |


### Request JSON-LD Object
A valid JSON-LD object


### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the schema.         |
| rev           | Long          | The current revision of the schema.           |


### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request**
    * The schema, domain or organization are deprecated.
    * The schema payload provided does not have valid shape constrains.
    * The schema payload provided imports a shape or schema which does not exist.
    * The schema payload provided imports a shape or schema which is not allowed to import.
    * The schema is already published.
* **409 Conflict** - The provided rev query parameter does not match the current revision.
* **404 Not Found** - The schema, domain or organization do not exist.


### Example request
```bash
curl -v -X PUT -H "Content-Type: application/json" "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject/v1.0.0?rev=1" -d '{"@type": "owl:Ontology2", "shapes": [{"@id": "bbpexpshape:AgeShape", "@type": "sh:NodeShape", "property": [{"@id": "_:b6", "sh:in": {"@list": ["Pre-natal", "Post-natal"] }, "maxCount": "1", "minCount": "1", "name": "Period", "node": [], "path": "bbpexp:period"}, {"@id": "_:b5", "class": "schema:QuantitativeValue", "maxCount": "1", "minCount": "1", "name": "Age", "node": "bbpexpshape:AgeValueShape", "path": "schema:value"} ] }, {"@id": "bbpexpshape:AgeValueShape", "@type": "sh:NodeShape", "property": [{"@id": "_:b2", "maxCount": "1", "minCount": "1", "name": "Unit Code", "node": [], "nodeKind": "sh:IRI", "path": "schema:unitCode"}, {"@id": "_:b4", "datatype": "xsd:double", "maxCount": "1", "minCount": "1", "name": "Age value", "node": [], "path": "schema:value"} ] }, {"@id": "bbpexpshape:SubjectShape", "@type": "sh:NodeShape", "description": "Subject (used in neuroscience experiment) shape definition. A subject should have an IRI as identifier.", "nodeKind": "sh:IRI", "property": [{"@id": "_:b8", "class": "bbpexp:Age", "description": "The subject age.", "maxCount": "1", "minCount": "0", "name": "Age", "node": "bbpexpshape:AgeShape", "path": "bbpexp:age"}, {"@id": "_:b9", "description": "Subject disease.", "maxCount": "1", "name": "Disease", "node": [], "path": "bbpexp:disease"}, {"@id": "_:b1", "description": "The species of the subject.", "maxCount": "1", "minCount": "1", "name": "Species", "node": [], "nodeKind": "sh:IRI", "path": "bbpexp:species"}, {"description": "Subject Transgenic.", "maxCount": "1", "name": "Transgenic", "node": [], "nodeKind": "sh:IRI", "path": "bbpexp:transgenic"}, {"@id": "_:b11", "description": "Description of the subject.", "name": "Description", "node": [], "path": "schema:description"}, {"@id": "_:b12", "description": "Laboratory/team internal name of the subject.", "name": "Name", "node": [], "path": "schema:name"}, {"@id": "_:b13", "class": "schema:QuantitativeValue", "description": "The weight of the subject.", "maxCount": "1", "name": "Weight", "node": "bbpexpshape:WeightShape", "path": "schema:weight"}, {"@id": "_:b7", "description": "The sex of the subject.", "maxCount": "1", "minCount": "1", "name": "Sex", "node": [], "path": "bbpexp:sex"}, {"@id": "_:b14", "description": "Subject Strain.", "maxCount": "1", "name": "Strain", "node": [], "nodeKind": "sh:IRI", "path": "bbpexp:strain"} ], "targetClass": "bbpexp:Subject"}, {"@id": "bbpexpshape:WeightShape", "@type": "sh:NodeShape", "property": [{"@id": "_:b0", "nodeKind": "sh:IRI", "maxCount": "1", "minCount": "1", "name": "Unit Code", "node": [], "path": "schema:unitCode"}, {"@id": "_:b3", "datatype": "xsd:double", "maxCount": "1", "minCount": "1", "name": "Weight", "node": [], "path": "schema:value"} ], "targetObjectOf": "schema:weight"} ], "@context": {"maxCount": {"@id": "sh:maxCount", "@type": "xsd:integer"}, "minCount": {"@id": "sh:minCount", "@type": "xsd:integer"}, "datatype": {"@id": "sh:datatype", "@type": "@id"}, "name": "sh:name", "path": {"@id": "sh:path", "@type": "@id"}, "nodeKind": {"@id": "node:Kind", "@type": "@id"}, "description": "sh:description", "class": {"@id": "sh:class", "@type": "@id"}, "property": {"@id": "sh:property", "@type": "@id"}, "isDefinedBy": {"@id": "rdfs:isDefinedBy", "@type": "@id"}, "targetClass": {"@id": "sh:targetClass", "@type": "@id"}, "targetObjectOf": {"@id": "sh:targetObjectOf", "@type": "@id"}, "node": {"@id": "sh:node", "@type": "@id"}, "rest": {"@id": "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest", "@type": "@id"}, "first": "http://www.w3.org/1999/02/22-rdf-syntax-ns#first", "in": {"@id": "sh:in", "@type": "@id"}, "schema": "http://schema.org/", "bbpexpshape": "https://bbp-nexus.epfl.ch/schemas/bbp/experiment/subject/v1.0.0/shapes/", "sh": "http://www.w3.org/ns/shacl#", "owl": "http://www.w3.org/2002/07/owl#", "sex": "http://www.hbp.FIXME.org/hbp_sex_ontology/", "xsd": "http://www.w3.org/2001/XMLSchema#", "bbpexp": "https://bbp-nexus.epfl.ch/voc/experiment/core/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#", "shapes": {"@reverse": "rdfs:isDefinedBy", "@type": "@id"} } }'
```

## Publish
###**`PATCH /schemas/{orgId}/{domainId}/{name}/{version}/config?rev={rev}`**
**Publish** the schema specified in **name & version** from the domain **domainId** inside the organization **orgId**. After published, the schema can be used to create and validate instances against it.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization.                                                                                   | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/). |
| rev           | Long          | The current revision of the schema.                                                                                           |

### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the schema.         |
| rev           | Long          | The current revision of the schema.           |

### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request**
    * The schema, domain or organization are deprecated.
    * The schema is already published.
* **409 Conflict** - The provided rev query parameter does not match the current revision.
* **404 Not Found** - The schema, domain or organization do not exist.


### Example request
```bash
curl -v -X PATCH "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject/v1.0.0/config?rev=2"
```


## Deprecate
###**`DELETE /schemas/{orgId}/{domainId}/{name}/{version}?rev={rev}`**
**Deprecates** the schema specified in **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization.                                                                                   | 
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain. It has to match the following regex: ([a-zA-Z0-9]+)          |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/). |
| rev           | Long          | The current revision of the schema.                                                                                           |

### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the schema.         |
| rev           | Long          | The current revision of the schema.           |

### Status Codes
* **200 OK** - Request completed successfully.
* **400 Bad Request** - The schema, domain or organization are deprecated.
* **409 Conflict** - The provided rev query parameter does not match the current revision.
* **404 Not Found** - The schema, domain or organization do not exist.


### Example request
```bash
curl -v -X DELETE "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject/v1.0.0?rev=2"
```

## Get shape
###**`GET /schemas/{orgId}/{domainId}/{name}/{version}/shapes/{fragment}`**
**Retrieves** the shape specified in **fragment** from the schema in **name & version** from the domain **domainId** inside the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization.                                                                                   |    
| domainId      | String        | The unique identifier for the domain for this particular organization.                                                        |
| name          | String        | The unique identifier for the schema for this particular domain.                                                              |
| version       | String        | The unique identifier for the schema version for this particular schema, following the [semantic format](http://semver.org/). |
| fragment      | String        | The unique short identifier for a shape inside the schema.                                                                    |



### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the shape.          |
| rev           | Long          | The current revision of the schema.           |
| deprecated    | Boolean       | whether the schema is deprecated or not.      |
| published     | Boolean       | whether the schema is deprecated or not.      |
| *             | *             | The payload of the shape.                     |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested shape does not exist.

### Example request
```bash
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/schemas/nexus/core/subject/v1.0.0/shapes/AgeValueShape"
```
