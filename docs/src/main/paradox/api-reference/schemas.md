# Schemas

Schema resources are rooted in the `/v0/schemas` collection.  As described in the
@ref:[API Reference](index.md), these represent the third level resources.  All configuration and policies apply to their
sub-resources.

### Create a schema

Schema `name` and `version` along with domain `domId` and organization `orgId` create local ids for these resources, which means the consumer has the necessary information to perform
a direct `PUT` request with the resource address.  Omitting the last revision implies a resource creation attempt.

```
 PUT /v0/schemas/{orgId}/{domId}/{name}/{version} 
{...}
```

The version is constrained by the [semantic format](http://semver.org/) and it adds versioning information to the schema.

The `{name}` defines the name of the schema.

The `{domId}` defines the name of the domain.

The `{orgId}` is the name for the organization.

The json payload must be compliant with the [SHACL definition](https://www.w3.org/TR/shacl/).

#### Example
Request
:   @@snip [schema.sh](../assets/api-reference/schemas/schema.sh)

Payload
:   @@snip [schema.json](../assets/api-reference/schemas/schema.json)

Response
:   @@snip [schema-ref-new.json](../assets/api-reference/schemas/schema-ref-new.json)

### Update a domain

```
PUT /v0/schemas/{orgId}/{domId}/{name}/{version}?rev={previous_rev}
{...}
```
... where `{previous_rev}` is the last known revision number for the schema.

The json value must be compliant with the [SHACL definition](https://www.w3.org/TR/shacl/).

#### Example

Request
:   @@snip [schema-update.sh](../assets/api-reference/schemas/schema-update.sh)

Payload
:   @@snip [schema.json](../assets/api-reference/schemas/schema.json)

Response
:   @@snip [schema-ref-new.json](../assets/api-reference/schemas/schema-ref.json)

### Fetch a schema

```
GET /v0/schemas/{orgId}/{domId}/{name}/{version}
```
#### Example

Request
:   @@snip [schema-get.sh](../assets/api-reference/schemas/schema-get.sh)

Response
:   @@snip [existing-schema.json](../assets/api-reference/schemas/existing-schema.json)

### Fetch a schema revision

```
GET /v0/schemas/{orgId}/{domId}/{name}/{version}?rev={rev}
```
#### Example

Request
:   @@snip [domain-get-rev.sh](../assets/api-reference/schemas/schema-get-rev.sh)

Response
:   @@snip [existing-domain.json](../assets/api-reference/schemas/existing-schema.json)


### Publish a schema

```
PATCH /v0/schemas/{orgId}/{domId}/{name}/{version}/config?rev={rev}
```

The json payload contains the key `published` with it's value (true|false).

#### Example

Request
:   @@snip [schema-patch.sh](../assets/api-reference/schemas/schema-patch.sh)

Payload
:   @@snip [schema-patch.json](../assets/api-reference/schemas/schema-patch.json)

Response
:   @@snip [schema-ref-new.json](../assets/api-reference/schemas/schema-ref-patch.json)


### Deprecate a schema

```
DELETE /v0/schemas/{orgId}/{domId}/{name}/{version}?rev={rev}
```

#### Example

Request
:   @@snip [schema-delete.sh](../assets/api-reference/schemas/schema-delete.sh)

Response
:   @@snip [schema-ref-new.json](../assets/api-reference/schemas/schema-ref-delete.json)

### Search schemas

Follows the general definition of searching in a collection of resources.

```
GET /v0/schemas/{orgId}/{domId}/{name}
      ?q={full_text_search_query}
      &filter={filter}
      &from={from}
      &size={size}
      &deprecated={deprecated}
      &published={published}
```
... where 

* `{orgId}` filters the resulting schemas to belong to a specific organization.
* `{domId}` filters the resulting schemas to belong to a specific domain.
* `{name}` filters the resulting schemas to have a specific name.
* `{full_text_search_query}` is an arbitrary string that is looked up in the attribute values of the selected schemas.
* `{filter}` is a filtering expression as described in the @ref:[Search and filtering](operating-on-resources.md#search-and-filtering) section.  
* `{from}` and `{size}` are the listing pagination parameters.  
* `{deprecated}` selects only schemas that have the specified deprecation status.
* `{published}` selects only schemas that have the specified published status.

All query parameters described (`q`, `filter`, `from`, `size`, `deprecated` and `published`) are optional.

The path parameters `/{orgId}/`, `/{domId}/` and `/{name}/` are optional.

#### Example

Request
:   @@snip [schemas-list.sh](../assets/api-reference/schemas/schema-list.sh)

Response
:   @@snip [schemas-list.json](../assets/api-reference/schemas/schema-list.json)
