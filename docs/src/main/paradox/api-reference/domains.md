# Domains

Domain resources are rooted in the `/v0/domains` collection.  As described in the
@ref:[API Reference](index.md), these represent the second level resources.  All configuration and policies apply to their
sub-resources.

**Note:** You might need to add `-H "Authorization: Bearer ***"` if you are attempting to operate on a protected resource using the curl examples.


### Create a domain

Domain `domId` along with organization `orgId` create local ids for these resources, which means the consumer has the necessary information to perform
a direct `PUT` request with the resource address.  Omitting the last revision implies a resource creation attempt.

```
 PUT /v0/domains/{orgId}/{domId} 
{...}
```

The `{domId}` is constrained to `[a-zA-Z0-9]+` and it defines the id of the domain.

The `{orgId}` defines the id of the organization that the schema belongs to.

The json payload contains the key `description` with it's value.

#### Example
Request
:   @@snip [domain.sh](../assets/api-reference/domains/domain.sh)

Payload
:   @@snip [domain.json](../assets/api-reference/domains/domain.json)

Response
:   @@snip [domain-ref-new.json](../assets/api-reference/domains/domain-ref-new.json)

### Fetch a domain

```
GET /v0/domains/{orgId}/{domId}
```
#### Example

Request
:   @@snip [domain-get.sh](../assets/api-reference/domains/domain-get.sh)

Response
:   @@snip [existing-domain.json](../assets/api-reference/domains/existing-domain.json)

#### Fetch a specific domain revision

```
GET /v0/domains/{orgId}/{domId}?rev={rev}
```
#### Example

Request
:   @@snip [domain-get-rev.sh](../assets/api-reference/domains/domain-get-rev.sh)

Response
:   @@snip [existing-domain.json](../assets/api-reference/domains/existing-domain.json)

#### Fetch a domain in a specific output format

```
GET /v0/domains/{orgId}/{domId}?format={format}
```

Supported `{format}` variants are `compacted`, `expanded`, `flattened`.

#### Example

Request
:   @@snip [domain-get-rev.sh](../assets/api-reference/domains/domain-get-format.sh)

Response
:   @@snip [existing-domain.json](../assets/api-reference/domains/existing-domain-expanded.json)


### Deprecate a domain

Deprecating a domain disables schema or instance creation in that domain.

```
DELETE /v0/domains/{orgId}/{domId}?rev={rev}
```

#### Example

Request
:   @@snip [domain-delete.sh](../assets/api-reference/domains/domain-delete.sh)

Response
:   @@snip [domain-ref-delete.json](../assets/api-reference/domains/domain-ref-delete.json)

### Search domains

Follows the general definition of searching in a collection of resources.

```
GET /v0/domains/{orgId}/
      ?q={full_text_search_query}
      &filter={filter}
      &from={from}
      &size={size}
      &deprecated={deprecated}
```
... where 

* `{orgId}` the organization the domain belongs to.
* `{full_text_search_query}` is an arbitrary string that is looked up in the attribute values of the selected domains.
* `{filter}` is a filtering expression as described in the @ref:[Search and filtering](operating-on-resources.md#search-and-filtering) section.  
* `{from}` and `{size}` are the listing pagination parameters.  
* `{deprecated}` selects only domains that have the specified deprecation status.

All query parameters described (`q`, `filter`, `from`, `size` and `deprecated`) are optional.

The path segment (`{orgId}`) is optional; when used, they constrain the resulting listing to contain only domains that share the same organization

#### Example

Request
:   @@snip [domains-list.sh](../assets/api-reference/domains/domain-list.sh)

Response
:   @@snip [domains-list.json](../assets/api-reference/domains/domain-list.json)
