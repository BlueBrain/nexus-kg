# Contexts

Context resources are rooted in the `/v0/contexts` collection.  As described in the
@ref:[API Reference](index.md), these represent the third level resources.  All configuration and policies apply to their
sub-resources.

**Note:** You might need to add `-H "Authorization: Bearer ***"` if you are attempting to operate on a protected resource using the curl examples.


### Create a context

Context `name` and `version` along with domain `domId` and organization `orgId` create local ids for these resources, which means the consumer has the necessary information to perform
a direct `PUT` request with the resource address.  Omitting the last revision implies a resource creation attempt.

```
 PUT /v0/contexts/{orgId}/{domId}/{name}/{version} 
{...}
```

The version is constrained by the [semantic format](http://semver.org/) and it adds versioning information to the context.

The `{name}` defines the name of the context.

The `{domId}` defines the id of the domain that the context belongs to.

The `{orgId}` defines the id of the organization that the context belongs to.

The json payload must be compliant with the [JSON-LD @context definition](https://www.w3.org/TR/json-ld/#the-context).

#### Example
Request
:   @@snip [context.sh](../assets/api-reference/contexts/context.sh)

Payload
:   @@snip [context.json](../assets/api-reference/contexts/context.json)

Response
:   @@snip [context-ref-new.json](../assets/api-reference/contexts/context-ref-new.json)

### Update a context

```
PUT /v0/context/{orgId}/{domId}/{name}/{version}?rev={previous_rev}
{...}
```
... where `{previous_rev}` is the last known revision number for the context.

The json payload must be compliant with the [JSON-LD @context definition](https://www.w3.org/TR/json-ld/#the-context).

#### Example

Request
:   @@snip [context-update.sh](../assets/api-reference/contexts/context-update.sh)

Payload
:   @@snip [context.json](../assets/api-reference/contexts/context.json)

Response
:   @@snip [context-ref-new.json](../assets/api-reference/contexts/context-ref.json)

### Fetch a context

```
GET /v0/contexts/{orgId}/{domId}/{name}/{version}
```
#### Example

Request
:   @@snip [context-get.sh](../assets/api-reference/contexts/context-get.sh)

Response
:   @@snip [existing-context.json](../assets/api-reference/contexts/existing-context.json)

### Fetch a context revision

```
GET /v0/contexts/{orgId}/{domId}/{name}/{version}?rev={rev}
```
#### Example

Request
:   @@snip [context-get-rev.sh](../assets/api-reference/contexts/context-get-rev.sh)

Response
:   @@snip [existing-context.json](../assets/api-reference/contexts/existing-context.json)


### Publish a context

```
PATCH /v0/contexts/{orgId}/{domId}/{name}/{version}/config?rev={rev}
```

The json payload contains the key `published` with it's value (true|false).

#### Example

Request
:   @@snip [context-patch.sh](../assets/api-reference/contexts/context-patch.sh)

Payload
:   @@snip [context-patch.json](../assets/api-reference/contexts/context-patch.json)

Response
:   @@snip [context-ref-patch.json](../assets/api-reference/contexts/context-ref-patch.json)


### Deprecate a context

Deprecating a context prevents use of the context by instances, schemas or other contexts.

```
DELETE /v0/contexts/{orgId}/{domId}/{name}/{version}?rev={rev}
```

#### Example

Request
:   @@snip [context-delete.sh](../assets/api-reference/contexts/context-delete.sh)

Response
:   @@snip [context-ref-delete.json](../assets/api-reference/contexts/context-ref-delete.json)

### Search contexts

Follows the general definition of searching in a collection of resources.

```
GET /v0/contextss/{orgId}/{domId}/{name}
      ?from={from}
      &size={size}
      &deprecated={deprecated}
      &published={published}
```
... where 

* `{orgId}` the organization the schema belongs to.
* `{domId}` the domain the schema belongs to.
* `{name}` the schema name.
* `{from}` and `{size}` are the listing pagination parameters.  
* `{deprecated}` selects only schemas that have the specified deprecation status.
* `{published}` selects only schemas that have the specified published status.

All query parameters described (`from`, `size`, `deprecated` and `published`) are optional.

The path segments (`{orgId}/{domId}/{name}`) are optional; when used, they constrain the resulting listing to contain only contexts that share the same organization, domain ids and name. 
Any of the segments can be omitted but since they are positional it's required that depended segments (to the left) are specified. For example, one can list all the contexts within a domain using a simple GET request on `/v0/contexts/{orgId}/{domId}`.

#### Example

Request
:   @@snip [context-list.sh](../assets/api-reference/contexts/context-list.sh)

Response
:   @@snip [context-list.json](../assets/api-reference/contexts/context-list.json)
