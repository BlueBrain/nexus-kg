# Instances

Instance resources are rooted in the `/v0/data` collection.  As described in the
@ref:[API Reference](index.md), these represent the last level resources.  


**Note:** You might need to add `-H "Authorization: Bearer ***"` if you are attempting to operate on a protected resource using the curl examples.

### Create an instance

An instance is created to be valid against an specific schema (`name` and `version`) and to belong to a certain organization `orgId` and domain `domId`. This is specified using the following URI:

```
 POST /v0/data/{orgId}/{domId}/{name}/{version} 
{...}
```

The `{name}/{version}` defines the schema that the instance belongs to.

The `{domId}` defines the id of the domain that the instance belongs to.

The `{orgId}` defines the id for the organization that the instance belongs to.

The json payload must be valid with the schema defined at `/v0/schemas/{orgId}/{domId}/{name}/{version}`.

#### Example
Request
:   @@snip [instance.sh](../assets/api-reference/instances/instance.sh)

Payload
:   @@snip [instance.json](../assets/api-reference/instances/instance.json)

Response
:   @@snip [instance-ref-new.json](../assets/api-reference/instances/instance-ref-new.json)

### Update an instance

It overrides the payload of the provided instance.
```
PUT /v0/data/{orgId}/{domId}/{name}/{version}/{id}?rev={previous_rev}
{...}
```
... where `{previous_rev}` is the last known revision number for the instance and `{id}` is the UUID of the instance.

The json payload must be valid with the schema defined at `/v0/schemas/{orgId}/{domId}/{name}/{version}`.

#### Example

Request
:   @@snip [instance-update.sh](../assets/api-reference/instances/instance-update.sh)

Payload
:   @@snip [instance.json](../assets/api-reference/instances/instance.json)

Response
:   @@snip [instance-ref.json](../assets/api-reference/instances/instance-ref.json)

### Attach a binary to an instance

Every instance may have a sub-resource (`/attachment`) which is a binary representation of the instance.

```
 PUT /v0/data/{orgId}/{domId}/{name}/{version}/{id}/attachment?rev={rev}
{...}
```
#### Multipart request

The values provided for `filename` and `Content-Type` will be recorded by the system and presented as part of the instance json value.

The binary must be sent as part of the form field `file`.

#### Example

Request
:   @@snip [instance-attachment-add.sh](../assets/api-reference/instances/instance-attachment-add.sh)

Response
:   @@snip [instance-attachment-ref.json](../assets/api-reference/instances/instance-attachment-ref.json)

### Fetch an instance

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}
```
#### Example

Request
:   @@snip [instance-get.sh](../assets/api-reference/instances/instance-get.sh)

Response
:   @@snip [existing-instance.json](../assets/api-reference/instances/existing-instance.json)

#### Fetch a specific instance revision

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}?rev={rev}
```
#### Example

Request
:   @@snip [instance-get-rev.sh](../assets/api-reference/instances/instance-get-rev.sh)

Response
:   @@snip [existing-instance.json](../assets/api-reference/instances/existing-instance.json)

#### Fetch an instance in a specific output format

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}?format={format}
```

Supported `{format}` variants are `compacted`, `expanded`, `flattened`.

#### Example

Request
:   @@snip [instance-get-rev.sh](../assets/api-reference/instances/instance-get-format.sh)

Response
:   @@snip [existing-instance.json](../assets/api-reference/instances/existing-instance-expanded.json)

### Fetch an instance's attachment

Retrieves the binary attachment of an instance at a specific revision.

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}/attachment
```
#### Example

Request
:   @@snip [instance-get.sh](../assets/api-reference/instances/instance-get-attachment.sh)

### Fetch an instance's attachment revision

Retrieves the attachment content of an instance for a specific revision.

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}/attachment?rev={rev}
```
#### Example

Request
:   @@snip [instance-get-attachment-rev.sh](../assets/api-reference/instances/instance-get-attachment-rev.sh)

### Deprecate an instance

Deprecating an instance prevents further updates or changes to its attachment.

```
DELETE /v0/data/{orgId}/{domId}/{name}/{version}/{id}?rev={rev}
```

#### Example

Request
:   @@snip [instance-delete.sh](../assets/api-reference/instances/instance-delete.sh)

Response
:   @@snip [instance-ref-delete.json](../assets/api-reference/instances/instance-ref-delete.json)

### Delete an instance's attachment

```
DELETE /v0/data/{orgId}/{domId}/{name}/{version}/{id}/attachment?rev={rev}
```
This does not delete the binary attached to an instance, although it prevents from fetching it from the latest review. The binary attached will be still available fetching an older review.

#### Example

Request
:   @@snip [instance-delete-attachment.sh](../assets/api-reference/instances/instance-delete-attachment.sh)

Response
:   @@snip [instance-ref-delete-attachment.json](../assets/api-reference/instances/instance-ref-delete-attachment.json)

### Search instances

Follows the general definition of searching in a collection of resources.

```
GET /v0/data/{orgId}/{domId}/{name}/{version}
      ?q={full_text_search_query}
      &filter={filter}
      &from={from}
      &size={size}
      &deprecated={deprecated}
      &published={published}
```
... where 

* `{orgId}` the organization the instance belongs to.
* `{domId}` the domain the instance belongs to.
* `{name}` the schema name that the instance conforms to.
* `{version}` the schema version that the instance conforms to.
* `{full_text_search_query}` is an arbitrary string that is looked up in the attribute values of the selected instances.
* `{filter}` is a filtering expression as described in the @ref:[Search and filtering](operating-on-resources.md#search-and-filtering) section.  
* `{from}` and `{size}` are the listing pagination parameters.  
* `{deprecated}` selects only instances that have the specified deprecation status.

All query parameters described (`q`, `filter`, `from`, `size`, `deprecated` and `published`) are optional.

The path segments (`{orgId}/{domId}/{name}/{version}`) are optional; when used, they constrain the resulting listing to contain only instances that share the same organization, domain ids and schema name and version. 
Any of the segments can be omitted but since they are positional it's required that depended segments (to the left) are specified. For example, one can list all the instances within a domain using a simple GET request on `/v0/data/{orgId}/{domId}`.

#### Example

Request
:   @@snip [instances-list.sh](../assets/api-reference/instances/instance-list.sh)

Response
:   @@snip [instances-list.json](../assets/api-reference/instances/instance-list.json)
