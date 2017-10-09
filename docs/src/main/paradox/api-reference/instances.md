# Instances

Instance resources are rooted in the `/v0/data` collection.  As described in the
@ref:[API Reference](index.md), these represent the last level resources.  

### Create an instance

An instance is created to be valid against an specific schema (`name` and `version`) and to belong to a certain organization `orgId` and domain `domId`. This is specified using the following URI:

```
 POST /v0/data/{orgId}/{domId}/{name}/{version} 
{...}
```

The `{name}/{version}` defines the schema.

The `{domId}` defines the name of the domain.

The `{orgId}` is the name for the organization.

The json value must be compliant with the [SHACL definition](https://www.w3.org/TR/shacl/) and should valid against the schema defined in `/v0/schemas/{orgId}/{domId}/{name}/{version}`.

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

The json value must be compliant with the [SHACL definition](https://www.w3.org/TR/shacl/) and should valid against the schema defined in `/v0/schemas/{orgId}/{domId}/{name}/{version}`.

#### Example

Request
:   @@snip [schema-update.sh](../assets/api-reference/instances/instance-update.sh)

Payload
:   @@snip [schema.json](../assets/api-reference/instances/instance.json)

Response
:   @@snip [schema-ref-new.json](../assets/api-reference/instances/instance-ref.json)

### Create an instance's attachment

Every instance has a subresource `attachment` which is a representation of the instance binary data or attachment.

```
 PUT /v0/data/{orgId}/{domId}/{name}/{version}/{id}/attachment?rev={rev}
{...}
```
#### Multipart request

*Content-Type*: application/json

It should contain one of the multipart types as:
```
Content-Disposition: form-data; name="file"; filename="{FILENAME}"
Content-Type: {VALID_MIME_TYPE}
```

#### Example

Request
:   @@snip [instance-attachment-add.sh](../assets/api-reference/instances/instance-attachment-add.sh)

Response
:   @@snip [instance-attachment-add-ref.json](../assets/api-reference/instances/instance-attachment-ref.json)

### Fetch an instance

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}
```
#### Example

Request
:   @@snip [instance-get.sh](../assets/api-reference/instances/instance-get.sh)

Response
:   @@snip [existing-instance.json](../assets/api-reference/instances/existing-instance.json)

### Fetch an instance revision

```
GET /v0/data/{orgId}/{domId}/{name}/{version}/{id}?rev={rev}
```
#### Example

Request
:   @@snip [instance-get-rev.sh](../assets/api-reference/instances/instance-get-rev.sh)

Response
:   @@snip [existing-instance.json](../assets/api-reference/instances/existing-instance.json)

### Fetch an instance's attachment

Retrieves the attachment content of an instance.

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
:   @@snip [instance-get.sh](../assets/api-reference/instances/instance-get-attachment-rev.sh)

### Deprecate an instance

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
:   @@snip [instance-delete.sh](../assets/api-reference/instances/instance-delete-attachment.sh)

Response
:   @@snip [instance-ref-delete.json](../assets/api-reference/instances/instance-ref-delete-attachment.json)

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

* `{orgId}` filters the resulting instances to belong to a specific organization.
* `{domId}` filters the resulting instances to belong to a specific domain.
* `{name}` filters the resulting instances to have a specific name.
* `{version}` filters the resulting instances to have a specific version.
* `{full_text_search_query}` is an arbitrary string that is looked up in the attribute values of the selected instances.
* `{filter}` is a filtering expression as described in the @ref:[Search and filtering](operating-on-resources.md#search-and-filtering) section.  
* `{from}` and `{size}` are the listing pagination parameters.  
* `{deprecated}` selects only instances that have the specified deprecation status.

All query parameters described (`q`, `filter`, `from`, `size`, `deprecated` and `published`) are optional.

The path parameters `/{orgId}/`, `/{domId}/` and `/{name}/` `/{version}/` are optional.

#### Example

Request
:   @@snip [instances-list.sh](../assets/api-reference/instances/instance-list.sh)

Response
:   @@snip [instances-list.json](../assets/api-reference/instances/instance-list.json)
