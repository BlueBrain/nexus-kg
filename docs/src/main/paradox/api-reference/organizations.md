# Organizations

Organization resources are rooted in the `/v0/organizations` collection.  As described in the
@ref:[API Reference](index.md), these represent the top level resources.  All configuration and policies apply to their
sub-resources.

**Note:** You might need to add `-H "Authorization: Bearer ***"` if you are attempting to operate on a protected resource using the curl examples.

### Create an organization

Organization names are local ids for these resources, which means the consumer has the necessary information to perform
a direct `PUT` request with the resource address.  Omitting the last revision implies a resource creation attempt.

```
PUT /v0/organizations/{name}
{...}
```

The `{name}` is constrained to `[a-z0-9]{3,5}`.

The json payload must be compliant with the [schema.org definition](http://schema.org/Organization) for organizations.
The nexus shacl schema for organization constrains the accepted values.

[//]: # (TODO: embed the nexus shacl schema for organization)

#### Example
Request
:   @@snip [organization.sh](../assets/api-reference/organizations/organization.sh)

Payload
:   @@snip [organization.json](../assets/api-reference/organizations/organization.json)

Response
:   @@snip [org-ref-new.json](../assets/api-reference/organizations/org-ref-new.json)

### Update an organization

```
PUT /v0/organizations/{name}?rev={previous_rev}
{...}
```
... where `{previous_rev}` is the last known revision number for the organization.

The json value must be compliant with the [schema.org definition](http://schema.org/Organization) for organizations.
The nexus shacl schema for organization constrains the accepted values.

[//]: # (TODO: link to embedded organization shacl schema)

#### Example

Request
:   @@snip [organization-update.sh](../assets/api-reference/organizations/organization-update.sh)

Payload
:   @@snip [organization.json](../assets/api-reference/organizations/organization.json)

Response
:   @@snip [org-ref-new.json](../assets/api-reference/organizations/org-ref.json)

### Fetch an organization

```
GET /v0/organizations/{name}
```
#### Example

Request
:   @@snip [organization-get.sh](../assets/api-reference/organizations/organization-get.sh)

Response
:   @@snip [existing-organization.json](../assets/api-reference/organizations/existing-organization.json)

#### Fetch a specific organization revision

```
GET /v0/organizations/{name}?rev={rev}
```
#### Example

Request
:   @@snip [organization-get-rev.sh](../assets/api-reference/organizations/organization-get-rev.sh)

Response
:   @@snip [existing-organization.json](../assets/api-reference/organizations/existing-organization.json)

#### Fetch an organization in a specific format

```
GET /v0/organizations/{name}?format={format}
```

Supported `{format}` variants are `compacted`, `expanded`, `flattened`.

#### Example

Request
:   @@snip [organization-get-rev.sh](../assets/api-reference/organizations/organization-get-format.sh)

Response
:   @@snip [existing-organization.json](../assets/api-reference/organizations/existing-organization-expanded.json)

### Deprecate an organization

```
DELETE /v0/organizations/{name}?rev={rev}
```

#### Example

Request
:   @@snip [organization-delete.sh](../assets/api-reference/organizations/organization-delete.sh)

Response
:   @@snip [org-ref-new.json](../assets/api-reference/organizations/org-ref-delete.json)

### Search organizations

Follows the general definition of searching in a collection of resources.

```
GET /v0/organizations
      ?q={full_text_search_query}
      &filter={filter}
      &from={from}
      &size={size}
      &deprecated={deprecated}
```
... where `{full_text_search_query}` is an arbitrary string that is looked up in the attribute values of the selected
organizations and `{filter}` is a filtering expression as described in the
@ref:[Search and filtering](operating-on-resources.md#search-and-filtering) section.  The `{from}` and `{size}` are
the listing pagination parameters.  The `{deprecated}` selects only organizations that have the specified deprecation
status.

All query parameters described (`q`, `filter`, `from`, `size` and `deprecated`) are optional.

#### Example

Request
:   @@snip [organization-delete.sh](../assets/api-reference/organizations/organization-list.sh)

Response
:   @@snip [organization-list.json](../assets/api-reference/organizations/organization-list.json)
