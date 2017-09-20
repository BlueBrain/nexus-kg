# Operating on resources

All resources in the system share a base set of operations.  Assuming a nexus deployment at
`http(s)://nexus.example.com` resource address of `/v0/{address}` the following operations should apply to most (all)
resources:

### Fetch the current revision of the resource

```
GET /v0/{address}
```

#### Status Codes

- **200 OK**: the resource is found and returned successfully
- **404 Not Found**: the resource was not found

### Fetch a specific revision of the resource

```
GET /v0/{address}?rev={rev}
```
... where `rev` is the revision number, starting at `1`.

#### Status Codes

- **200 OK**: the resource revision is found and returned successfully
- **404 Not Found**: the resource revision was not found

### Fetch a resource history

Returns the collection of changes performed on the resource (the deltas).

```
GET /v0/{address}/history
```

#### Status Codes

- **200 OK**: the resource is found and its history is returned successfully
- **404 Not Found**: the resource was not found

### Create a new resource

Depending on whether the resource is a singleton resource or is part of a wider collection of resources of the same
type the verbs `POST` and `PUT` are used.

For a singleton resource:

```
PUT /v0/{address}
{...}
```

For a collection resources:

```
POST /v0/{collection_address}
{...}
```
... where `collection_address` is the address of the collection the resource belongs to.

#### Status Codes

- **201 Created**: the resource was created successfully
- **400 Bad Request**: the resource is not valid or cannot be created at this time
- **409 Conflict**: the resource already exists

### Update a resource

In order to ensure a client does not perform any changes to a resource without having had seen the previous revision of
the resource, the last revision needs to be passed as a query parameter.

```
PUT /v0/{address}?rev={previous_rev}
{...}
```

#### Status Codes

- **200 OK**: the resource was created successfully
- **400 Bad Request**: the resource is not valid or cannot be created at this time
- **409 Conflict**: the provided revision is not the current resource revision number

### Partially update a resource

A partial update is still an update, so the last revision needs to be passed as a query parameter as well.

```
PATCH /v0/{address}?rev={previous_rev}
{...}
```

#### Status Codes

- **200 OK**: the resource was created successfully
- **400 Bad Request**: the resource is not valid or cannot be created at this time
- **409 Conflict**: the provided revision is not the current resource revision number

### Deprecate a resource

Deprecating a resource is considered to be an update as well.

```
DELETE /v0/{address}?rev={previous_rev}
```

#### Status Codes

- **200 OK**: the resource was created successfully
- **400 Bad Request**: the resource is not valid or cannot be created at this time
- **409 Conflict**: the provided revision is not the current resource revision number

## Search and filtering

TBD.

[//]: # (TODO: describe search and filtering)

## Error Signaling

The services makes use of the HTTP Status Codes to report the outcome of each API call.  The status codes are
complemented by a consistent response data model for reporting client and system level failures.

Format
:   @@snip [error.json](../assets/api-reference/error.json)

Example
:   @@snip [error-example.json](../assets/api-reference/error-example.json)

While the format only specifies `code` and `message` fields, additional fields may be presented for additional
information in certain scenarios.