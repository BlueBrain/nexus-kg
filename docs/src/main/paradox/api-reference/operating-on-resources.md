# Operating on resources

Any resources in the system might be protected using an **access token**, provided by the HTTP header `Authorization: Bearer {access_token}`. Visit [Authentication](../../iam/api-reference/auth.html) in order to learn more about how to retrieve an access token.


All resources in the system share a base set of operations. Assuming a nexus deployment at
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
... where `{rev}` is the revision number, starting at `1`.

#### Status Codes

- **200 OK**: the resource revision is found and returned successfully
- **404 Not Found**: the resource revision was not found

### Fetch a resource in a specific output format

```
GET /v0/{address}?format={format}
```
... where supported `{format}` variants are `compacted`, `expanded` and `flattened`.
See the [JSON-LD](https://json-ld.org/spec/latest/json-ld/#expanded-document-form)
specifications for more details.

#### Status Codes

- **200 OK**: the resource revision is found and returned successfully
- **404 Not Found**: the resource was not found


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
... where `{collection_address}` is the address of the collection the resource belongs to.

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

All collection resources support full text search with filtering and pagination and present a consistent data model
as the result envelope.

General form:
```
GET /v0/{collection_address}
      ?q={full_text_search_query}
      &filter={filter}
      &fields={fields}
      &sort={sort}
      &from={from}
      &size={size}
      &deprecated={deprecated}
```
... where all of the query parameters are individually optional.

- `{collection_address}` is the selected collection to list, filter or search; for example: `/v0/data`, `/v0/schemas`,
`/v0/data/myorg/mydomain`
- `{full_text_search_query}`: String - can be provided to select only the resources in the collection that have
attribute values matching (containing) the provided token; when this field is provided the results will also include
score values for each result
- `{filter}`: JsonLd - a filtering expression in JSON-LD format (the structure of the filter is explained below)
- `{fields}`: a comma separated list of fields which are going to be retrieved as a result. The reserved keyword `all` retrieves all the fields.
- `{sort}`: a comma separated list of fields (absolute qualified URIs) which are going to be used to order the results. Prefixing a field with `-` will result into descending ordering on that field while prefixing it with `+` results in ascending ordering. When no prefix is set, the default behaviour is to assume ascending ordering..
- `{from}`: Number - is the parameter that describes the offset for the current query; defaults to `0`
- `{size}`: Number - is the parameter that limits the number of results; defaults to `20`
- `{deprecated}`: Boolean - can be used to filter the resulting resources based on their deprecation status

Filtering example while listing:

List Instances
: @@snip [list-instances.txt](../assets/api-reference/list-instances.txt) { type=shell }

List Instances Response
:   @@snip [instance-list.json](../assets/api-reference/instance-list.json)

Filtering example while searching (notice the additional score related fields):

Search and Filter Instances
: @@snip [search-list-instances.txt](../assets/api-reference/search-list-instances.txt) { type=shell }

Search and Filter Instances Response
:   @@snip [instance-list.json](../assets/api-reference/instance-search-list.json)

### Filter expressions

Filters follow the general form:

```
comparisonOp    = 'eq' | 'ne' | 'lt' | 'lte' | 'gt' | 'gte' | 'in'
logicalOp       = 'and' | 'or' | 'not' | 'xor'
op              = comparisonOp | logicalOp

path            = uri | property path
comparisonValue = literal | uri | {comparisonValue}

comparisonExpr  = json {
                      "op": comparisonOp,
                      "path": path,
                      "value": comparisonValue
                    }

logicalExpr     = json {
                      "op": logicalOp,
                      "value": {filterExpr}
                    }

filterExpr      = logicalExpr | comparisonExpr

filter          = json {
                      "@context": {...},
                      "filter": filterExpr
                    }
```
... which roughly means:

- a filter is a json-ld document
- with a user defined context
- that describes a filter value as a filter expression
- a filter expression is either a comparison expression or a logical expression
- a comparison expression contains a path property (a uri or a [property path](https://www.w3.org/TR/sparql11-query/#propertypaths)), the value to compare and an
  operator which describes how to compare that value
- a logical expression contains a collection of filter expressions joined together through a logical operator

Before evaluating the filter, the json-ld document is provided with an additional default context that overrides any
user defined values in case of collisions:
```
{
  "@context": {
    "nxv": "https://nexus.example.com/v0/voc/nexus/core/",
    "nxs": "https://nexus.example.com/v0/voc/nexus/search/",
    "path": "nxs:path",
    "op": "nxs:operator",
    "value": "nxs:value",
    "filter": "nxs:filter"
  }
}
```

Example filters:

Comparison
:   @@snip [simple-filter.json](../assets/api-reference/simple-filter.json)

With context
:   @@snip [simple-filter-with-context.json](../assets/api-reference/simple-filter-with-context.json)

Nested filter
:   @@snip [nested-filter.json](../assets/api-reference/nested-filter.json)

Property path
:   @@snip [property-path-filter.json](../assets/api-reference/property-path-filter.json)

Property path with context
:   @@snip [property-path-context-filter.json](../assets/api-reference/property-path-context-filter.json)

### Search response format

The response to any search requests follows the described format:

```json
{
  "total": {hits},
  "maxScore": {max_score},
  "results": [
    {
      "resultId": {resource_id},
      "score": {score_id},
      "source": {
        "@id": {id},
        "links": [
          {
            "rel": "self",
            "href": {self_uri}
          },
          {
            ...
          }
        ]
      }
    },
    {
      ...
    }
  ],
  "links": [
    {
      "rel": "self",
      "href": "https://nexus.example.com/v0/{address}?from=20&size=20"
    },
    {
      "rel": "next",
      "href": "https://nexus.example.com/v0/data?from=40&size=20"
    },
    {
      "rel": "previous",
      "href": "https://nexus.example.com/v0/data?from=0&size=20"
    }
  ]
}
```

...where

* `{hits}` is the total number of results found for the requested search.
* `{maxScore}` is the maximum score found across all hits.
* `{resource_id}` is the qualified id for one of the results.
* `{score_id}` is the score for this particular resource
* `{self_uri}` is the relationship to itself

The relationships `next` and `previous` at the top level offer discovery of more resources, in terms of navigation/pagination. 

The fields `{maxScore}` and `{score_id}` are optional fields and will only be present whenever a `q` query parameter is provided on the request.

## Error Signaling

The services makes use of the HTTP Status Codes to report the outcome of each API call.  The status codes are
complemented by a consistent response data model for reporting client and system level failures.

Format
:   @@snip [error.json](../assets/api-reference/error.json)

Example
:   @@snip [error-example.json](../assets/api-reference/error-example.json)

While the format only specifies `code` and `message` fields, additional fields may be presented for additional
information in certain scenarios.

## Resource discovery

We use [HATEOAS](https://en.wikipedia.org/wiki/HATEOAS) to provide resource discovery through the `links` array.

It comprises a list of objects with two fields: `rel` and `href`. 
Each of those objects can be translated as *'the current resource has a relationship of type `rel` with the resource `href`'*

### Examples
Instance resource example
:   @@snip [instance-links.json](../assets/api-reference/instance-links.json)

Search links example
:   @@snip [pagination-links.json](../assets/api-reference/pagination-links.json)