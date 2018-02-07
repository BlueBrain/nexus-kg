# Queries

Queries resources are rooted in the `/v0/queries` collection. 

**Note:** You might need to add `-H "Authorization: Bearer ***"` if you are attempting to operate on a protected resource using the curl examples.

## Create a query
```
POST /v0/queries/{collection_address}?from={from}&size={size}
{...}
```
... where all of the query parameters are individually optional.

- `{collection_address}` is the selected collection to list, filter or search; for example: `/myorg/mydomain`, `/`,
`/myorg/mydomain/myschema/v1.0.0`
- `{from}`: Number - is the parameter that describes the offset for the current query; defaults to `0`
- `{size}`: Number - is the parameter that limits the number of results; defaults to `20`

The json payload is defined as...
```
{
  "@context": { ... },
  "filter": { ... },
  "q": "fullTextSearchQuery",
  "resource": "instances|schemas|contexts|domains|organizations",
  "deprecated": false|true,
  "published": false|true,
  "format": "compacted|expanded|flattened|framed|default",
  "fields": [ "uri1", ..., "uriN"],
  "sort": [ "uri1", ..., "uriN" ]
}
```

... where all of the query parameters are individually optional.

- `@context`: JsonLd - can be provided to define extra prefix mappings. By default, the search core context will be used. If this filed is provided, its content will be merged with the search core context.
- `filter`: Filter - can provide the filtering expression, explained [here](./operating-on-resources.html#filter-expressions)
- `q`: String - can be provided to select only the resources in the collection that have
       attribute values matching (containing) the provided token; when this field is provided the results will also include
       score values for each result
- `resource`: String - can be provided to define the target type of resource. Default value: instances
- `deprecated`: Boolean - can be provided to filter the resulting resources based on their deprecation status
- `published`: Boolean - can be provided to filter the resulting resources based on their published status
- `format`: String - can be provided to define the output format of the response. Default value: default
- `fields`: List[String] - can be provided to define which fields to show in the response. The reserved keyword `all` retrieves all the fields.
- `sort`: List[String] - can be provided to define the sorting criteria of the results. fields to show in the response.
          Prefixing a field with `-` will result into descending ordering on that field while prefixing it with `+` results in ascending ordering. When no prefix is set, the default behaviour is to assume ascending ordering..

#### Status Codes

- **308 PermanentRedirect**: it provides with the `Location` HTTP Header where the created query can be executed
- **400 Bad Request**: the payload provided for the query is invalid

#### Example

Filtering example while listing. Note that the response shown is the response after following the `Location` HTTP Header link

Create query
: @@snip [list-instances-post.txt](../assets/api-reference/list-instances-post.txt) { type=shell }

Query payload
: @@snip [list-instances-payload.json](../assets/api-reference/list-instances-payload.json)

Follow redirect Response
:   @@snip [instance-list.json](../assets/api-reference/instance-list.json)

Filtering example while searching (notice the additional score related fields):

Create query
: @@snip [search-list-instances-post.txt](../assets/api-reference/search-list-instances-post.txt) { type=shell }

Query payload
: @@snip [search-list-instances-payload.json](../assets/api-reference/search-list-instances-payload.json)

Follow redirect Response
:   @@snip [instance-list.json](../assets/api-reference/instance-search-list.json)



## Execute a query

```
GET /v0/queries/{queryId}?from={from}&size={size}
```

- `{queryId}`: UUID - is the unique identifier of a previously created query.

- `{from}`: Number - is the parameter that describes the offset for the current query; defaults to `0`
- `{size}`: Number - is the parameter that limits the number of results; defaults to `20`

#### Status Codes

- **200 OK**: the query has been found and executed correctly
- **404 Not Found**: the query was not found
- **400 Bad Request**: the path for the query is not valid

#### Example

Execute query
: @@snip [list-instances-post.txt](../assets/api-reference/list-instances-post.txt) { type=shell }


Query Response
:   @@snip [instance-list.json](../assets/api-reference/instance-list.json)
