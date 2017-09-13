# API Basics

## Pagination

When you want to request a resource which aggregates several items in the response, the amount of items returned could exceed the maximum allowed. That's when pagination has to be used.

### Pagination request
Pagination is requested using the following parameters `from` and `size`.

| Query parameter  | Type          | Description                                                                    | Default value |
| -------------    |-------------  | ---------------------------------------------                                  | --------------|
| from             | Long          | The offset to describe in which page to start retrieving items.                | 0             |
| size             | Int           | The amount of items retrieved per page.                                        | 20            |    

A resource with pagination can be requested as follows: **`/{resource}?from={offset}&size={size}`**

## Listing and querying response format

When you want to request a resource which aggregates several items in the response, the response format will follow structure defined here:

| Field            | Type                                   | Description                                                | 
| -------------    |-------------                           | ---------------------------------------------              |
| *                |                                        | [Pagination fields](#pagination-response).                 |
| maxScore         | Option[Float]                          | The maximum score from each individual result.             |
| results          | [List[Result]](#result-json-ld-object) | The list of results.                                       |

#### Result JSON-LD Object
| Field         | Type                                  | Description                                                                                            |
| ------------- |-------------                          | ---------------------------------------------                                                          |
| resultId      | String                                | The fully qualified URI of this result.                                                                |
| score         | Option[String]                        | The score for this particular result.                                                                  |
| source        | [Source](#source-json-ld-object)      | Specific information about the result.                                                                 |

#### Source JSON-LD Object
| Field         | Type          | Description                                                                                                                    |
| ------------- |-------------  | ---------------------------------------------                                                                                  |
| @id           | String        | The unique identifier for the resource                                                                                         |
| links         | [List[Link]](#link-json-ld-object)    | [HATEOAS](https://en.wikipedia.org/wiki/HATEOAS) links for discoverability of other related resources. |

#### Link JSON-LD Object
| Field         | Type          | Description                                                               |
| ------------- |-------------  | ---------------------------------------------                             |
| rel           | String        | The type of relationship between this href link and the returned Source. The relationships **next** and **previous** are used for pagination purposes.  |
| href          | String        | The qualified URI for this relationship.                                  |
