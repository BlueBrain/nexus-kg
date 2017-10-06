# Domains
**`/domains/{orgId}`** path describes domains inside an specific organization. The following resources are allowed:

@@@ note
Old endpoint **`/organizations/{orgId}/domains`** is now deprecated and will be removed in a future release. Please switch to using new endpoints described below. 
@@@
## Get
###**`GET /domains/{orgId}/{id}`**
**Retrieves** the domain specified in **id** from the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                   |
| ------------- |-------------  | ---------------------------------------------                                                                                 |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| id            | String        | The unique identifier for the domain for this particular organization. It has to match the following regex: ([a-zA-Z0-9]+) |

### Response JSON-LD Object
| Field         | Type          | Description                                                               |
| ------------- |-------------  | ---------------------------------------------                             |
| @id           | String        | The unique identifier for the domain for this particular organization. |
| rev           | Long          | The current revision of the domain.                                       |
| deprecated    | Boolean       | whether the domain is deprecated or not.                                  |
| description   | String        | A description for the domain.                                             |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested domain does not exist.

### Example request
```bash
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/domains/nexus/core"
```

## List
###**`GET /domains?deprecated={deprecated}`**
###**`GET /domains/{orgId}?deprecated={deprecated}`**
**Retrieves a list** of domains

### Request path and query parameters
| Field         | Type                  | Description                                                                                                                   |
| ------------- |-------------          | ---------------------------------------------                                                                                 |
| orgId         | Option[String]        | The organization identifier to which the listing domain belong.                                                               | 
| deprecated    | Option[Boolean]       | A deprecated filter for the domain you want to list. If not set, it will return both deprecated and not deprecated domains.   |
| *             |                       | [Pagination fields](basics.html#pagination-response).                                                                         |

### Status Codes
* **200 OK** - Request completed successfully.

### Response JSON-LD Object

The response format is the one defined in [Listing and querying response format](basics.html#listing-and-querying-response-format)

### Status Codes
* **200 OK** - Request completed successfully.

### Examples request
```bash
# Filtering by organization nexus
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/domains/nexus"
```

## Create
###**`PUT /domains/{orgId}/{id}`**
**Creates** the domain specified in **id** from the organization **orgId** with the provided payload.

### Request path and query parameters
| Field         | Type          | Description                                                                                                                |
| ------------- |-------------  | ---------------------------------------------                                                                              |
| orgId         | String        | The unique identifier for the organization                                                                                 | 
| id            | String        | The unique identifier for the domain for this particular organization. It has to match the following regex: ([a-zA-Z0-9]+) |

### Request JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| description   | String        | A description for the domain.                 |

### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the domain.         |
| rev           | Long          | The current revision of the domain.           |

### Status Codes
* **201 Created** - Request completed successfully.
* **400 Bad Request** - The provided id path parameter does not match the regex ([a-zA-Z0-9]+)
* **409 Conflict** - The requested domain already exists.

### Example request
```bash
curl -v -X PUT -H "Content-Type: application/json" -d '{"description": "Core Domain"}' "https://bbp-nexus.epfl.ch/{environment}/{version}/domains/nexus/core"
```

## Deprecate
###**`DELETE /domains/{orgId}/{id}?rev={rev}`**
**Deprecates** the domain specified in **id** from the organization **orgId**.

### Request path and query parameters
| Field         | Type          | Description                                                                                           |
| ------------- |-------------  | ---------------------------------------------                                                         |
| orgId         | String        | The unique identifier for the organization                                                                                    | 
| id            | String        | The unique identifier for the domain for this particular organization. It has to match the following regex: ([a-zA-Z0-9]+) |
| rev           | Long          | The current revision of the domain.           |

### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the domain.         |
| rev           | Long          | The current revision of the domain.           |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found**
    * The requested organization does not exist.
    * The requested domain does not exist.
    * The request query parameter rev is missing.
* **400 Bad Request** - The domain is deprecated.
* **409 Conflict** - The provided rev query parameter does not match the current revision.

### Example request
```bash
curl -v -X DELETE "https://bbp-nexus.epfl.ch/{environment}/{version}/domains/nexus/core?rev=1"
```