# Organizations

**`/organizations`** path which describes organizations. The following resources are allowed:

## Get
###**`GET /organizations/{id}`**

**Retrieves** the organization specified in **id**.

### Request path and query parameters
| Field         | Type          | Description                                                                                           |
| ------------- |-------------  | ---------------------------------------------                                                         |
| id            | String        | The unique identifier for the organization. It has to match the following regex: [a-z0-9]{3,5}        |

### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the organization.   |
| rev           | Long          | The current revision of the organization.     |
| deprecated    | Boolean       | whether the organization is deprecated or not.|
| *             | *             | The payload of the organization.              |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested organization does not exist.

### Example request
```bash
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/organizations/nexus"
```

## List
###**`GET /organizations?deprecated={deprecated}`**
**Retrieves a list** of organizations.

### Request path and query parameters
| Field         | Type                  | Description                                                                                                                   |
| ------------- |-------------          | ---------------------------------------------                                                                                 |
| deprecated    | Option[Boolean]       | A deprecated filter for the organization you want to list. If not set, it will return both deprecated and not deprecated organizations.   |
| *             |                       | [Pagination fields](basics.html#pagination-response).                                                                         |

### Status Codes
* **200 OK** - Request completed successfully.

### Response JSON-LD Object

The response format is the one defined in [Listing and querying response format](basics.html#listing-and-querying-response-format)

### Status Codes
* **200 OK** - Request completed successfully.

### Examples request
```bash
curl -v "https://bbp-nexus.epfl.ch/{environment}/{version}/organizations"
```

## Create
###**`PUT /organizations/{id}`**

**Creates** the organization specified in **id** with the provided payload.

### Request path and query parameters
| Field         | Type          | Description                                                                                           |
| ------------- |-------------  | ---------------------------------------------                                                         |
| id            | String        | The unique identifier for the organization. It has to match the following regex: [a-z0-9]{3,5}        |

### Request JSON-LD Object
A valid JSON-LD object

### Response JSON-LD Object
| Field         | Type          | Description                                                           |
| ------------- |-------------  | ---------------------------------------------                         |
| @id           | String        | The unique identifier for the organization.                           |
| rev           | Long          | The current revision of the organization.                             |

### Status Codes
* **201 Created** - Request completed successfully.
* **400 Bad Request** - The provided id path parameter does not match the regex [a-z0-9]{3,5}
* **409 Conflict** - The requested organization already exists.

### Example request
```bash
curl -v -X PUT -H "Content-Type: application/json" -d '{"description": "Nexus Organization"}' "https://bbp-nexus.epfl.ch/{environment}/{version}/organizations/nexus"
```


## Update
###**`PUT /organizations/{id}?rev={rev}`**
**Updates** the organization specified in **id** overriding the existing payload with the provided one.

### Request path and query parameters
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| id            | String        | The unique identifier for the organization. It has to match the following regex: [a-z0-9]{3,5}        |
| rev           | Long          | The current revision of the organization.     |

### Request JSON-LD Object
A valid JSON-LD object

### Response JSON-LD Object
| Field         | Type          | Description                                                           |
| ------------- |-------------  | ---------------------------------------------                         |
| @id           | String        | The unique identifier for the organization.                           |
| rev           | Long          | The current revision of the organization.                             |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found** - The requested organization does not exist.
* **409 Conflict** - The provided rev query parameter does not match the current revision.

### Example request
```bash
curl -v -X PUT -H "Content-Type: application/json" -d '{"description": "Nexus Organization", "label": "Updated"}' "https://bbp-nexus.epfl.ch/{environment}/{version}/organizations/nexus?rev=1"
```


## Deprecate
###**`DELETE /organizations/{id}?rev={rev}`**

**Deprecates** the organization specified in **id**.

### Request path and query parameters
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| id            | String        | The unique identifier for the organization. It has to match the following regex: [a-z0-9]{3,5}        |
| rev           | Long          | The current revision of the organization.     |

### Response JSON-LD Object
| Field         | Type          | Description                                   |
| ------------- |-------------  | --------------------------------------------- |
| @id           | String        | The unique identifier for the organization.   |
| rev           | Long          | The current revision of the organization.     |

### Status Codes
* **200 OK** - Request completed successfully.
* **404 Not Found**
    * The requested organization does not exist.
    * The request query parameter rev is missing.
* **409 Conflict** - The provided rev query parameter does not match the current revision.
* **400 Bad Request** - The organization is deprecated.

### Example request
```bash
curl -v -X DELETE "https://bbp-nexus.epfl.ch/{environment}/{version}/organizations/nexus?rev=2"
```