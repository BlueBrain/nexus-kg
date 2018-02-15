@@@ index

* [operating on resources](operating-on-resources.md)
* [queries](queries.md)
* [organizations](organizations.md)
* [domains](domains.md)
* [contexts](contexts.md)
* [schemas](schemas.md)
* [instances](instances.md)

@@@

# API Reference

As with all Nexus services the KnowledgeGraph exposes a RESTful interface over HTTP(S) for synchronous communication. 
The generally adopted transport format is JSON based, specifically [JSON-LD](https://json-ld.org/).

The service operates on 4 types of resources: @ref:[Organizations](organizations.md), @ref:[Domains](domains.md),
@ref:[Schemas](schemas.md) and @ref:[Instances](instances.md), nested as described in the diagram below.

![Resources](../assets/api-reference/resources.png)

## Resource Lifecycle

All resources in the system generally follow the very same lifecycle, as depicted in the diagram below.  Changes to the
data (creation, updates, state changes) are recorded into the system as revisions.  This functionality is made possible
by the [event sourced](https://martinfowler.com/eaaDev/EventSourcing.html) persistence strategy that is used in the
underlying primary store of the system.

![Resource Lifecycle](../assets/api-reference/resource-lifecycle.png)

Data is never removed from the system, but rather is marked as deprecated.  Depending on the type of resource, the
_deprecation_ flag may have various semantics:

* Organizations: the resource itself and sub-resources cannot be updated
* Domains: the resource itself and sub-resources cannot be updated
* Schemas: the resource itself cannot be updated and new instances conformant to it cannot be created
* Instances: the resource itself cannot be updated and binaries cannot be replaced

Future policies may use this flag to determine if or when the deprecated data may be archived.
