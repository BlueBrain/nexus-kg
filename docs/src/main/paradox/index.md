# KG API Reference

@@@ note { title="Base Uri: https://bbp-nexus.epfl.ch/{environment}/{version}/" }
* Environments: dev, staging, prod
* Versions: v0
@@@


@@@ index

* [basics](basics.md)
* [organizations](organizations.md)
* [domains](domains.md)
* [schemas](schemas.md)
* [instances](instances.md)

@@@


The KG API expose a series of resources grouped into different categories:

* **@ref:[Organizations](organizations.md)**: Resources related to the top level structure on the system.
* **@ref:[Domains](domains.md)**: Resources related to the next level structure on the system. This level can be understood as groups inside an organization.
* **@ref:[Schemas](schemas.md)**: Resources related to managing schemas which are a set of rules and constrains that apply to instances.
* **@ref:[Instances](instances.md)**: Resources related to managing instances. An instance contains a payload which is validated against it's schema.


