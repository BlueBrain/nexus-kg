curl -XPUT -H "Content-Type: application/json" "https://nexus.example.com/v0/organizations/myorg"
    -d '{"@context": {"schema": "http://schema.org/"}, "schema:name": "myorg"}'