curl -v -XPATCH -H "Content-Type: application/json" "https://nexus.example.com/v0/schemas/myorg/mydom/myschema/v1.0.0/config?rev=2"
    -d '{"published": true}'