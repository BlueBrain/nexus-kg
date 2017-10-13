curl -v -XPATCH -H "Content-Type: application/json" "https://nexus.example.com/v0/schemas/myorg/mydom/myschema/1.0.0/config?rev=2"
    -d '{"published": true}'