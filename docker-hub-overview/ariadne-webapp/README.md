
## Ariadne Webapp – Docker Hub Repository Overview

**Repo Name**: `ariadneindustries/ariadne-webapp`  
**Current Tag**: `0.3.0-web-bff` 

**The Ariadne Webapp is a frontend UI for the Ariadne Engine, enabling users to interact with AI workflows, data exploration, and plugin management via a web interface.**

**Github Repository**: https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine

**Please consider the Github Repository for more information on how to setup the docker container and the Ariadne Engine**

---

### Purpose & Description

The **Ariadne Webapp** serves as the user-facing interface for the **Ariadne Engine**, providing an intuitive web-based dashboard to:

- Access and manage AI workflows  
- Interact with LLMs, VLMs, embeddings, and FalkorDB via the engine backend  
- Configure and run custom Python-based flows  
- Manage MCP-based plugins  
- Authenticate via integrated Identity Provider (IDP)  

It connects directly to the Ariadne Engine via the `AAA_ENDPOINT_URL` environment variable and uses the IDP for secure user authentication.

---

### Dependencies

To function, the Ariadne Webapp requires:

1. **Ariadne Engine Instance**:  
   - Must be running at the configured `AAA_ENDPOINT_URL` (e.g., `http://host.docker.internal:44444/endpoint`)  
   - The engine must have an active IDP service (e.g., `/integrated_idp` endpoint) for authentication

2. **Identity Provider (IDP)**:  
   - Access to the IDP via `IDP_BASE_URL` (e.g., `http://host.docker.internal:44444/integrated_idp`)  
   - Required for user login and session management

> ⚠️ Without an active Ariadne Engine and IDP, the Webapp will not function.

---

### Usage Example

Deploy using `docker-compose.yml`:

```yaml
services:
  ariadne-webapp:
    image: ariadneindustries/ariadne-webapp:0.1.0-rc.1-web-bff
    restart: unless-stopped
    container_name: ariadnewebapp_010
    ports:
      - "43380:80" # IMPORTANT: IT MUST BE THIS PORT. Currently it is hard coded in the webapp
      - "44380:443" # this is not needed. TLS is not directly supported inside the image. Please us a Proxy if needed
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      - AAA_ENDPOINT_URL=http://host.docker.internal:44444/endpoint
      - IDP_BASE_URL=http://host.docker.internal:44444/integrated_idp
    networks:
      - ariadne-network
```

> This configuration assumes the Ariadne Engine is running on `host.docker.internal:44444` and has an integrated IDP enabled.

---

### Maintenance Notes

- **Maintained by**: Ariadne Industries GmbH  
- **Update Frequency**: Managed internally; no public release schedule  
- **Versioning**: Tags follow `0.1.0-rc.x-web-bff` → `0.1.0-web-bff`

---

### Access Level and License

- **Public repository** on Docker Hub  
- **Custom license/usage permission** — users may pull and run, but cannot redistribute or build from source  
- **License Terms**: [https://www.ariadneanyverse.de/Annex_On-premise_License_Terms.pdf](https://www.ariadneanyverse.de/Annex_On-premise_License_Terms.pdf)