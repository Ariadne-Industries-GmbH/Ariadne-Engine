# Ariadne Engine — Docker Hub Repository Overview

**Repository**: `ariadneindustries/ariadne-engine`  
**Current Release Tag**: `1.0.0-on-prem`  
**Related Frontend Image**: `ariadneindustries/ariadne-webapp:1.0.0-web-bff`  
**GitHub Repository**: https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine

Please use the GitHub repository as the primary reference for deployment details, configuration examples, release assets, and setup guidance.

---

## Purpose & Description

The Ariadne Engine is a private, self-hosted AI backend developed by **Ariadne Industries GmbH** for autonomous data workflows, agentic automation, and knowledge-centric AI processing.

It is designed as a **meta-system for data intelligence** that can orchestrate:

- LLMs, VLMs, and embedding models through `model_config.json`
- autonomous subagents and background "dreaming" runtimes
- knowledge-graph and long-term-memory backends (FalkorDB or the embedded Ladybug core)
- workspace file management with per-user access policies
- MCP-based tools, skills, and custom Python flow scripts
- speech recognition (standalone faster-whisper service) and multimodal processing

The current `1.0.0` generation builds on the `0.3.x` line and adds the new embedded **Ladybug** graph core (v0.19) with **automatic one-way Kuzu migration**, a **standalone Whisper speech-recognition service** (`AAA_FASTER_WHISPER_SERVICE_MODE`), **workspace file management** support, **per-user local automation policies**, and **hardened worker health checks** with structured diagnostics. The engine itself now runs as a **modular FastAPI monolith**: a single process serves the API, flows, agent runtime, and long-term memory — with fewer moving parts and simplified configuration.

---

## Deployment Scope

The Docker image provides the **engine backend**. A full local deployment may also include:

- `ariadneindustries/ariadne-webapp:1.0.0-web-bff` for the web frontend
- one or more local inference services such as `llama.cpp`, `vLLM`, or Ollama-compatible endpoints
- optionally `falkordb/falkordb:latest` when using FalkorDB instead of the embedded Ladybug core

For the canonical setup, use [`docker-compose-example.yml`](../docker-compose-example.yml) in this repository.

---

## Model & Service Dependencies

The engine supports flexible model routing via `model_config.json`. Current repository guidance references:

- `llama.cpp`
- `vLLM`
- Ollama
- OpenAI-compatible APIs
- Ariadne-hosted cloud endpoints for supported privacy tiers

Typical Docker setups include:

1. **Inference endpoints**  
   At least one configured model endpoint for exclusive/local model usage. Any OpenAI-compatible model server can be used if configured correctly in `model_config.json`.

2. **Embeddings endpoint**  
   `AAA_EMBEDDINGS_BASE_URL` is required for embeddings functionality. The compose example uses a dedicated `llama.cpp` embeddings server.

3. **Graph backend**  
   Use `AAA_GRAPHITI_BACKEND=falkordb` with a FalkorDB container or external instance, or `AAA_GRAPHITI_BACKEND=ladybug` to use the embedded per-user Ladybug databases inside `./databases`. The legacy value `kuzu` selects the embedded backend and migrates existing Kuzu databases to Ladybug automatically (one-way, with backup).

4. **Speech recognition (whisper)**  
   By default (`AAA_FASTER_WHISPER_SERVICE_MODE=integrated`), the engine starts the faster-whisper service itself as a background service. Set `AAA_FASTER_WHISPER_SERVICE_MODE=external` and `AAA_FASTER_WHISPER_BASE_URL` to use an externally managed service instead.

5. **Optional frontend**  
   The engine can run standalone, but typical browser-based usage includes the separate Ariadne Webapp container.

---

## Required and Optional Mounted Files

The current example setup uses bind mounts under `/app/aaa-bundle/...`.

Common required mounts:

- `./databases -> /app/aaa-bundle/databases`
- `./models/docling -> /app/aaa-bundle/models/docling`  

Common recommended mounts:

- `./models/faster-whisper -> /app/aaa-bundle/models/faster-whisper`
- `./model_config.json -> /app/aaa-bundle/model_config.json` for exclusive/local model routing

Common optional mounts:

- `./flow-scripts -> /app/aaa-bundle/flow-scripts`
- `./skills -> /app/aaa-bundle/skills`
- `./mcp_servers.json -> /app/aaa-bundle/mcp_servers.json`
- `./local_automation_policy.json -> /app/aaa-bundle/config/local_automation_policy.json`
- `./dreaming_runtime_config.json -> /app/aaa-bundle/dreaming_runtime_config.json`

If an optional file mount is kept in `docker-compose.yml`, create the file on the host first, even if it is empty.

---

## Docker Compose Guidance

The repository currently recommends **long-form bind mounts** with `create_host_path: false` instead of short volume syntax.

This matters because Docker may otherwise create missing host paths as `root:root`, which can break write access for the engine runtime. Create the host directories and files yourself before starting the stack.

The example deployment also includes:

- `extra_hosts: host.docker.internal:host-gateway`
- `HOST_UID` / `HOST_GID` environment variables for host ownership alignment
- `UMASK=0002`

For terminal sandboxing and local command execution inside Docker, the compose example further requires:

- `privileged: true`
- `security_opt` including `apparmor=unconfined`
- `security_opt` including `seccomp=unconfined`

These settings are specifically relevant when using the engine's sandboxed terminal tooling (bubblewrap inside the container).

---

## Example Engine Service

```yaml
services:
  ariadne-engine:
    image: ariadneindustries/ariadne-engine:1.0.0-on-prem
    restart: unless-stopped
    ports:
      - "44444:44444"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    security_opt:
      - apparmor=unconfined
      - seccomp=unconfined
    privileged: true
    volumes:
      - type: bind
        source: ./databases
        target: /app/aaa-bundle/databases
        bind:
          create_host_path: false
      - type: bind
        source: ./models/docling
        target: /app/aaa-bundle/models/docling
        read_only: true
        bind:
          create_host_path: false
      - type: bind
        source: ./models/faster-whisper
        target: /app/aaa-bundle/models/faster-whisper
        bind:
          create_host_path: false
      - type: bind
        source: ./model_config.json
        target: /app/aaa-bundle/model_config.json
        read_only: true
        bind:
          create_host_path: false
    environment:
      - HOST_UID=${HOST_UID:-1000}
      - HOST_GID=${HOST_GID:-1000}
      - UMASK=0002
      - AAA_IDENTITY_SOURCE=integrated-idp
      - AAA_EMBEDDINGS_BASE_URL=http://llama-cpp-embedding-server:44441/v1
      - AAA_IS_PRIVACY_LEVEL_EXCLUSIVE_ENABLED=true
      - AAA_IS_PRIVACY_LEVEL_PREMIUM_ENABLED=false
      - AAA_IS_PRIVACY_LEVEL_STANDARD_ENABLED=false
      - AAA_GRAPHITI_BACKEND=falkordb
      - AAA_FALKORDB_HOST=falkordb
      - AAA_FALKORDB_PORT=6379
      - AAA_FALKORDB_PASSWORD=${AAA_FALKORDB_PASSWORD:-default}
      - AAA_FASTER_WHISPER_MODEL=small
      - AAA_FASTER_WHISPER_DEVICE=cpu
      - AAA_WORKER_PROCESSES=2
```

See [`docker-compose-example.yml`](../docker-compose-example.yml) for the full multi-service setup including Webapp, FalkorDB, and the embeddings server.

---

## Important Environment Variables

Common variables from the current example:

- `AAA_IDENTITY_SOURCE=integrated-idp`
- `AAA_EMBEDDINGS_BASE_URL=...`
- `AAA_GRAPHITI_BACKEND=ladybug|falkordb`
- `AAA_FALKORDB_HOST`, `AAA_FALKORDB_PORT`, `AAA_FALKORDB_PASSWORD`
- `AAA_FASTER_WHISPER_MODEL`
- `AAA_FASTER_WHISPER_DEVICE`
- `AAA_FASTER_WHISPER_SERVICE_MODE` (`integrated` default / `external` with `AAA_FASTER_WHISPER_BASE_URL`)
- `AAA_WORKER_PROCESSES`
- `AAA_IS_PRIVACY_LEVEL_EXCLUSIVE_ENABLED`
- `AAA_IS_PRIVACY_LEVEL_PREMIUM_ENABLED`
- `AAA_IS_PRIVACY_LEVEL_STANDARD_ENABLED`
- `AAA_WORKER_HEALTHCHECK_TIMEOUT_SECONDS` (default `60`), `AAA_WORKER_HEALTHCHECK_STARTUP_GRACE_SECONDS` (default `30`), `AAA_WORKER_HEALTHCHECK_MAX_CONSECUTIVE_FAILURES` (default `2`)
- `AAA_ALLOW_USER_LOCAL_AUTOMATION_POLICY_MUTATIONS`, `AAA_ALLOW_USER_MCP_REGISTRY_MUTATIONS`
- optional cloud/provider keys such as `AAA_OPENAI_API_KEY`, `AAA_MISTRAL_PLATFORM_API_KEY`, or `AAA_FIREWORKS_API_KEY`

Additional optional runtime settings shown in the repository include dreaming runtime configuration via either mounted JSON config or environment-variable fallbacks, and Ladybug embedded-database tuning variables (`AAA_LADYBUG_*`).

---

## Maintenance Notes

- **Maintained by**: Ariadne Industries GmbH
- **Repository role**: Public setup, configuration, documentation, and release-assets repository for the Ariadne Engine
- **Current public Docker example**: `ariadneindustries/ariadne-engine:1.0.0-on-prem`
- **Related frontend image**: `ariadneindustries/ariadne-webapp:1.0.0-web-bff`

Versioning shown in this repository currently reflects the `1.0.0` release line (previous: `0.3.1`).

---

## Access Level and License

- **Public repository** on Docker Hub
- **Custom license / usage permission**

Refer to the current license terms and official deployment information from Ariadne Industries:

- https://www.ariadneanyverse.de/Annex_On-premise_License_Terms.pdf
- https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine
