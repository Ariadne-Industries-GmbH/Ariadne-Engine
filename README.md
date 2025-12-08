# Ariadne Engine On-Premises Docker Setup Guide

## Introduction

The Ariadne Engine is an on-premises AI engine developed by **Ariadne Industries GmbH** that supports complex workflows with integrated microservices. This guide provides step-by-step instructions for setting up the engine using Docker.

## Prerequisites
- **Docker** (version 20.10 or higher)
- **Docker Compose** (version 1.29 or higher)
- GPU for optimal performance (for some models)
- At least 16GB of RAM

---

## Purpose & Dependencies

The Ariadne Engine is designed to run complex workflows with the following integrated microservices:
- **Local or cloud LLMs**: Supports llama.cpp and Ollama APIs, as well as Cloud LLMs provided by Ariadne Industries GmbH.
- **Vision-Language Models (VLMs)**: Automatically downloads models like SmolVLM2-500M-Video-Instruct if missing.
- **Embeddings server**: OpenAI-conform APIs tested with llama.cpp.
- **FalkorDB**: Knowledge graph storage and querying (`falkordb/falkordb:latest`).
- **Customizable Python-based scripting engine** for implementing custom flows.

---

## Configuration Files

You will need the following configuration files in your project root:

### model_config.json (Required)

Example:

```json
{
    "ministral-14b": { // This key corresponds to the display name of this model in the UI.
      "url": "http://localhost:44410",
      "service_type": "llama.cpp", // Supported types: 'ollama' or 'llama.cpp'.
      "temperature": 0.7
    },
  "ministral-3b": {
    "url": "http://localhost:44408",
    "service_type": "llama.cpp",
    "temperature": 0.7 // Optional parameter with default values (e.g., 0.7). Can be omitted if no customization is needed. More params added in future versions
  }
}

```

Missing this file will prevent the engine from configuring model endpoints properly.

### mcp_servers.json (Required)

Configure according to your needs (see [GitHub repository](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine) for examples).
Missing this file will prevent synchronization of MCP-based plugins into the database, leading to failures in plugin-dependent workflows.

---

## Model Setup

### Directory Structure

Create the following **required** directory structure in your project root:

```
project_root/
├── databases/
│   └── falkordb/         # FalkorDB data (auto-created)
├── models/
│   ├── docling/          # Docling models (auto-downloaded)
│   ├── faster-whisper/   # Faster-whisper models (auto-downloaded)
│   └── others/           # LLM, VLM, and Embeddings models
└── flow-scripts/         # Custom Python flow scripts (Required)
```

**Note:** The `./flow-scripts/` directory **must exist**. Missing this directory will prevent loading of custom flows.
The absence of any configuration files or directories mentioned above (`model_config.json`, `mcp_servers.json`, `docling`, `faster-whisper`) will cause the engine to fail during startup.

### Model Dependencies

Ensure the following models are available in their respective directories:

1. **LLM Models**:

  You can use additional LLM models by downloading them from Hugging Face and hosting them via `llama.cpp` or `ollama`. 
  Add configurations for these models in the `model_config.json` file under their desired display names.

  Models, that we currently recommend:
    - https://huggingface.co/unsloth/Ministral-3-14B-Instruct-2512-GGUF
    - https://huggingface.co/unsloth/Ministral-3-8B-Instruct-2512-GGUF
    - https://huggingface.co/unsloth/Ministral-3-3B-Instruct-2512-GGUF
    - Place the downloaded file in `./models/others/` if you follow the docker compose example of this repository.

2. **VLM Model**:
 
  You can use additional VLM models by downloading them from Hugging Face and hosting via `llama.cpp`.
  You can also point AAA_LLAMA_VLM_BASE_URL to any other OpenAI-compliant VLM API, but others then llama.cpp with SmolVLM2 is currently not tested.
  The flexibility to configure other VLMs (e.g., OpenAI-compliant APIs) is planned for future releases.
  Currently recommended model: SmolVLM2-500M-Video-Instruct (fast on CPU, suitable for document processing).
     - https://huggingface.co/ggml-org/SmolVLM2-500M-Video-Instruct-GGUF
     - Place the downloaded file in `./models/others/` if you follow the docker compose example of this repository.

3. **Embeddings Model**:
  
   You can use additional embedding models by downloading them from Hugging Face and hosting via `llama.cpp`. The `AAA_EMBEDDINGS_BASE_URL` in the Docker Compose file allows you to point to any `llama.cpp`-compatible server or OpenAI-compliant embedding APIs. (currently only llama.cpp is tested)
   Currently recommended model: BGE-M3, which is fast on CPU and suitable for text processing pipelines.
      - https://huggingface.co/bbvch-ai/bge-m3-GGUF
      - Place the downloaded file in `./models/others/` if you follow the repository's Docker Compose example.


4. **Faster-whisper & Docling Models**:
   These are automatically downloaded by the engine if missing from their respective directories (`./models/faster-whisper` and `./models/docling`).

---

## Docker Setup

### Usage Example (Backend)

Deploy using `docker-compose.yml`. Below is the **official configuration** based on `docker-compose-example.yml`:

```yaml
services:
  ariadne-engine:
    image: ariadneindustries/ariadne-engine:0.1.0-rc.3-on-prem
    restart: "no"
    user: "${HOST_UID}:${HOST_GID}"
    ports:
      - "44444:44444"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - ./databases:/app/aaa-bundle/databases
      - ./models/docling:/app/aaa-bundle/models/docling:ro
      - ./models/faster-whisper:/app/aaa-bundle/models/faster-whisper
      - ./flow-scripts:/app/aaa-bundle/flow-scripts:ro
      - ./mcp_servers.json:/app/aaa-bundle/mcp_servers.json:ro
      - ./model_config.json:/app/aaa-bundle/model_config.json:ro
    environment:
      - AAA_IDP_HOST=http://host.docker.internal:8000 # Defaults to "https://idp.ariadneanyverse.de" **but ignored if AAA_IDENTITY_SOURCE=integrated-idp**
      - AAA_LLAMA_VLM_BASE_URL=http://llama-vlm-server:44409/v1 # Required for VLM functionality. **Error if not set.**
      - AAA_EMBEDDINGS_BASE_URL=http://llama-cpp-embedding-server:44441/v1 # Required for embeddings functionality. **Error if not set.**
      - AAA_IS_PRIVACY_LEVEL_EXCLUSIVE_ENABLED=true # Defaults to true. Enables Exclusive LLMs based on model_config.json (default for local-only configurations). 
      - AAA_IS_PRIVACY_LEVEL_PREMIUM_ENABLED=false # Defaults to true. When enabled, use Premium-level privacy LLMs provided by Ariadne Industries.
      - AAA_IS_PRIVACY_LEVEL_STANDARD_ENABLED=false # Defaults to true. When enabled, use Standard-level privacy LLMs provided by Ariadne Industries.
      - AAA_ACTIVATE_WEB_SEARCH_SUBAGENT=false # Defaults to false. When disabled, web search subagent is inactive. Currently not supported locally.
      - AAA_FASTER_WHISPER_MODEL=small # default is large-v3-turbo. Small is good for most cases and fast on cpu
      - AAA_FALKORDB_HOST=falkordb # Defaults to "host.docker.internal". Must match the container name in `docker-compose.yml` if using a separate FalkorDB instance.
      - AAA_FALKORDB_PORT=6379 # Overrides default port of 44400. Only required if connecting to an external FalkorDB instance on non-default ports.
      - AAA_FALKORDB_PASSWORD=${AAA_FALKORDB_PASSWORD:-default} # Defaults to "default" if not set
      # - AAA_LOCAL_AAA_PORT=44444 # defaults to 44444 -> sets in this setup the port for all microservices
      - AAA_IDENTITY_SOURCE=integrated-idp # default is ariadne-anyverse, for local setup it should be integrated-idp.
      - AAA_WORKER_PROCESSES=2 # Overrides default of 4 workers. Each worker uses around **4GB of RAM**.
    networks:
      - ariadne-network
```

See `docker-compose-example.yml` in this repository for a full setup including `llama.cpp` servers.

### Environment Variables

Create a `.env` file with the following variables (adjust as needed):

```
HOST_UID=1000
HOST_GID=1000
GPU_LLM=<your path to your model>
CPU_LLM=<your path to your model>
```

---

## User Interface Setup

The Ariadne Engine UI webapp (`ariadne-webapp`) provides a frontend interface for visual interaction with agentic workflows, document processing, and knowledge graph management.

### Purpose & Dependencies

- **Purpose**: Visualize and interact with the engine's features, such as agentic tasks, model outputs, and long-term memory.
- **Dependencies**:
  - The UI webapp must always be paired with a running instance of `ariadne-engine`.

### Usage Example (Webapp)

Deploy the UI webapp using `docker-compose.yml`. Add the following service to your existing configuration:

```yaml
services:
  ariadne-webapp:
    image: ariadneindustries/ariadne-webapp:0.1.0-rc.2-web-bff
    restart: unless-stopped
    container_name: ariadnewebapp_010
    ports:
      - "43380:80"   # HTTP port for accessing the webapp
      - "44380:443"  # HTTPS port (optional, not properly supported, please use a Proxy server if you need TLS)
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      - AAA_ENDPOINT_URL=http://host.docker.internal:44444/endpoint # Required. Must point to the active Ariadne Engine backend.
      - IDP_BASE_URL=http://host.docker.internal:44444/integrated_idp # For **local setups**, MUST point to the integrated_idp of the engine (no other options available).
    networks:
      - ariadne-network
```

**Note**: The `AAA_ENDPOINT_URL` must be set to the same port as your Ariadne Engine backend (`44444`). Ensure that the `IDP_BASE_URL` points to the integrated IDP of the engine.

---

### Verification & Troubleshooting (UI + Backend)

#### 1. **Verify UI Connectivity**:

   - Open a browser and navigate to `http://localhost:43380` (or `https://localhost:44380`).
   - Ensure the UI loads correctly and displays data from the engine.

#### 2. **Check for Common Issues**:

   - If the webapp fails to load, verify that the Ariadne Engine backend is running (`docker ps`) and that environment variables are set correctly.
   - For authentication errors, ensure `IDP_BASE_URL` points to the integrated_idp of the engine (`44444/integrated_idp`).

#### 3. **Verify Backend Connectivity**:

   Check service logs for any errors:
   ```bash
   docker logs <container_name>
   ```

#### 4. Verify connectivity to the required ports:

   - Ariadne Engine: **44444**
   - LLM GPU Server: **44410**
   - LLM CPU Server: **44408**
   - VLM Server: **44409**
   - Embeddings Server: **44441**

#### 5. **Missing Configuration Files/Directories**:

   If any of the following are missing, the engine will fail to start or function properly:
   - `model_config.json` (Required for model endpoint configuration).
   - `mcp_servers.json` (Required for synchronizing MCP-based plugins into the database).
   - `./flow-scripts/` directory (Required for loading custom flow scripts).

---

### Starting the Services

1. Run the following command in the project root directory to start both the Ariadne Engine and the UI webapp:
   ```bash
   docker-compose up -d
   ```

2. Verify that all services are running properly:
   ```bash
   docker ps
   ```

---

## Additional Resources

- [Ariadne Engine GitHub Repository](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine) (Official documentation)
