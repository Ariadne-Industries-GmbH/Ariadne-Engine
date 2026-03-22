# 🚀 Ariadne Engine: Your Private AI Data Intelligence

<div align="center">
  <img src="./images/ariadne_anyverse_agents.gif" alt="Ariadne Engine Agents"/>
</div>

**Automate complex AI workflows on your data — with your private AI operating system**

The **Ariadne Engine** is a **meta-system for data intelligence**, designed to **autonomously process and connect your data** using AI agents, knowledge graphs, and modular workflows. Unlike traditional LLM interfaces, it abstracts away the complexity of managing models and interactions — so you can focus on **extracting actionable insights from your data**.

Built by **Ariadne Industries GmbH**, it is the technical backbone of the **[Ariadne Anyverse](https://www.ariadneanyverse.de)**, a digital ecosystem where **data sovereignty meets AI automation**.

---

## 🚀 Release v0.2.0: Advanced Skill System & Enhanced Automation

### What's New Since v0.1.0?

🔥 The Ariadne Engine has evolved significantly with **v0.2.0**, introducing a **powerful skill-based architecture** and numerous enhancements:

✅ **Skill-Based Agent System**: New modular skill system with dynamic loading and management \
✅ **MCP-Based Skills**: Skills can now be backed by MCP servers for seamless integration \
✅ **Skill Builder**: Create and manage custom skills directly through the engine \
✅ **Enhanced Context Retrieval**: Improved tools for retrieving and managing contextual information \
✅ **Advanced Job Scheduling**: Better scheduling capabilities with metadata and notifications \
✅ **Document Processing**: Enhanced document extraction with VLM integration for images \
✅ **System Prompt Improvements**: Better LLM guidance with skill-aware prompts

> **Stay tuned!** Follow our [GitHub](https://github.com/Ariadne-Industries-GmbH) or [LinkedIn](https://linkedin.com/company/ariadne-industries) for updates.

---

## 🎯 Who Is This For?

The Ariadne Engine is tailored for:

- **Developers** who want to build **autonomous AI workflows** without reinventing the wheel.
- **Enterprises** needing **GDPR-compliant, on-premises data intelligence**.
- **Power users** who want to **unlock value from their data** using agentic automation.

---

## 🔥 Why a Meta-System?

Most LLM tools require you to manage models, agents, and workflows manually. The Ariadne Engine **handles the complexity for you**:

✅ **Agentic automation**: Internal agents interact with LLMs, VLMs, Speech Recognition and embeddings — **you define the workflows, not the infrastructure**. \
✅ **Knowledge graphs**: Your data becomes a **connected intelligence layer**, enabling long-term reasoning across documents, APIs, and internal systems. \
✅ **Full control**: Deploy on-premises for maximum privacy or use our cloud version (hosted in Germany, GDPR-compliant). \
✅ **Optimized for Technological Sovereignty**:
- **Battle-tested with local LLMs** running on consumer hardware.
- **No forced cloud dependency**: Works with open-source models (Ollama, llama.cpp) and avoids vendor lock-in.
- **Local, file-based storage system** for maximum portability
- **CPU-friendly for specific workflows**: use the scripting engine to build special data flows with multimodal AI models.

> **Not just another LLM frontend**: The engine is designed to **orchestrate multi-modal AI workflows** — think of it as a **private AI operating system** for your data.

---

## 🎯 Core Capabilities

| Feature               | Description                                                                 |
|-----------------------|-----------------------------------------------------------------------------|
| **Skill-Based Agents** | Modular skills system with dynamic loading and MCP integration. Define custom skills and compose them for complex workflows. |
| **Knowledge Graphs**  | Your data is structured as a **dynamic intelligence layer**, enabling cross-references and long-term context. *(No raw storage -> connected insights.)* |
| **Modular AI Integration** | Supports LLMs, VLMs, and embeddings via APIs (e.g., Ollama, llama.cpp, or cloud providers). Configure once, use flexibly. |
| **Privacy by Design**  | Local-only processing or cloud privacy tiers — your choice. Hosted in Germany for compliance. |
| **Job Scheduling** | Automate tasks and workflows with time-based triggers and notifications. |

---

## 🦸 The Engine's Superpowers

### 📦 What's Inside?

The Ariadne Engine comes with:
1. **Pre-configured skill-based workflows**: Start automating tasks with built-in skills like document processing, context retrieval, and job scheduling.
2. **Knowledge graph infrastructure**: FalkorDB-powered storage for your data as a connected intelligence layer.
3. **Meta Agents**: Autonomous AI agents that interact with your data, models, and external systems on your behalf.
4. **Skill Builder**: Create and manage custom skills directly through the engine interface.
5. **UI Webapp**: A dashboard to visualize workflows, manage knowledge graphs, and chat with agents.

### 🔒 Privacy & Control

Choose how your data is processed:
- **Local-only**: All workflows run on your hardware.
- **Cloud privacy tiers**: Use our GDPR-compliant cloud LLMs (hosted in Germany) while keeping sensitive data on-premises.

> **Test the minimal cloud version for a first look**: [https://ai.ariadneanyverse.de](https://ai.ariadneanyverse.de)

---

## 🛠️ Get Started in 5 Minutes

> **Important**: This public repository is meant for **setup, configuration, release assets and documentation** of the Ariadne Engine. It is **not the open source codebase of the engine itself**. 

### Deployment Options

The Ariadne Engine offers two deployment methods:

#### 1. Native Binary Deployment (New native Windows & Linux release in v0.2.0 - Recommended for most users)

For users who want the fastest path to a working installation, the recommended starting point in `v0.2.0` is the **native Windows / Linux binary**. You can download the release, start the executable, follow the launcher, and let the engine prepare the runtime for you.

**Requirements:**
- [ ] **Windows 10/11** or **Linux** (Ubuntu 24.04+ recommended)
- [ ] **GPU recommended** (NVIDIA CUDA or Vulkan supported)
- [ ] **6GB RAM minimum** (engine itself)
- [ ] **10GB+ free disk space** (for model downloads and runtime assets)

**Features:**
- ✅ **Automatic model downloads** - The launcher automatically downloads required models (Ministral 3B/8B/14B, BGE-M3 embeddings, faster-whisper)
- ✅ **Interactive setup wizard** - Guided configuration for privacy mode, AI brain selection, and hardware optimization
- ✅ **Companion app integration** - Optional desktop app launcher
- ✅ **Hardware detection** - Automatic GPU/CPU detection and optimization
- ✅ **Manual mode** - Advanced users can manage their own `model_config.json` and inference stack

**Download**: Get the latest binary from our [releases page](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine/releases).

**How to start the native version**

- On **Windows**, you can simply **double-click** `ariadne_engine.exe` or start it from PowerShell:

```powershell
.\ariadne_engine.exe
```

- On **Linux**, you can start the binary from your file manager or from a terminal:

```bash
./ariadne_engine
```

**First Run**: The binary launcher will guide you through setup, including:
1. Language selection
2. Privacy mode configuration (`Local + Cloud`, `Local only`, `Cloud only`, or `Manual`)
3. AI Brain selection (automatic hardware detection and optimization)
4. Speech recognition model selection
5. Automatic model downloads (if needed)
6. Environment configuration

> **First run takes time**: Initial setup includes automatic model downloads (~10-30 min depending on internet speed and model selection).

**What the native launcher creates and manages**

The native deployment prepares the runtime next to the executable. Depending on your setup choices and first use, the directory will typically contain:

```
engine_directory/
├── databases/
│   └── falkordb/                     # FalkorDB data if you use graph features
├── models/
│   ├── docling/                      # Docling models (auto-downloaded)
│   ├── faster-whisper/               # faster-whisper models (auto-downloaded)
│   └── others/                       # LLM, VLM, and embedding models
├── llama_cpp/                        # Managed llama.cpp server binaries
├── flow-scripts/                     # Optional custom Python flow scripts
├── skills/                           # Optional global skills
├── model_config.json                 # Auto-generated or manual, depending on setup
├── mcp_servers.json                  # Optional, created empty when needed
├── startup_runtime_setup.json        # Launcher configuration and setup state
├── llama_server_launcher_config.json # Auto-generated local inference config
└── ariadne_engine_app                # Optional companion app
```

> **Background**: In `v0.2.0`, the launcher can generate and maintain large parts of the local runtime automatically. This is the main reason why the native binary is now the easiest path for users who want a self-hosted AI engine without manually wiring every service.

**Starting the engine after setup**

The launcher offers several options:
- **Start Server**: Start the engine backend only
- **Start Server and App**: Start the engine and the optional companion app together
- **Expert Terminal Mode**: Open a shell in the engine directory for advanced inspection and manual control
- **Run Setup Again**: Re-run the setup flow and change privacy mode, AI Brain choice, or speech model

You can also start the backend directly after setup:

```bash
./ariadne_engine --server
```

On Windows:

```powershell
.\ariadne_engine.exe --server
```

**Manual Mode for Power Users**

If you select `Manual` mode, the launcher does **not** build a managed local model setup for you. Instead, it expects you to manage your own inference stack and point the engine to it manually.

This mode is intended for users who want to:
- use their own `llama.cpp` servers
- use `Ollama`
- use another supported provider
- maintain `model_config.json` themselves
- skip automatic local model downloads entirely

> **Important**: In `Manual` mode, starting the server stays blocked until exclusive local privacy has at least one valid model entry in `model_config.json`.

#### 2. Docker Deployment (For technical users - isolated environment)

Docker is the more explicit and more configurable deployment path. It is the better choice if you want to deploy the **engine backend**, the **web app frontend**, optional **local model servers**, and **FalkorDB** as separate services with clear boundaries.

**Requirements:**
- [ ] **Docker** (v20.10+)
- [ ] **Docker Compose**
- [ ] **GPU recommended**
- [ ] **6GB RAM minimum** (engine requires 6GB, but local models need more)
- [ ] **16GB RAM recommended** for full local model support

> **Pro Tip**: Use our [`docker-compose-example.yml`](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine/blob/main/docker-compose-example.yml) as the main reference for a full backend + frontend setup, and [`docker-compose-llms.yml`](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine/blob/main/docker-compose-llms.yml) for additional local `llama.cpp` server examples.

---

## Configuration Files

The engine can work with a mix of automatically generated runtime state, manually managed deployment files, and optional extension directories.

### `model_config.json`

This file tells the engine which models to use and how to connect to them.

**When is it required?**

- **Native / Guided Setup**: It can be created automatically by the launcher
- **Native / Manual Mode**: You must provide and maintain it yourself
- **Docker / local exclusive privacy**: In practice, you usually provide it manually
- **Cloud-only setups**: Local model entries are not needed if you do not use exclusive local privacy

#### Example (using built-in models)

```json
{
  "ministral-3-3b": {
    "url": "http://localhost:44410/v1",
    "provider": "llama.cpp",
    "privacy_level": "Exclusive",
    "temperature": 0.2,
    "input_modalities": ["text", "image"],
    "output_modalities": ["text"]
  },
  "ministral-3-8b": {
    "url": "http://localhost:44411/v1",
    "provider": "llama.cpp",
    "privacy_level": "Exclusive",
    "temperature": 0.2,
    "input_modalities": ["text", "image"],
    "output_modalities": ["text"]
  }
}
```

#### Example (custom models)

```json
{
  "qwen3-32b-vl": {
    "url": "http://localhost:44410/v1",
    "provider": "llama.cpp",
    "privacy_level": "Exclusive",
    "temperature": 0.2,
    "input_modalities": ["text", "image"],
    "output_modalities": ["text"]
  },
  "smolvlm2-500M": {
    "url": "http://localhost:44409/v1",
    "provider": "llama.cpp",
    "privacy_level": "Exclusive",
    "temperature": 0.8,
    "input_modalities": ["text", "image"],
    "output_modalities": ["text"]
  },
  "bitnet-local-gguf": {
    "url": "http://localhost:44411/v1",
    "provider": "bitnet.cpp",
    "privacy_level": "Exclusive",
    "temperature": 0.4,
    "input_modalities": ["text"],
    "output_modalities": ["text"]
  }
}
```

> ⚠️ If exclusive local privacy is enabled, the engine needs at least one valid local model in `model_config.json`.
> If your system has less than 12GB RAM or no powerfull GPU, consider using cloud models or an external LLM provider.

#### Native Deployment - Automatic Generation

**Automatic configuration available.** The binary launcher can automatically generate this file based on your hardware and preferences.

**Auto-generated format:**

```json
{
  "ministral-3-3b": {
    "url": "http://localhost:44410/v1",
    "provider": "llama.cpp",
    "privacy_level": "Exclusive",
    "temperature": 0.2,
    "input_modalities": ["text", "image"],
    "output_modalities": ["text"]
  }
}
```

**Features:**
- ✅ **Automatic model selection** - Based on your hardware capabilities
- ✅ **Hardware-optimized configuration** - Context size, parallel processing, etc.
- ✅ **Multi-model support** - Can include multiple Ministral models (3B, 8B, 14B)
- ✅ **Manual override** - You can edit the auto-generated file

**Model keys used by auto-generation:**
- `ministral-3-3b`: Fast, low-memory usage (3B parameters)
- `ministral-3-8b`: Balanced performance (8B parameters)
- `ministral-3-14b`: Highest quality (14B parameters)

> **Note**: If you use manual mode during setup, you must provide this file yourself. The launcher will not auto-generate it.

### `mcp_servers.json` (Optional)

**Configures plugins and external integrations (e.g., APIs, databases).**

We follow closely the Claude Desktop Config so you can mostly copy the config for any given MCP Server.

**Examples**

```json
{
  "mcpServers": {
    "osm-mcp-server": {
      "name": "OpenStreetMap Location-Based App MCP Server",
      "description": "A Tool for the AI to search for places and routes.",
      "transport": "stdio",
      "command": [
        "./mcps/osm_mcp_server"
      ],
      "url": null,
      "bearer_token": null,
      "env": null,
      "tags": null,
      "created_at": "2025-04-28"
    },
    "mcp-email-server": {
      "name": "E-Mail MCP Server",
      "description": "A connector for using default SMTP and IMAP E-Mail Servers.",
      "transport": "stdio",
      "command": [
        "./mcps/mcp_email_server_bin",
        "stdio"
      ],
      "url": null,
      "bearer_token": null,
      "env": {"MCP_EMAIL_SERVER_CONFIG_PATH": "./mcps/mcp_email_server/config.toml"},
      "tags": null,
      "created_at": "2025-06-06"
    },
    "mcp-filesystem": {
      "name": "Filesystem MCP Server",
      "description": "A connector for using the local filesystem, restricted on given directories.",
      "transport": "stdio",
      "command": [
        "./mcps/mcp-filesystem",
        "stdio",
        "/home/user/filesystem"
      ],
      "url": null,
      "bearer_token": null,
      "env": null,
      "tags": null,
      "created_at": "2025-09-08"
    },
    "ariadne-webresearch-mcp": {
      "name": "Ariadne Webresearch MCP",
      "description": "Use MCP tools for web search and webcrawler retrieval with strict prompt-injection resistance and chunk-aware content handling.",
      "transport": "http",
      "command": [],
      "url": "http://192.168.178.91:8000/mcp",
      "bearer_token": null,
      "env": null,
      "tags": null,
      "created_at": "2026-02-06"
    }
  }
}
```

> ⚠️ The `mcp_servers.json` file is optional. If it is missing, the engine can create a default empty configuration with zero global MCPs.

**Minimal MCP Config**

```json
{
  "mcpServers": {
  }
}
```

### `skills/` (New in v0.2.0)

The new skill system is one of the most important additions in `v0.2.0`.

Global skills can be shipped in a top-level `skills/` directory. If the directory exists, the engine will discover and load valid `SKILL.md`-based skills from it. If the directory does not exist, the engine simply proceeds without global deployment-scoped skills.

This means:
- the directory is **recommended** if you want to ship reusable skills with your deployment
- the directory is **not mandatory** for the engine to boot
- user-specific skills can still exist separately inside user storage

**Minimal structure**

```text
skills/
└── document-reader/
    └── SKILL.md
```

### `flow-scripts/` (Optional)

The `flow-scripts/` directory is used for custom Python workflows.

This is important historically:
- in older setups, users often had to create the folder manually
- in the current implementation, missing flow-script directories are tolerated and created when needed
- custom flows themselves are still optional

So the correct practical interpretation for `v0.2.0` is:
- **custom flows are optional**
- the engine can handle a missing `flow-scripts/` directory
- if you want custom workflows, this is the place to put them

---

## 🗃️ Storage Dependencies: How Data is Managed

The Ariadne Engine uses different storage layers depending on which features you activate.

### 1. User-Specific Embedded Databases

- **Location**: Automatically created in the `databases/` folder (e.g., `./databases/user_123/`)
- **What's stored**:
  - User-specific configurations
  - Temporary or session-based data
  - Embedded databases for lightweight storage
  - Uploaded files
- **How it works**:
  The engine manages these folders automatically. No manual setup is required for normal operation.

### 2. FalkorDB (Required for Knowledge Graphs and Long-Term Memory)

- **Location**: Usually `./databases/falkordb/` in Docker setups
- **What's stored**:
  - Knowledge graphs
  - Long-term memory data
  - Graph-oriented connections across documents and structured entities

**Important background**

- The engine itself can run **without** FalkorDB.
- If FalkorDB is missing, the engine still works, but **knowledge graphs and graph-backed long-term memory are unavailable**.
- In `v0.2.0`, this also applies to the native binary deployment: knowledge graphs still depend on an available FalkorDB service.

**Recommended FalkorDB service**

```yaml
falkordb:
  image: falkordb/falkordb:latest
  restart: unless-stopped
  ports:
    - "${AAA_FALKORDB_PORT:-44400}:6379"
    - "3000:3000" # optional Web UI
  environment:
    # Redis-Args (Persistence + security + visibility)
    REDIS_ARGS: "--protected-mode yes --bind 0.0.0.0 --requirepass ${AAA_FALKORDB_PASSWORD:-default} --appendonly yes --appendfsync everysec --aof-use-rdb-preamble yes --save 3600 1 --save 300 100 --save 60 10000 --dbfilename dump.rdb"
    # FalkorDB arguments
    FALKORDB_ARGS: "THREAD_COUNT 4"
  volumes:
    - ./databases/falkordb:/var/lib/falkordb/data
  healthcheck:
    test: ["CMD-SHELL", "redis-cli -a ${AAA_FALKORDB_PASSWORD:-default} PING | grep -q PONG"]
    interval: 10s
    timeout: 3s
    retries: 10
  stop_grace_period: 20s
  networks:
    - ariadne-network
```

---

## Deploy Like a Pro: Docker Setup

Docker is the better choice when you want a more explicit, service-oriented setup with separate containers for:
- the Ariadne Engine backend
- the Ariadne Webapp frontend
- optional local `llama.cpp` model servers
- FalkorDB

### Directory Structure

Create the following structure in your project root:

```
project_root/
├── databases/
│   └── falkordb/         # FalkorDB data (auto-created)
├── models/
│   ├── docling/          # Docling models (auto-downloaded)
│   ├── faster-whisper/   # faster-whisper models (auto-downloaded)
│   └── others/           # LLM, VLM, and embedding models
├── flow-scripts/         # Optional custom Python flow scripts
├── skills/               # Optional global skills for v0.2.0+
├── mcp_servers.json      # Optional global MCP config
└── model_config.json     # Required for local exclusive model setups
```

> ⚠️ The absence of `model_config.json` will cause startup problems when you enable exclusive local privacy without any configured local model. If `mcp_servers.json` is not provided, the engine can create a default empty configuration with zero global MCPs.

> ⚠️ **First run takes time**: The engine auto-downloads `docling` and `faster-whisper` models (~10 min).

> **Pro Tip**: Use our [`docker-compose-example.yml`](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine/blob/main/docker-compose-example.yml) as the full-stack reference and [`docker-compose-llms.yml`](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine/blob/main/docker-compose-llms.yml) for alternative local model server setups.

### AI Model Dependencies

Ensure the following models are available in their respective directories:

1. **LLM Models**:

   You can use additional LLM models by downloading them from Hugging Face and hosting them via `llama.cpp` or `ollama`.
   Add configurations for these models in the `model_config.json` file under their desired display names.

   Models that we currently recommend:
   - https://huggingface.co/unsloth/Ministral-3-14B-Instruct-2512-GGUF
   - https://huggingface.co/unsloth/Ministral-3-8B-Instruct-2512-GGUF
   - https://huggingface.co/unsloth/Ministral-3-3B-Instruct-2512-GGUF

   Place the downloaded file in `./models/others/` if you follow the Docker Compose example of this repository.

2. **VLM Model**:

   You can use additional VLM models by downloading them from Hugging Face and hosting via `llama.cpp`.
   Currently recommended model: SmolVLM2-500M-Video-Instruct (fast on CPU, suitable for document processing).
   - https://huggingface.co/ggml-org/SmolVLM2-500M-Video-Instruct-GGUF

   Place the downloaded file in `./models/others/` if you follow the Docker Compose example of this repository.

3. **Embeddings Model**:

   You can use additional embedding models by downloading them from Hugging Face and hosting via `llama.cpp`. The `AAA_EMBEDDINGS_BASE_URL` in the Docker Compose file allows you to point to any `llama.cpp`-compatible server or OpenAI-compliant embedding APIs.
   Currently recommended model: BGE-M3, which is fast on CPU and suitable for text processing pipelines.
   - https://huggingface.co/bbvch-ai/bge-m3-GGUF

   Place the downloaded file in `./models/others/` if you follow the repository's Docker Compose example.

4. **faster-whisper & Docling Models**:
   - **Docker**: These are automatically downloaded by the engine if missing from their respective directories (`./models/faster-whisper` and `./models/docling`)
   - **Binary**: Automatically downloaded during the first run setup process

> We provide a few Docker Compose setups with `llama.cpp` for some good local open source LLMs in `docker-compose-llms.yml`.

### Docker Compose Example (Backend + Frontend)

Deploy using `docker-compose.yml`. Below is a representative setup based on `docker-compose-example.yml`. It shows the engine backend and the webapp frontend together, which is the most common self-hosted Docker deployment pattern.

```yaml
networks:
  ariadne-network:
    driver: bridge

services:
  ariadne-engine:
    image: ariadneindustries/ariadne-engine:0.2.0-on-prem
    restart: unless-stopped
    user: "${HOST_UID}:${HOST_GID}" # you can try to configure this to have better access to files on your host the engine creates within the docker container. OPTIONAL.
    ports:
      - "44444:44444"
    extra_hosts:
      - "host.docker.internal:host-gateway"
    volumes:
      - ./databases:/app/aaa-bundle/databases
      - ./models/docling:/app/aaa-bundle/models/docling:ro
      - ./models/faster-whisper:/app/aaa-bundle/models/faster-whisper
      - ./flow-scripts:/app/aaa-bundle/flow-scripts:ro
      - ./skills:/app/aaa-bundle/skills:ro # optional global skills for v0.2.0+
      - ./mcp_servers.json:/app/aaa-bundle/mcp_servers.json:ro # Optional: if not provided, the engine can create a default empty configuration with zero global MCPs
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
      - AAA_FASTER_WHISPER_DEVICE=cpu # Defaults to cpu. Set to cuda to run faster-whisper on a GPU.
      - AAA_FALKORDB_HOST=falkordb # Defaults to "host.docker.internal". Must match the container name in `docker-compose.yml` if using a separate FalkorDB instance.
      - AAA_FALKORDB_PORT=6379 # Overrides default port of 44400. Only required if connecting to an external FalkorDB instance on non-default ports.
      - AAA_FALKORDB_PASSWORD=${AAA_FALKORDB_PASSWORD:-default} # Defaults to "default" if not set
      # - AAA_LOCAL_AAA_PORT=44444 # defaults to 44444
      - AAA_IDENTITY_SOURCE=integrated-idp # default is ariadne-anyverse, for local setup it should be integrated-idp.
      - AAA_WORKER_PROCESSES=2 # Overrides default of 4 workers. Each worker uses around **4GB of RAM**.
    networks:
      - ariadne-network

  ariadne-webapp:
    image: ariadneindustries/ariadne-webapp:0.2.0-web-bff
    restart: unless-stopped
    ports:
      - "43380:80"   # HTTP port for accessing the webapp
      - "44380:443"  # HTTPS port (optional, not properly supported, please use a Proxy server if you need TLS)
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      - AAA_ENDPOINT_URL=http://host.docker.internal:44444/endpoint # can be set to the Ariadne Engine container, server ip address, or host. An Ariadne Engine must run there.
      - IDP_BASE_URL=http://host.docker.internal:44444/integrated_idp # should be the Identity Provider used by the Ariadne Engine instance. Can be the integrated IDP of the Ariadne Engine if enabled
    networks:
      - ariadne-network
```

See `docker-compose-example.yml` in this repository for a full setup including local model servers and FalkorDB.

### Environment Variables

#### Docker Deployment

Create a `.env` file with the following variables (adjust as needed):

```bash
HOST_UID=1000
HOST_GID=1000
AAA_FALKORDB_PASSWORD=default
```

#### Native Deployment - Automatic Environment Setup

The binary launcher automatically sets the following environment variables. You can also set them manually in a `.env` file if needed:

**Core Configuration Variables:**
```bash
MODEL_CONFIG_PATH=./model_config.json
AAA_IS_PRIVACY_LEVEL_EXCLUSIVE_ENABLED=true
AAA_IS_PRIVACY_LEVEL_PREMIUM_ENABLED=true
AAA_EMBEDDINGS_BASE_URL=http://localhost:44441/v1
AAA_FASTER_WHISPER_MODEL=small
```

**Advanced Configuration Variables:**
```bash
# Privacy levels
AAA_IS_PRIVACY_LEVEL_STANDARD_ENABLED=false

# FalkorDB configuration
AAA_FALKORDB_HOST=localhost
AAA_FALKORDB_PORT=6379
AAA_FALKORDB_PASSWORD=default

# Engine configuration
AAA_LOCAL_AAA_PORT=44444
AAA_IDENTITY_SOURCE=integrated-idp
AAA_WORKER_PROCESSES=2

# faster-whisper configuration
AAA_FASTER_WHISPER_DEVICE=cpu
```

**Variable Descriptions:**

- `MODEL_CONFIG_PATH`: Path to your `model_config.json` file (auto-set by binary launcher)
- `AAA_IS_PRIVACY_LEVEL_EXCLUSIVE_ENABLED`: Enable/disable local models (true/false)
- `AAA_IS_PRIVACY_LEVEL_PREMIUM_ENABLED`: Enable/disable cloud premium models (true/false)
- `AAA_IS_PRIVACY_LEVEL_STANDARD_ENABLED`: Enable/disable cloud standard models (true/false)
- `AAA_EMBEDDINGS_BASE_URL`: URL for the embedding service (auto-configured by launcher)
- `AAA_FASTER_WHISPER_MODEL`: Speech recognition model (`base`, `small`, `medium`, `large-v3-turbo`)
- `AAA_FASTER_WHISPER_DEVICE`: Device for speech recognition (`cpu` or `cuda`)
- `AAA_FALKORDB_HOST`: FalkorDB host address
- `AAA_FALKORDB_PORT`: FalkorDB port number
- `AAA_FALKORDB_PASSWORD`: FalkorDB password
- `AAA_LOCAL_AAA_PORT`: Engine server port
- `AAA_IDENTITY_SOURCE`: Identity provider (`integrated-idp` or `ariadne-anyverse`)
- `AAA_WORKER_PROCESSES`: Number of worker processes (each uses ~4GB RAM)

> **Note**: The binary launcher automatically sets these variables based on your setup choices. You only need to manually set them if you're using custom configurations or troubleshooting.

---

## 🔧 Flow Scripting: Extend the Engine with Python

The `flow-scripts/` directory enables you to **extend the engine's functionality** with Python scripts.
Here is where you define **custom workflows** using Python. These scripts allow you to:
- **Integrate custom logic** into the engine's agentic workflows.
- **Automate complex tasks** (e.g., data processing, API calls, or multi-step reasoning).
- **Extend functionality** beyond pre-built agents.

### 📌 Current Status

- **No official documentation yet**: Flow scripting is available now, but we're working on comprehensive guides.
- **Unstable API**: Flow scripting currently uses the low level engine APIs which may change and does not yet provide stable building blocks.
- **Need help?** Contact [Ariadne Industries](https://www.ariadneanyverse.de/contact) for customization support or to discuss your use case.

---

## 📂 Directory Structure for Flow Scripts

To use custom workflows (including the knowledge graph flows), your `flow-scripts/` directory can follow this structure:

**Example:**

```
flow-scripts/
├── __init__.py           # Imports all flow modules if you use package-style loading
├── flow_scripts
│   └── my_custom_workflow.py
└── utils/
```

> **Note**: In current versions, the engine tolerates a missing `flow-scripts/` directory and creates it when needed. You only need to populate it if you actually want to ship custom flows.

---

### ✨ Pre-Built Knowledge Graph Flows

1. **Advanced Knowledge Graph Construction Flow** *(Currently Complex, but Functional)*
   - **Purpose**: **Idempotently builds a knowledge graph** from your uploaded files using the engine's internal features and stores it persistently in the graph database.
   - **How it works**:
     - Processes all files in the engine's file index.
     - Uses the engine's AI capabilities to extract and connect information into a structured graph.
     - Stores the graph internally for future queries.
   - **Note**: This script is currently complex but fully functional. We're working on simplifying it and improving documentation.
2. **Simple Knowledge Graph Index Query Flow**
   - **Purpose**: Query the current state of the knowledge graph indexation of your file index.
   - This is also a simple showcase on how to build your own flow with Python.

### 📌 How to Use These Flows in the Web App

To utilize these flows in the Ariadne Webapp:
1. Place the provided `flow-scripts` from the `flow-scripts` folder including the `__init__.py` and the subfolder into your **engine's `flow-scripts/` directory**.
2. Restart the engine to load the new scripts.
3. Use the Webapp's **AI File Explorer** and in there the "AI File Index" to start the graph creation with your uploaded files.

---

## 🖥️ Ariadne Webapp: Your Control Center

The **Ariadne Webapp** is your dashboard for:
- Visualizing agentic workflows.
- Managing knowledge graphs.
- Interacting with Meta Agents via chat.
- Creating and managing custom skills.

### 🔗 How It Connects to the Engine
1. The UI must run **alongside** an `ariadne-engine`. The engine and Webapp can be configured to run on different servers. Multiple Webapps can use one engine.
2. Set `AAA_ENDPOINT_URL` in the Webapp's `.env` to point to your engine (`http://localhost:44444/endpoint`).
3. For local setups, ensure `IDP_BASE_URL` points to the engine's integrated IDP:
   ```bash
   IDP_BASE_URL=http://host.docker.internal:44444/integrated_idp
   ```

> **Native deployment note**: For the native binary / companion app flow, backend selection is handled inside the app itself. This README focuses on engine deployment and the Docker-based Webapp configuration.

### Usage Example (Webapp)

Deploy the UI webapp using `docker-compose.yml`. Add the following service to your existing configuration:

```yaml
services:
  ariadne-webapp:
    image: ariadneindustries/ariadne-webapp:0.2.0-web-bff
    restart: unless-stopped
    ports:
      - "43380:80"   # HTTP port for accessing the webapp
      - "44380:443"  # HTTPS port (optional, not properly supported, please use a Proxy server if you need TLS)
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      - AAA_ENDPOINT_URL=http://host.docker.internal:44444/endpoint # Required. Must point to the active Ariadne Engine backend.
      - IDP_BASE_URL=http://host.docker.internal:44444/integrated_idp # For local setups, should point to the identity provider used by the engine instance.
    networks:
      - ariadne-network
```

> The `AAA_ENDPOINT_URL` must be set to the same port as your Ariadne Engine backend (`44444`). Ensure that the `IDP_BASE_URL` points to the integrated IDP of the engine when you use the local integrated setup.

---

### Verification & Troubleshooting (UI + Backend)

#### 1. **Verify UI Connectivity**

- Open a browser and navigate to `http://localhost:43380` (or `https://localhost:44380`).
- Ensure the UI loads correctly and displays data from the engine.

#### 2. **Check for Common Issues**

- If the Webapp fails to load, verify that the Ariadne Engine backend is running (`docker ps`) and that environment variables are set correctly.
- For authentication errors, ensure `IDP_BASE_URL` points to the integrated IDP of the engine (`44444/integrated_idp`).

#### 3. **Verify Backend Connectivity**

Check service logs for any errors:

```bash
docker logs <container_name>
```

#### 4. Verify connectivity to the required ports

- Ariadne Engine: **44444**
- LLM GPU Server: **44410** -> Port depends on your setup and how you configured the local models in `model_config.json`. **It is up to you what and how many LLMs you use**
- LLM CPU Server: **44408** -> Port depends on your setup and how you configured the local models in `model_config.json`. **It is up to you what and how many LLMs you use**
- VLM Server: **44409**
- Embeddings Server: **44441**

#### 5. **Common startup and configuration issues**

- `model_config.json` is missing or invalid while exclusive local privacy is enabled
- FalkorDB is not reachable while graph features are being used
- `AAA_EMBEDDINGS_BASE_URL` is not set for retrieval and embedding functionality
- local model server URLs in `model_config.json` do not match the actual ports of your `llama.cpp` or `Ollama` setup
- required ports are already in use

#### 6. **Missing Configuration Files/Directories**

If any of the following are missing, the engine may fail to start or certain features may remain unavailable:
- `model_config.json` (required for local exclusive model setups)
- `mcp_servers.json` (optional, engine can create an empty default if missing)
- `./flow-scripts/` directory (optional unless you want custom flow scripts)
- `./skills/` directory (optional unless you want to ship global skills in `v0.2.0+`)

---

### Starting and Verifying the Services

#### Native Deployment

- Start the launcher by **double-clicking the executable** or running it from a terminal.
- Use **Start Server** or **Start Server and App** after setup.
- Or start the backend directly with `./ariadne_engine --server` or `.\ariadne_engine.exe --server`.

#### Docker Deployment

Start the stack with:

```bash
docker compose up -d
```

Verify that the required services are running:

```bash
docker ps
```

#### Health Check

Verify that the engine is responding:

```bash
curl http://localhost:44444/health
```

If the Webapp is part of your stack, it is typically reachable at:
- `http://localhost:43380`
- `https://localhost:44380`

#### Common Commands

**Stop the engine:**
- Press `Ctrl+C` in the terminal where the engine is running
- Or close the launcher window
- In Docker, use `docker compose down`

**Restart with new settings:**
- In native mode, run the launcher again and select **Run Setup Again**
- In Docker mode, update your compose config or env files and restart the stack

**Update models:**
- The launcher automatically checks for model updates on startup
- You can force a re-download by deleting the model files and running setup again

**Check logs:**
- In native mode, logs are written to the console where you started the server
- In Docker mode, inspect container logs with `docker logs <container_name>`

---

## Additional Resources

- [Ariadne Engine GitHub Repository](https://github.com/Ariadne-Industries-GmbH/Ariadne-Engine) (Official documentation)
- [Ariadne Anyverse](https://www.ariadneanyverse.de/) (Digital ecosystem for data sovereignty)

---

## 🚀 Join the Vision

Interested in how the Ariadne Engine fits into the bigger picture?
- Check out the **[Ariadne Anyverse vision](https://www.ariadneanyverse.de/)** (digital ecosystem for data sovereignty).
- Follow our **[B2B & B2C activities](https://www.linkedin.com/company/ariadneanyverse/)** for updates.

---

## License

**Custom license/usage permission**
- users may pull and run the Docker image and use the engine for private use, including commercial use, but cannot redistribute.
- all rights belong to Ariadne Industries GmbH and Fabian Fuchs
- See https://www.ariadneanyverse.de/Annex_On-premise_License_Terms.pdf

By downloading and using our Software, you agree with the License Terms given.

### License TLDR

**1 Scope & Definitions**
1. These license terms apply to every installation of the software within the customer's IT
environment ("on-premise"). A user account with the provider is not required for this.
2. The customer is the company designated in the offer/agreement to whom the software is licensed.
3. An instance refers to a functional copy of the software on a server or container system of the customer.

**2 Grant of License**
1. The provider grants the customer a simple, non-transferable, non-sublicensable right to
use the software for the customer's internal business purposes.
2. Unless otherwise specified in the offer, the license is perpetual and is not limited by
quantity or performance.
3. Source code or object code is not transferred.

**3 Usage Restrictions**
The customer shall in particular not:
- make the software available to or lease it to third parties;
- reverse engineer, decompile, or disassemble the software (cf. § 69e German Copyright
Act [UrhG]), unless legally required and the provider refuses the necessary cooperation;
- remove or alter protection notices, logos, or copyright statements;
- publish the results of benchmarks without prior written consent.

The last point excludes issues and bugs, which can be reported publicly in this GitHub Repository, so we
can fix them as soon as possible.
