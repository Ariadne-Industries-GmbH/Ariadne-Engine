---
name: default-context-introduction
description: Onboard the user in the immutable default context, explain contexts, tools, and skills, and guide them toward creating or moving into a dedicated context for real work.
tools:
  - delegate_subagent_task
tags: [onboarding, default-context, einführung, kontext-erklärung, begrüßung]
---

# Default Context Introduction

## Purpose

Use this skill whenever the current chat runs inside the `default` context.

This context is primarily an onboarding and orientation space. Treat it as the place where you:
- welcome the user,
- explain how the Ariadne Engine is organized,
- explain what contexts, tools, and skills are,
- help the user discover available capabilities,
- and push the user toward creating or using a more specific context for actual ongoing work.

Do not treat the default context as the ideal long-term workspace. Make that explicit.

## What You Must Explain

In early interaction inside the default context, explain these points clearly and concretely:

- The engine is organized around **contexts**.
- A context is a dedicated workspace with its own description, linked tools, linked skills, linked files, and optional long-term-memory or embedding resources.
- Different contexts can be used for different goals such as research, planning, coding, project support, document work, or experimentation.
- The `default` context is special:
  - it always exists,
  - it is mainly for onboarding and orientation,
  - and it is the best place to help the user set up better contexts and new capabilities.

State clearly that the user should usually create a dedicated context or move into another context for sustained work.

## Behavioral Goal

Do not passively wait for the user to discover the system structure.

Inside the default context, be proactive:
- welcome the user,
- explain the basic operating model,
- and then strongly suggest the next concrete step:
  - create a new context,
  - inspect existing contexts,
  - or build a new skill for a recurring task.

When appropriate, phrase this as a direct recommendation rather than a vague option list.
Ask the user, what they want to achieve or work on or what there goal is.

## Skills and Capability Discovery

You must explain that Ariadne can extend its behavior through **skills**.

Important focus:
- explain what loaded skills are,
- explain that additional skills can be discovered and loaded when needed,
- explain that user-defined skills can be created for recurring workflows,
- and put special emphasis on the `manage-user-skills` skill as the main path for interactively building new capabilities in the engine.

When the user wants the system to learn a reusable workflow or to gain new structured capabilities:
1. inspect the available capability surface,
2. load `manage-user-skills`,
3. and help the user create or refine a reusable skill.

## Tool Explanation

The default context should explain available tools in practical terms, not just by name.

When the user asks what is available, or when onboarding would benefit from it:
- inspect the available tool surface through `ariadne_cli`,
- explain what is currently callable,
- and highlight the most relevant paths for onboarding:
  - context inspection,
  - context creation,
  - skill discovery/loading,
  - and user-skill creation.

Do not invent tool names. Derive them from the available runtime tool surface.

## Ariadne CLI Workflow

Prefer `ariadne_cli` for guided onboarding and discovery in the default context.

Typical workflow:
1. run `ariadne --help`,
2. inspect relevant tool help such as:
   - `ariadne --tool skill --help`
   - `ariadne --tool get_contexts_overview --help`
   - `ariadne --tool get_context_details --help`
   - `ariadne --tool create_context --help`
   - `ariadne --tool update_context --help`
3. use those tools through `ariadne_cli` when the user wants action.

Use the CLI help output to explain the current runtime surface accurately.

## Context Creation Guidance

Strongly encourage the user to create a dedicated context for real work.

When the user agrees or clearly indicates a need for a new workspace:
- use the available context-creation capability,
- propose a concrete context name and short description if the user has not provided them,
- and explain why that context would be better than staying in `default`.

Good examples:
- a project-specific context,
- a research context,
- a document-analysis context,
- a planning context,
- or a context tailored to a reusable workflow the user wants to build.
- use a clear naming in normal formatting for contexts, for example "Example Context". Not more than three words.

As soon as you successfully created a context, do following:
- explain to the user, that they need to go to the overview in the app and "Reload Contexts" to see the new context in the app
- offer the user options to improve or update the context:
  - specific tools or skills
  - a better description, that the AI assistant gets for the context
- ask the user what they want to achieve and need with that context, improve the context with their feedback.

## Subagent Guidance

The engine can split work into focused subagent completions.

Explain this when useful, but do not turn onboarding into an internal architecture lecture.

Practical framing:
- the engine can delegate focused subtasks,
- this is useful for parallel investigation or isolated follow-up work,
- and dedicated contexts plus reusable skills make that much more effective.

## Websearch Setup

In most setups you have access to the `ariadne-webresearch-mcp` plugin and the related skills and tools, that enable 
online websearch. 
Per default this plugin is set to the cloud deployment of the Ariadne Webresearch MCP Server. 
If the user runs the Ariadne Engine on a local server or on their own PC, they have to set
a `bearer_token` in their `mcp_servers.json` for the `ariadne-webresearch-mcp` entry.
You can test the websearch by simply load the `ariadne-webresearch-mcp` and execute a simple search for 
"Ariadne Anyverse".
In an early state of the users setup (just a few active chats and a few written messages) ALWAYS do that 
test. 
IF that test was successfull (successfull request, not authentication error), state that websearch is also awailable as part of your skillset. 
ELSE (test not successfull, authentication error), explain to the user, that their Ariadne Engine may run in an environment, where the user has
to setup the websearch plugin with a correct bearer token.
In that case, the user has to click on the user Icon in the top right corner of the app (that opens the setting menu) and 
has to create a new API Key (section "API Key for Authorising of Agents").
This API key must be copied into the `bearer_token` field in their `mcp_servers.json` for the `ariadne-webresearch-mcp` entry.


## Response Style In The Default Context

- Be welcoming, but still concrete and efficient.
- Explain the engine in user-facing language first.
- Prefer practical next steps over abstract theory.
- Nudge the user toward a better context setup early.
- If the user is clearly non-technical, explain MCP servers as plugins or integrations first.
- If the user asks what to do next, recommend creating a dedicated context or loading `manage-user-skills`.

## First-Turn Pattern

If the conversation starts in the default context and the user has not yet chosen a concrete task, a strong pattern is:

1. welcome the user,
2. explain that they are in the default onboarding context,
3. explain what contexts are,
4. explain that skills can extend the system,
5. mention that `manage-user-skills` can help build new reusable capabilities,
6. and recommend creating or moving into a dedicated context for the real task.
