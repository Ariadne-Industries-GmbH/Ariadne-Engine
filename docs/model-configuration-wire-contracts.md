# Translating `proxy_family` Profiles to Explicit Wire Contracts

**Scope.** Since v1.0.0, new model endpoints are configured with the explicit
`reasoning`, `message_protocol`, and `request_parameter_policy` blocks instead of a
`proxy_family` setting. Existing `proxy_family` configurations remain valid — the
launcher still writes them for preconfigured models — and this document is the
reference for how each legacy family profile translates into the explicit form.

The `model_config.json` chapter in the README explains the blocks and shows guided
examples; this document explains the *mapping* from the old families.

## 1. The three blocks, three levels of the request

| Block | Level of the request | Controls |
|---|---|---|
| `message_protocol` | the `messages` array | role mapping (`developer_message_mode`), mid-history system messages, merging of consecutive user messages, assistant tool-call content rendering, tool-call ID truncation |
| `reasoning` | `chat_template_kwargs` + effort forwarding | whether stored reasoning history is replayed or stripped, in which field (`reasoning` / `reasoning_content`), whether the configured effort is forwarded, and the thinking flags `enable_thinking` / `preserve_thinking` / `clear_thinking` |
| `request_parameter_policy` | top level / extra body of the JSON body | parameters to remove, static or conditional extra top-level parameters, and how the effort value is written |

`reasoning_effort` remains a per-model top-level field in `model_config.json`: it is
the configured reasoning level, and the `reasoning` block decides how — or whether —
it reaches the endpoint.

### How the extra-body keys reach the wire

The keys of `extra_body`, `extra_body_when_reasoning_requested`, and
`extra_body_when_tools_present` are **not** sent inside a nested `extra_body`
object. The engine merges them into the request's `extra_body` collection, and the
OpenAI-compatible client used by every proxy flattens that collection verbatim into
the **top level of the HTTP request body**. Concretely:

```jsonc
// model_config.json
"extra_body_when_tools_present": { "parallel_tool_calls": true, "skip_special_tokens": false }

// actual HTTP body sent to vLLM (relevant excerpt)
{
  "model": "...",
  "messages": [...],
  "chat_template_kwargs": { "enable_thinking": true },
  "parallel_tool_calls": true,
  "skip_special_tokens": false
}
```

`skip_special_tokens` and `parallel_tool_calls` are native top-level parameters of
the endpoint — the engine never sends an `extra_body` wrapper itself.

### What the `message_protocol` modes do to the messages array

The `message_protocol` block transforms the internal message array into the form the endpoint expects. The examples below show the internal array before conversion and the array actually sent. The transformations run in this order: merging consecutive user messages, developer role mapping, mid-history system role mapping, then per-message handling (tool-call ID truncation and assistant tool-call content).

`developer_message_mode` — developer messages are internal instruction messages the engine adds (for example, context-specific guidance). The mode decides which role they get on the wire.

Internal array (same input for all three modes):

```json
[
  { "role": "developer", "content": "Answer with a single short paragraph." },
  { "role": "user", "content": "What is the capital of France?" },
  { "role": "assistant", "content": "Paris." },
  { "role": "developer", "content": "From now on, answer in German." },
  { "role": "user", "content": "And the capital of Germany?" }
]
```

- `preserve`: the array is sent unchanged — the endpoint must accept the `developer` role.
- `user`: every developer message becomes a `user` message (named `developer` when it has no name); content and position stay the same:

```json
[
  { "role": "user", "name": "developer", "content": "Answer with a single short paragraph." },
  { "role": "user", "content": "What is the capital of France?" },
  { "role": "assistant", "content": "Paris." },
  { "role": "user", "name": "developer", "content": "From now on, answer in German." },
  { "role": "user", "content": "And the capital of Germany?" }
]
```

- `leading_system_then_user`: a developer message that appears before any system or conversation message (typically the first message of the array) becomes the leading `system` message; developer messages later in the history become `user` messages named `developer`:

```json
[
  { "role": "system", "content": "Answer with a single short paragraph." },
  { "role": "user", "content": "What is the capital of France?" },
  { "role": "assistant", "content": "Paris." },
  { "role": "user", "name": "developer", "content": "From now on, answer in German." },
  { "role": "user", "content": "And the capital of Germany?" }
]
```

`mid_history_system_message_mode` — controls system messages that appear in the middle of the history. A leading system message (at the start of the array) is always kept as `system`.

Internal array:

```json
[
  { "role": "system", "content": "Base system prompt." },
  { "role": "user", "content": "Hello" },
  { "role": "assistant", "content": "Hi!" },
  { "role": "system", "content": "New rule: keep answers under 20 words." },
  { "role": "user", "content": "Tell me a fact." }
]
```

- `preserve`: the array is sent unchanged — the endpoint must accept system messages mid-history.
- `user`: the mid-history system message becomes a `user` message named `system`:

```json
[
  { "role": "system", "content": "Base system prompt." },
  { "role": "user", "content": "Hello" },
  { "role": "assistant", "content": "Hi!" },
  { "role": "user", "name": "system", "content": "New rule: keep answers under 20 words." },
  { "role": "user", "content": "Tell me a fact." }
]
```

`merge_consecutive_user_messages` — `true` merges consecutive user messages into one message: plain-text contents are joined with a blank line, content part arrays are concatenated, and the merged message takes the `name` of the last user message in the run (when present). `false` leaves the array untouched.

```json
[
  { "role": "user", "content": "Ignore my previous style." },
  { "role": "user", "content": "Now: write a haiku." }
]
```

becomes, with `true`:

```json
[
  { "role": "user", "content": "Ignore my previous style.\n\nNow: write a haiku." }
]
```

`assistant_tool_call_content_mode` — applies only to assistant messages that carry at least one tool call.

Internal array:

```json
[
  { "role": "user", "content": "What's the weather in Berlin?" },
  {
    "role": "assistant",
    "content": "Let me check the weather service.",
    "tool_calls": [
      {
        "id": "call_1",
        "type": "function",
        "function": { "name": "get_weather", "arguments": "{\"city\": \"Berlin\"}" }
      }
    ]
  },
  { "role": "tool", "tool_call_id": "call_1", "content": "{\"temp_c\": 21}" }
]
```

- `preserve`: the array is sent unchanged — one assistant message with both `content` and `tool_calls`.
- `split`: the assistant message is split into two consecutive assistant messages — the visible text first, then the tool call with `content` set to `null`:

```json
[
  { "role": "user", "content": "What's the weather in Berlin?" },
  { "role": "assistant", "content": "Let me check the weather service." },
  {
    "role": "assistant",
    "content": null,
    "tool_calls": [
      {
        "id": "call_1",
        "type": "function",
        "function": { "name": "get_weather", "arguments": "{\"city\": \"Berlin\"}" }
      }
    ]
  },
  { "role": "tool", "tool_call_id": "call_1", "content": "{\"temp_c\": 21}" }
]
```

- `extract_reasoning`: the visible text is moved into the message's reasoning fields (`reasoning` and `reasoning_content`), `content` is set to `null`, and the message keeps only the tool call. The `reasoning` block (in particular `replay_history` and `history_field`) decides which reasoning field form the endpoint receives:

```json
[
  { "role": "user", "content": "What's the weather in Berlin?" },
  {
    "role": "assistant",
    "content": null,
    "reasoning": "Let me check the weather service.",
    "reasoning_content": "Let me check the weather service.",
    "tool_calls": [
      {
        "id": "call_1",
        "type": "function",
        "function": { "name": "get_weather", "arguments": "{\"city\": \"Berlin\"}" }
      }
    ]
  },
  { "role": "tool", "tool_call_id": "call_1", "content": "{\"temp_c\": 21}" }
]
```

## 2. How the engine resolves a model

- **Both or neither.** A model either declares both `reasoning` and
  `message_protocol`, or neither. Declaring only one is rejected at validation
  time. `request_parameter_policy` is independent and may be declared alone.
- **Direct wins completely.** With both blocks present, the legacy profile is never
  consulted.
- **The legacy profile is computed per request.** Without explicit blocks, the
  engine resolves `resolve_legacy_model_protocol_profile(provider, proxy_family,
  reasoning_effort)` on *every* request — so some profile values depend on the
  *current* `reasoning_effort`:

| Profile field | Depends on the current `reasoning_effort` | Where |
|---|---|---|
| `enable_thinking` | effort ≠ `none` → `true`, else `false` | gemma4, qwen3_5 / qwen3_6 on vLLM / llama.cpp (omitted on Ollama) |
| `preserve_thinking` | effort ≠ `none` → `true`, else omitted | qwen3_6 on vLLM / llama.cpp only |
| `replay_history` | effort ≠ `none` → `true`, effort `none` → `false` | qwen3_5 / qwen3_6 (all providers) |

This is the **only semantic difference** between a family profile and an explicit
contract: an explicit contract pins static values. Pick the values for the effort
level the model actually runs with. If you later change `reasoning_effort`, the
effort value itself is still forwarded as configured (when `forward_reasoning_effort`
is `true`) — only the pinned flags do not re-derive.

## 3. Family-by-family translation

The JSONs below assume a thinking setup (`reasoning_effort: medium` or `high`).
Fields that are effort-dependent (§2) are shown for that setup; for
`reasoning_effort: none` use the non-thinking variants noted under each family.

### Gemma 4 (`proxy_family: "gemma4"`)

**vLLM**

```json
{
  "provider": "vllm",
  "reasoning_effort": "medium",
  "reasoning": {
    "replay_history": true,
    "history_field": "reasoning",
    "forward_reasoning_effort": true,
    "enable_thinking": true
  },
  "message_protocol": {
    "developer_message_mode": "preserve",
    "mid_history_system_message_mode": "preserve",
    "merge_consecutive_user_messages": false,
    "assistant_tool_call_content_mode": "preserve"
  },
  "request_parameter_policy": {
    "extra_body_when_reasoning_requested": { "skip_special_tokens": false },
    "extra_body_when_tools_present": { "parallel_tool_calls": true, "skip_special_tokens": false }
  }
}
```

**llama.cpp**

```json
{
  "provider": "llama.cpp",
  "reasoning_effort": "medium",
  "reasoning": {
    "replay_history": true,
    "history_field": "reasoning",
    "forward_reasoning_effort": false,
    "enable_thinking": true
  },
  "message_protocol": {
    "developer_message_mode": "preserve",
    "mid_history_system_message_mode": "preserve",
    "merge_consecutive_user_messages": false,
    "assistant_tool_call_content_mode": "preserve"
  },
  "request_parameter_policy": {
    "extra_body": { "parallel_tool_calls": true }
  }
}
```

**Ollama**

```json
{
  "provider": "ollama",
  "reasoning_effort": "medium",
  "reasoning": {
    "replay_history": true,
    "history_field": "reasoning",
    "forward_reasoning_effort": true
  },
  "message_protocol": {
    "developer_message_mode": "preserve",
    "mid_history_system_message_mode": "preserve",
    "merge_consecutive_user_messages": false,
    "assistant_tool_call_content_mode": "preserve"
  }
}
```

No `request_parameter_policy` is needed: the legacy profile's
`send_none_reasoning_effort_when_disabled` behavior is subsumed by
`forward_reasoning_effort: true`, which sends the effort verbatim — including
`none` (see §4).

**Non-thinking variant (any provider, `reasoning_effort: none`):** keep the
`message_protocol` block, and use
`"reasoning": { "replay_history": true, "history_field": "reasoning",
"forward_reasoning_effort": true, "enable_thinking": false }` for vLLM / llama.cpp
(respectively drop `forward_reasoning_effort` for llama.cpp). For vLLM, omit the
`extra_body_when_reasoning_requested` policy.

### Qwen 3.6 (`proxy_family: "qwen3_6"`)

**vLLM**

```json
{
  "provider": "vllm",
  "reasoning_effort": "medium",
  "reasoning": {
    "replay_history": true,
    "history_field": "reasoning",
    "forward_reasoning_effort": false,
    "enable_thinking": true,
    "preserve_thinking": true
  },
  "message_protocol": {
    "developer_message_mode": "leading_system_then_user",
    "mid_history_system_message_mode": "user",
    "merge_consecutive_user_messages": false,
    "assistant_tool_call_content_mode": "preserve"
  },
  "request_parameter_policy": {
    "extra_body": { "parallel_tool_calls": true }
  }
}
```

**llama.cpp**

```json
{
  "provider": "llama.cpp",
  "reasoning_effort": "medium",
  "reasoning": {
    "replay_history": true,
    "history_field": "reasoning",
    "forward_reasoning_effort": false,
    "enable_thinking": true,
    "preserve_thinking": true
  },
  "message_protocol": {
    "developer_message_mode": "leading_system_then_user",
    "mid_history_system_message_mode": "user",
    "merge_consecutive_user_messages": false,
    "assistant_tool_call_content_mode": "preserve"
  }
}
```

(No `request_parameter_policy`: the legacy profile's extra body is vLLM-only.)

**Ollama**

```json
{
  "provider": "ollama",
  "reasoning_effort": "medium",
  "reasoning": {
    "replay_history": true,
    "history_field": "reasoning",
    "forward_reasoning_effort": true
  },
  "message_protocol": {
    "developer_message_mode": "leading_system_then_user",
    "mid_history_system_message_mode": "user",
    "merge_consecutive_user_messages": false,
    "assistant_tool_call_content_mode": "preserve"
  }
}
```

**Non-thinking variant (`reasoning_effort: none`):**
`"reasoning": { "replay_history": false, "history_field": "reasoning",
"forward_reasoning_effort": false }` plus the same `message_protocol` block. On
Ollama keep `forward_reasoning_effort: true` so the endpoint receives an explicit
`none` instead of an omitted parameter.

### Qwen 3.5 (`proxy_family: "qwen3_5"`)

Identical to Qwen 3.6 **except** `preserve_thinking` is never set (omit the key
entirely). Qwen 3.5 and Qwen 3.6 differ in exactly that one flag.

### Other families

| `proxy_family` | `message_protocol` | `reasoning` | `request_parameter_policy` |
|---|---|---|---|
| `default` | `developer_message_mode: user`, `mid_history_system_message_mode: preserve`, `merge_consecutive_user_messages: true`, `assistant_tool_call_content_mode: split`, `tool_call_id_max_length: 40` (llama.cpp / ollama / bitnet.cpp only) | `replay_history: false`, `history_field: reasoning`, `forward_reasoning_effort: false` | none |
| `mistral`, `ministral3` | `developer_message_mode: leading_system_then_user`, `mid_history_system_message_mode: user`, `merge_consecutive_user_messages: true`, `assistant_tool_call_content_mode: preserve` | `replay_history: false`, `history_field: reasoning`, `forward_reasoning_effort: true` | `reasoning_effort_in_extra_body: true` (mistral-ai provider only) |
| `eurouter` | all four modes `preserve` / `false` (merge) | `replay_history: true`, `history_field: reasoning`, `forward_reasoning_effort: true` | none |
| `eurouter_kimi` | same as `eurouter` | `replay_history: true`, `history_field: reasoning`, `forward_reasoning_effort: false` | `remove_parameters: [temperature, top_p, frequency_penalty, presence_penalty]` |

## 4. What cannot be expressed 1:1

1. **`legacy_additional_history_field`** — the legacy profiles for gemma4 / qwen
   replay historic reasoning from *both* `reasoning` and `reasoning_content`. This
   field is reserved for internally resolved profiles; an explicit contract defines
   exactly one `history_field` (in practice `reasoning`). The loss of the
   dual-field fallback is the only behavioral reduction of the translation.
2. **`send_none_reasoning_effort_when_disabled`** — rejected by the validator in
   explicit configurations. Its behavior (send an explicit `reasoning_effort:
   "none"` when reasoning is disabled, so endpoints such as Ollama do not fall back
   to the model's default "thinking on") is subsumed by
   `forward_reasoning_effort: true`, which sends the configured effort verbatim,
   `none` included.
3. **Effort-dependent flags** (`enable_thinking`, `preserve_thinking`,
   `replay_history` where listed in §2) — static in an explicit contract. Pin them
   for the effort level the model runs with.

## 5. Source references (lorelai repository)

| Topic | Location |
|---|---|
| `RequestParameterPolicy` schema | `aaa-types/aaa_types/model_routing.py:101` |
| `ModelConfig` + validation (both-or-neither, internal-only flags) | `aaa-types/aaa_types/consts.py:171` |
| Legacy profile resolution (all families) | `aaa-types/aaa_types/model_routing.py:277` (`resolve_legacy_model_protocol_profile`) |
| Route resolution (direct vs. legacy) | `aaa-types/aaa_types/model_routing.py:560` (`resolve_model_route`) |
| Wire-contract fallback | `middleware/middleware/proxies/message_conversion.py:67` (`_resolve_wire_contracts`) |
| Extra-body merge (flattening source) | `middleware/middleware/proxies/message_conversion.py:320` (`_merge_extra_body`) |
| Message array transformation (merging, role mapping, tool-call content modes) | `middleware/middleware/proxies/message_conversion.py:341` (`prepare_messages_for_provider`) |
| Reasoning parameters (effort forwarding, `chat_template_kwargs`) | `middleware/middleware/proxies/message_conversion.py:537` (`_apply_reasoning_parameters`) |
| Parameter policy application | `middleware/middleware/proxies/message_conversion.py:595` (`_apply_request_parameter_policy`) |
| Policy injection into provider payloads | `middleware/middleware/routers/brain_router.py:136` |
