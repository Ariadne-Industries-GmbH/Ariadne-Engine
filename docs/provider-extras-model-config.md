# Provider-Facing Control Fields: `provider_extras`

**Scope.** `provider_extras` is the only place in `model_config.json` where an operator can
hand a concrete provider endpoint control fields that the engine itself has no schema for.
The field is deliberately generic: it belongs to no provider, and the engine never
interprets its content. It carries the declaration as far as the provider proxy that wants
to read it.

The `model_config.json` chapter in the README gives the short contract and an example; this
document is the reference for the merge rules and for reading the engine log.

## 1. Why the field exists

Some properties of a model are properties of a *provider endpoint*, not of the engine. A
concrete example: one upstream model is served by several upstreams behind the same cloud
provider, and one of those upstreams returns broken answers for one particular request
shape. That is a property of the model *at that provider*, so it belongs in the routing
declaration — not as a special case in engine code.

This is the reason `provider_extras` has no schema. Giving these fields a schema would put
provider knowledge into the engine and would make every new provider-specific control a
change in engine code.

## 2. Contract

| Aspect | Behaviour |
|---|---|
| Type | A mapping with arbitrary content. Anything that is not a mapping is a configuration error: `'provider_extras' must be a mapping.` |
| Default | Empty mapping. A model without the declaration sends exactly the same request as before the field existed. |
| Schema | None. The engine checks only that a mapping is present and learns no key names. |
| Evaluation | Only inside the provider proxy that implements the field. A provider without support sees no change at all. |
| Hand-off | Routing provides a **deep copy**. A proxy may modify its own values without touching the stored configuration. |
| Visibility | `ResolvedModelRoute.describe()` shows the field, so route resolution does not silently drop it. |
| Applies to | Both configuration paths: exclusive models from `model_config.json` and declared premium models go through the same validation, so a self-hosted EUrouter endpoint uses the same field as the premium product. |

The deep copy is not a formality. Model configurations live in a process-wide loader. A
shallow copy would share a nested list across every request, and a proxy that appended to
it would silently rewrite the stored configuration for every later request.

## 3. How the values reach a proxy

The router hands a filled declaration to the selected proxy as the underscore marker
`_provider_extras`. Underscore keys are the engine-internal transport form towards a proxy
and are stripped centrally from the payload before the provider is called: the
OpenAI-compatible client rejects an unknown keyword, and a proxy without its own extra
support passes its payload on unchanged. A field one proxy does not read can therefore
never disturb a different provider.

## 4. Example: excluding upstreams from the selection

EUrouter is the first provider that evaluates the mapping. Its `provider` block controls
which upstreams may receive a request. This declaration excludes two upstreams:

```json
{
  "glm-5.3-flash": {
    "url": "https://router.example/v1",
    "provider": "eurouter",
    "privacy_level": "Premium",
    "api_key_env_var": "AAA_EUROUTER_API_KEY",
    "reasoning_effort": "low",
    "provider_extras": {
      "provider": {
        "ignore": ["upstream-a", "upstream-b"]
      }
    }
  }
}
```

From that declaration the following block ends up on the wire:

```json
{
  "ignore": ["upstream-a", "upstream-b"],
  "allow_fallbacks": true,
  "data_collection": "deny",
  "data_residency": "EU",
  "max_retention_days": 0,
  "eu_owned": true
}
```

The other five values are enforced by the engine and are always applied last: fallbacks allowed, no data collection, EU residency, zero retention days, EU-owned ownership.
That gives the merge rules:

- **List keys** are merged as a union, without duplicates, preserving order. A list set by
  the caller is therefore not replaced by the configured one, nor the configured one by a
  single request.
- **Every other key**: the configuration wins over the caller.
- **The data-protection floor always wins.** `data_collection`, `data_residency`,
  `max_retention_days`, and `eu_owned` cannot be relaxed through `provider_extras`, not
  even with a value that would say so.
- **Unknown keys** stay in the `provider` block and are not validated. EUrouter ignores
  what it does not know.

The list is a **declarative statement, not automation**. If a provider adds upstreams that
do not serve a model cleanly, that is a maintenance step in the declaration.

## 5. Example: a field nobody evaluates

Because the field has no schema, a typo stays valid. This value is accepted, read by
nobody, and changes nothing in the request:

```json
"provider_extras": {
  "proivder": { "ignore": ["upstream-a"] }
}
```

You can verify such a case through the route resolution: `describe()` shows the field
verbatim, and a test can compare the expected declaration against the resolved route. The
engine deliberately does not report this error, because it must not know the key names.

## 6. Which upstream instance actually served an answer

For troubleshooting it matters not only what was configured, but what the provider actually
chose. The EUrouter proxy therefore logs the upstream slug together with the engine
`request_id` for every attempt:

```text
EUrouter provider stream started request_id=8a08025b-a849-4ab6-a692-80fe19e3bada upstream_provider=tensorix.
EUrouter provider stream failed request_id=4c930b4c-926e-4f70-a026-531e38961099 upstream_provider=inceptron.
EUrouter provider request completed request_id=1e6444b6-589a-40e2-8c59-8364577f766f upstream_provider=tensorix.
```

A missing identifier in the failure case appears as `upstream_provider=unknown`, not as an
omitted field. The slug comes from the response body rather than a header: headers are not
reachable through the client SDK, while every response part carries the provider identifier
in the body.

The log line replaces no error message and decides nothing. It is an observation, so that a
repeated failure on one provider instance becomes readable from the log alone.

## 7. What the field is *not*

`provider_extras` is not for anything the engine understands. Reasoning level, message
format, role contracts, token limits, modalities, and request queues each have their own
validated field and describe engine behaviour — see the `reasoning`, `message_protocol`,
and `request_parameter_policy` blocks in the README and
[`model-configuration-wire-contracts.md`](model-configuration-wire-contracts.md).

`provider_extras` is only for control fields that a provider proxy interprets according to
its own implementation.
