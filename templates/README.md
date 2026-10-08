# Credential templates

These are safe-to-commit templates for the two files that carry your LLM
provider credentials. Both live **outside this repo**, per-machine, and are
never committed with real keys in them.

| Template | Copy to | Purpose |
|---|---|---|
| `opencode_auth.json.example` | `~/.local/share/opencode/auth.json` | Keys for opencode's *built-in* providers (`opencode`, `nvidia`, etc.) |
| `opencode.jsonc.example` | `~/.config/opencode/opencode.jsonc` | Extra custom providers — additional independent quota buckets (`nvidia2`, `nvidia3`, ...) |

## Setup

```bash
mkdir -p ~/.local/share/opencode ~/.config/opencode
cp templates/opencode_auth.json.example ~/.local/share/opencode/auth.json
cp templates/opencode.jsonc.example ~/.config/opencode/opencode.jsonc
```

Then edit both copies and replace every `REPLACE_WITH_...` placeholder with
a real key.

- `auth.json` needs at least an `opencode` key (or a `nvidia` key if you're
  using NVIDIA's build.nvidia.com models) to get a single run working.
- `opencode.jsonc`'s `nvidia2` entry is optional — skip it for a first run.
  Add more provider slots later, one per extra API key, so a rate limit on
  one key doesn't stall the whole pipeline.

**Don't hand-edit `opencode.jsonc` beyond a first test.** Once the ARGUS
cluster is up, use `uvm_loop/scripts/add_llm_provider.sh` instead — it
writes the same file, but also copies it into every running opencode
worker container (this path is not bind-mounted from the host, so a
host-only edit never reaches a container) and smoke-tests the new
provider before it's trusted in an unattended run:

```bash
uvm_loop/scripts/add_llm_provider.sh <base_url> <api_key> <model_name> [slug]
```

See the root `README.md`'s "LLM provider credentials" section for the full
setup flow this fits into.
