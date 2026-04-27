# Fivetran OS Connectors

Open-source Fivetran connectors built by the team behind **[milo.ai](https://milo.ai)** to bridge the gap between Fivetran and the long tail of tools that either aren't supported by a built-in Fivetran connector — or are supported, but only partially.

Every connector in this repo is built on the [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk), runs on Fivetran's managed infrastructure, and lands data in your existing Fivetran destination alongside your other syncs.

---

## Why this exists

Fivetran has excellent built-in connectors for hundreds of sources — but a few common business tools fall through the cracks:

- **No connector at all** for sources our customers depend on
- **A connector exists, but it's missing data** that's critical for analytics or finance reporting (e.g. salaries, payroll outside one region, half the menu/modifier schema)
- **Connector exists, but it's slow to update** when the upstream API adds new endpoints

These OS connectors fill those gaps. Every one of them is feature-complete against the upstream API and designed to be a drop-in replacement (or supplement) for the built-in Fivetran connector where one exists.

---

## Connectors

| Connector | Source | Tables | Why it exists |
|-----------|--------|--------|---------------|
| [`fivetran-xero/`](./fivetran-xero) | [Xero](https://developer.xero.com/) Accounting + UK Payroll | **81** | Built-in Fivetran Xero supports only Australian payroll — this adds full **UK Payroll** (29 tables) on top of the 52 accounting tables |
| [`fivetran-productive/`](./fivetran-productive) | [Productive.io](https://developer.productive.io/) | **91** | Built-in Fivetran Productive **does not sync salaries** and is missing 20+ tables (contracts, memberships, entitlements, proposals, purchase orders, bills, overheads, etc.) |
| [`toast/`](./toast) | [Toast POS](https://doc.toasttab.com/) Standard API | **55** | The [Fivetran community Toast connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors/toast) ships only ~35 tables — this adds Menu v2, inventory, extended configuration, restaurant detail, and tip withholding |

Each subdirectory has its own README with quick-start instructions, the full table list, and a feature-parity comparison against the corresponding built-in or community Fivetran connector.

---

## Common conventions

All connectors in this repo follow the same shape so you can move between them quickly:

- **Python 3.12** in a conda env (one env per connector — names are `fivetran-xero`, `fivetran-productive`, `fivetran-toast`)
- **Local debug** via `python connector.py` → produces `files/warehouse.db` (DuckDB) for inspection
- **Deployment** via `fivetran deploy --api-key … --destination … --connection … --configuration configuration.json`
- **State checkpointing** at safe boundaries so syncs resume after interruption
- **Direct `op.upsert()` / `op.delete()` / `op.checkpoint()`** — no `yield` (Connector SDK v2 pattern)
- **Rate limiting** with sliding-window buffers and automatic 429/5xx retry
- **`configuration.json` is gitignored** — credentials never go in the repo

The full set of conventions and design decisions is documented in [`CLAUDE.md`](./CLAUDE.md).

---

## Quick start (any connector)

```bash
git clone https://github.com/autonomousminds/fivetran-os-connectors.git
cd fivetran-os-connectors/<connector>     # e.g. toast, fivetran-xero, fivetran-productive
```

Then follow the connector's own README for credentials and environment setup. The flow is the same in every case:

```bash
conda create -n fivetran-<connector> python=3.12 -y
conda activate fivetran-<connector>
pip install fivetran-connector-sdk
# add connector-specific deps (see its README / requirements.txt / environment.yml)

# fill in configuration.json with credentials, then:
python connector.py                  # local debug → files/warehouse.db

fivetran deploy \
  --api-key YOUR_BASE64_API_KEY \
  --destination YOUR_DESTINATION_NAME \
  --connection YOUR_CONNECTION_NAME \
  --configuration configuration.json
```

---

## Contributing

Found a missing endpoint, a schema bug, or want to add a new connector? Open an issue or PR. Each connector is self-contained — adding a new one means a new top-level directory with its own `README.md`, `connector.py`, `configuration.json` template, and conda environment.

When adding a new connector, please:

- Match the existing README structure (Why it exists → Quick Start → Feature parity → Tables → Sync strategy → Architecture → References)
- List **every table** the connector syncs, grouped by domain
- Include an explicit comparison table vs. the built-in/community Fivetran connector if one exists for the same source
- Use direct `op.upsert()` (no yield) and Connector SDK v2 conventions

---

## About

These connectors are maintained by the team behind **[milo.ai](https://milo.ai)** — we use them in production for our own customers and ship them open source so the rest of the Fivetran community doesn't have to rebuild the same thing in private.

If you're hitting a gap in a Fivetran built-in connector and want help building a custom one, [get in touch](https://milo.ai).

---

## References

- [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk)
- [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide)
- [Connector SDK Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Connector SDK Best Practices](https://fivetran.com/docs/connector-sdk/best-practices)
- [Connector SDK GitHub](https://github.com/fivetran/fivetran_connector_sdk)
