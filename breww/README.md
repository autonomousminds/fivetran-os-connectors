# Breww Fivetran Connector

A Fivetran Custom Connector that ingests data from the [Breww](https://breww.com) public REST API.

Built with the [Fivetran Connector SDK](https://fivetran.com/docs/connector-sdk).

## API

- **Base URL**: `https://breww.com/api/`
- **OpenAPI schema**: `https://breww.com/api/schema/` (interactive viewer at `/api/schema/elements/`)
- **Auth**: `Authorization: Bearer BRW.<id>.<hex>` (generate the key in Breww at **Settings → Apps**)
- **Pagination**: DRF PageNumberPagination — `?page=N` with response `{count, next, previous, results}`. The connector follows `next` until null.

## Tables synced (44)

40 are 1:1 with a Breww API list endpoint; 4 more are extracted from nested
arrays that have no top-level endpoint of their own.

### Reference (full re-sync each run)
`business_details`, `sites`, `locations`, `users`

### Commercial / CRM
`orders`†, `order_lines`, `order_adjustment_lines`, `customers_suppliers`, `contacts`, `customer_types`, `customer_delivery_windows`, `credit_notes`, `credit_note_lines`, `credit_note_allocations`, `customer_payments`, `payments`, `tax_rates`, `deals`, `crm_activities`, `crm_activity_types`

### Inventory / supply
`products`, `stock_items`, `stock_received`, `inventory_receipts`, `purchase_orders`, `supplier_invoices`, `container_types`, `nr_container_brands`, `goods_in_document_pools`, `fulfillments`

### Production
`drinks`, `drink_batches`, `drink_batch_actions`, `drink_batch_stock_items_used`, `ingredient_batches`, `ingredient_batch_actions`, `ingredient_batch_stock_items_used`, `fermentation_readings`, `vessels`, `planned_packagings`

### Child tables (extracted from parent nested arrays)
| Table | Parent | FK column | Why |
|---|---|---|---|
| `purchase_order_entries` | `purchase_orders` | `purchase_order_id` | PO line items — stock_item, quantity, price per line |
| `product_component_drinks` | `products` | `product_id` | Drinks that compose a product (BOM, e.g. a mixed-pack SKU) |
| `product_component_stock_items` | `products` | `product_id` | Stock items consumed when a product is assembled |
| `order_payments_refunds` | `orders` | `order_id` | Per-refund detail (parent payment, method, amount) — not exposed via `/payments/` |

Legend:
- **†** True incremental on `last_modified_at`. Only `orders` exposes this
  filter — it captures both new rows and edits.
- *(unmarked)* Full re-sync each run. Breww exposes `created_at` /
  `created_on` filters on 10 of these resources, but a created-only cursor
  would miss edits to existing records (customer renames, PO revisions,
  batch volume corrections, etc.). We deliberately accept the extra sync
  time (~10–15 min for ~37k extra records pulled per run) so every edit
  is captured.

All top-level tables use `id` as the primary key. Child tables use `id`
where the upstream array element has one, otherwise a composite PK
(e.g. `product_component_drinks` is keyed on `(product_id, drink_id, container_type_id)`).

## Schema notes

Records are transformed for BI-friendliness — foreign keys are exploded out
of nested objects so warehouse joins are trivial, addresses are flattened
into columns, and array-shaped nested resources are extracted into child
tables. The full set of rules in helpers.py:

1. **Foreign-key extraction** — any sub-object containing an `id` field
   (e.g. `customer`, `created_by`, `sales_person`) becomes one column per
   primitive sub-field, prefixed with the original key:
   ```
   {"customer": {"id": 123, "name": "Acme Brewery",
                 "reference": "AC-001", "type": "trade"}}
   →   customer_id = 123
       customer_name = "Acme Brewery"
       customer_reference = "AC-001"
       customer_type = "trade"
   ```
   This covers 57 nested-FK fields across the 40 top-level tables, so
   `orders.customer_id = customers_suppliers.id` works without `json_extract`.

2. **Address flattening** — `billing_address`, `delivery_address`, `address`
   sub-objects are flattened with a column prefix:
   ```
   {"billing_address": {"city": "London", "country": "United Kingdom"}}
   →   billing_address_city = "London"
       billing_address_country = "United Kingdom"
   ```

3. **Child-table extraction** — four nested arrays that contain useful
   structured data with no top-level endpoint of their own are extracted into
   the child tables documented above. The parent's array field is dropped
   from the parent row to avoid duplication.

4. **JSON encoding for the rest** — non-FK nested objects without an `id`
   (e.g. `custom_fields`) and any non-extracted array are stored as a JSON
   string. BI users can still query these with `json_extract`/`->>` if needed.

5. **Order-line de-duplication** — `order_lines` and `adjustment_lines`
   arrays on `/orders/` records are dropped from the orders row because those
   resources have their own top-level endpoints (`/order-lines/`,
   `/order-adjustment-lines/`) — they're synced as their own tables.

## Orphan recovery (hidden-record handling)

Breww's list endpoints silently filter out records that the detail endpoints
still serve, in two distinct patterns:

1. **Soft-delete** — `/customers-suppliers/` and `/users/` omit records with
   a `deleted` timestamp. Without mitigation, ~30% of orders reference
   customer_ids that aren't in `customers_suppliers` (churned customers,
   WooCommerce/Eebria gateway accounts, archived locations).
2. **Hidden-filter** — `/stock-items/` and `/container-types/` omit records
   that look perfectly active (`obsolete=False`, `deleted=None`) for reasons
   Breww doesn't expose. Affected records include fundamental brewing
   formats like "440ml Can", "9G Firkin", "Grain - Golden Promise Malt".
   None of `?id__in=`, `?obsolete__in=true,false`, `?include_obsolete=true`,
   `?show_all=true` exposes them — only the detail endpoint does.

After all primary syncs complete the connector runs a **post-sync orphan
recovery pass**:

1. Every FK reference written by `upsert` is registered in-memory by column
   name (`customer_id`, `created_by_id`, `parent_company_id`, `sales_person_id`,
   `updater_id`, `approver_id`, `rejecter_id`, `canceler_id`, `deleter_id`,
   `completed_by_id`, `stock_item_id`, `container_type_id`).
2. The set of referenced ids is diffed against the ids successfully written
   to each parent table during the same run.
3. Each missing id is fetched via its detail endpoint and upserted with the
   standard `flatten_record` transform. Recovery loops up to 3 iterations
   to catch second-order orphans (e.g. a recovered customer's `created_by_id`
   pointing to an ex-employee user).

Recovery targets: `customers_suppliers`, `users`, `stock_items`,
`container_types`. Recovered customer rows carry a non-null `deleted`
timestamp — filter `WHERE deleted IS NULL` for "active only" BI views.

Cost: roughly one detail GET per orphan id. On a fresh sync of a typical
brewery (~750 active customers + ~750 historical + ~150 hidden stock-items
and container-types + ~20 ex-employees), this adds ~1,700 API calls and
~25 minutes of wall-clock time. Subsequent incremental runs only recover
ids newly referenced since the last sync, so the overhead drops to near
zero in steady state.

## Redundant fields dropped from parent records

To avoid storing the same data twice in the warehouse, the connector drops
three nested fields from their parent records:

| Parent | Field dropped | Data available via |
|---|---|---|
| `customers_suppliers` | `contacts` | `/contacts/` table (1:M with `customer_id` FK) |
| `customers_suppliers` | `delivery_windows` | always emitted as empty `{}` by Breww; the actual configuration ID is preserved as `delivery_windows_id` (FK to `customer_delivery_windows` table) |
| `customer_payments` | `order_allocations` | `/payments/` table — each row is one `(customer_payment, order)` allocation |

Empty `{}` dicts on any field are skipped instead of being stored as a
literal `'{}'` column value — saves cruft on optional nested-config fields.

## Known limitations

- **Integer-encoded status columns** — `orders.order_status` and
  `orders.payment_status` are integer codes (e.g. `3` = "Confirmed",
  `4` = "Completed") with no lookup table exposed by the Breww API.
  Document the mapping in your dbt models or BI tool. Observed distribution:

  | order_status | payment_status |
  |---|---|
  | 3 = bulk of orders (≈95%) | 3 = bulk (≈95%) |
  | 4 = secondary (~4%) | 1 ≈ 3% |
  | 5 = small (~0.2%) | 2 ≈ 1% |

  These are uniform across breweries, but Breww does not publish the
  enum. Contact your Breww account manager for the authoritative mapping.

- **`fulfillments.order_id` is null for 100% of `UPLIFT_ULLAGE` rows** —
  this is the correct upstream shape (ullage collection runs are standalone
  events not tied to a sales order). Filter with
  `WHERE type != 'UPLIFT_ULLAGE'` for delivery-style analytics.

- **No `last_modified_at` on most resources** — only `/orders/` exposes a
  modified-since filter. To still capture edits on the remaining 43 tables,
  the connector full-resyncs them on every run rather than relying on a
  `created_at` cursor (which would silently miss edits). The trade-off is
  a slower sync (~75–90 min total instead of ~60).

## Setup

```bash
conda activate fivetran-productive    # reuses the existing env (SDK + requests)
cd breww
cp configuration.json.example configuration.json
# edit configuration.json and paste your Breww API key
```

## Run a debug sync

### Apple Silicon (M1 / M2 / M3) — use validate.py

The Fivetran SDK tester JAR bundles an **x86_64 JVM and DuckDB JNI library**
that crash under Rosetta with `SIGSEGV` in `libduckdb_java*.so` after a few
thousand upserts (see `hs_err_pid*.log`). This is an SDK packaging issue, not a
connector bug — production Fivetran runs use a different runtime and are
unaffected.

For local validation, use the bundled `validate.py`:

```bash
python validate.py
```

It runs `connector.update()` directly with stubbed `op.upsert/checkpoint/delete`
that buffer to memory and write to `files/local_warehouse.db` via Python's
`duckdb` package — no JVM, no JNI, no crashes.

Inspect with:
```bash
duckdb files/local_warehouse.db -c "SHOW TABLES;"
duckdb files/local_warehouse.db -c "SELECT COUNT(*) FROM orders;"
duckdb files/local_warehouse.db -c "SELECT billing_address_city, delivery_address_country FROM orders LIMIT 3;"
```

### Linux / Intel Mac — use the standard `fivetran debug`

```bash
fivetran debug --configuration configuration.json
```

Produces `files/warehouse.db`. Inspect with:
```bash
duckdb files/warehouse.db -c "SELECT COUNT(*) FROM orders;"
```

### Incremental behavior (either runner)

A second run should sync **only** the delta:
- `orders` advances on `last_modified_at` (picks up both new and edited orders)
- 10 other tables advance on `created_at` / `created_on` (new rows only)
- 28 reference / lines tables fully re-sync each run

`validate.py` persists its own resumable state in `files/local_state.json`
(separate from the SDK tester's state, so the two runners don't collide).
Delete `files/` to reset.

## Deploy

```bash
fivetran deploy --configuration configuration.json
```

## File layout

```
breww/
  connector.py                # entry point — schema() + update()
  auth.py                     # Bearer header builder
  api_client.py               # paginated GET helper with 429/5xx retry
  helpers.py                  # flatten_record, upsert, sync_table generic helper
  schema.py                   # all 40 {table, primary_key} dicts
  tables_commercial.py        # 16 commercial / CRM resources
  tables_inventory.py         # 10 inventory / supply resources
  tables_production.py        # 10 production resources
  tables_reference.py         # 4 reference resources
  validate.py                 # Apple Silicon workaround — runs update() with stubbed
                              # SDK ops + native Python duckdb (bypasses crashy JNI)
  requirements.txt
  configuration.json.example
  README.md
  .gitignore
```

## Operational notes

### Rate limits — IMPORTANT

Breww's documented public-API rate limits are:

- **60 requests per minute** (soft — returns 429 with short `Retry-After`)
- **5,000 requests per day** (hard — returns 429 with `Retry-After` up to ~19 hours)

The connector throttles itself to **~57 req/min** (1.05 s between requests)
to stay safely under the per-minute ceiling without wasting quota on
retries.

A full initial sync of a typical brewery (~290k records across 44 tables,
plus ~940 orphan-recovery detail GETs) is approximately **3,500–4,800
requests**. That fits inside the 5,000/day quota, but with little margin.

**The connector MUST be scheduled at most once per 24 hours.** Faster
schedules will exhaust the daily quota mid-sync, abort with a SEVERE
log line ("Breww daily quota exhausted"), and resume from the last
checkpoint on the next scheduled run — but you'll never catch up.

Steady-state cost on subsequent runs is lower because:
- `orders` uses the `last_modified_at` cursor, so only edited rows pull
- Orphan recovery only fetches *newly-referenced* missing ids

A daily schedule is the right default.

### Other notes

- Checkpoints are emitted every 100 records per table plus at the end of
  each table, so a long initial sync (e.g. `orders` at 35k+ rows) is fully
  resumable. If a sync aborts (rate limit, network error), the next run
  picks up where the last checkpoint left off.
- Accountancy-sync endpoints (`/accountancy-sync-*/{auth_id}/…`) are
  excluded — they are write-back workflows that require an `auth_id` and
  are not ingest data.

## Source

Plan and OpenAPI analysis: 40 user-facing list endpoints out of 109 documented paths. The connector deliberately omits the 6 accountancy-sync resource groups and all POST/PATCH/DELETE operations; it is read-only.
