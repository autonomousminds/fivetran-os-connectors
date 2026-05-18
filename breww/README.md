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
`orders`†, `order_lines`, `order_adjustment_lines`, `customers_suppliers`‡, `contacts`, `customer_types`, `customer_delivery_windows`, `credit_notes`‡, `credit_note_lines`, `credit_note_allocations`, `customer_payments`, `payments`, `tax_rates`, `deals`, `crm_activities`‡, `crm_activity_types`

### Inventory / supply
`products`, `stock_items`, `stock_received`, `inventory_receipts`‡, `purchase_orders`‡, `supplier_invoices`‡, `container_types`, `nr_container_brands`, `goods_in_document_pools`, `fulfillments`

### Production
`drinks`, `drink_batches`‡, `drink_batch_actions`‡, `drink_batch_stock_items_used`‡, `ingredient_batches`, `ingredient_batch_actions`‡, `ingredient_batch_stock_items_used`, `fermentation_readings`, `vessels`, `planned_packagings`‡

### Child tables (extracted from parent nested arrays)
| Table | Parent | FK column | Why |
|---|---|---|---|
| `purchase_order_entries` | `purchase_orders` | `purchase_order_id` | PO line items — stock_item, quantity, price per line |
| `product_component_drinks` | `products` | `product_id` | Drinks that compose a product (BOM, e.g. a mixed-pack SKU) |
| `product_component_stock_items` | `products` | `product_id` | Stock items consumed when a product is assembled |
| `order_payments_refunds` | `orders` | `order_id` | Per-refund detail (parent payment, method, amount) — not exposed via `/payments/` |

Legend:
- **†** True incremental on `last_modified_at` (captures edits as well as new rows). Only `orders` exposes this filter.
- **‡** Incremental on `created_at` / `created_on` (captures only new rows; edits after creation are not picked up).
- *(unmarked)* Full re-sync each run — the endpoint exposes no date filter. Most are small reference tables.

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

- The Breww API publishes no rate-limit headers. The client throttles itself to a max of ~20 req/s and retries 429s using the `Retry-After` header (capped at 300 s — beyond that the sync aborts and resumes from checkpoint next run).
- Checkpoints are emitted every 1000 records per table plus at the end of each table, so a long initial sync (e.g. `orders` at 35k+ rows) is resumable.
- Accountancy-sync endpoints (`/accountancy-sync-*/{auth_id}/…`) are excluded — they are write-back workflows that require an `auth_id` and are not ingest data.

## Source

Plan and OpenAPI analysis: 40 user-facing list endpoints out of 109 documented paths. The connector deliberately omits the 6 accountancy-sync resource groups and all POST/PATCH/DELETE operations; it is read-only.
