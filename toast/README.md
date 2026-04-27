# Fivetran Custom Connector: Toast POS

Custom Fivetran connector that syncs **the full Toast Standard API surface** — filling the gap left by the [Fivetran community Toast connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors/toast), which only ships ~35 tables and skips large parts of the menu, modifier, configuration, and inventory schema.

## What's Included

- **Full parity** with the Toast Standard API across orders, labor, configuration, cash management, and menus
- **55 total tables** — vs. ~35 in the upstream Fivetran community connector
- **Menu v2** (rich nested menus, modifier groups, modifier options, premodifier groups, item↔modifier-group references) — not synced by the community connector
- **Extended configuration coverage**: tax rates, void reasons, service charges, break types, no-sale reasons, printers, premodifiers, cash drawers
- **Inventory** endpoint
- Single-row-per-restaurant endpoints flattened (`restaurant_detail`, `tip_withholding`)
- Incremental sync via 30-day time windows with state checkpointing
- Token caching with **Fernet encryption** for credentials in state
- Graceful 401/403/429/400/409 handling with retries and back-off
- Soft deletes propagated via `op.delete()`

---

## Quick Start

### Step 1: Clone the repo

```bash
git clone https://github.com/autonomousminds/fivetran-os-connectors.git
cd fivetran-os-connectors/toast
```

### Step 2: Set up Python environment

Using conda (recommended):
```bash
conda create -n fivetran-toast python=3.12 -y
conda activate fivetran-toast
pip install fivetran-connector-sdk requests cryptography
```

Or using pip/venv:
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install fivetran-connector-sdk requests -r requirements.txt
```

### Step 3: Get Toast API credentials

1. Request access to the [Toast Standard API](https://doc.toasttab.com/) for your partner integration
2. Provision a **Machine Client** (`TOAST_MACHINE_CLIENT`) and obtain the **Client ID** and **Client Secret**
3. Confirm the API **domain** for your environment (`ws-api.toasttab.com` for production)
4. Generate a **Fernet key** to encrypt the cached access token in state:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```

### Step 4: Configure credentials

Edit `configuration.json`:
```json
{
  "clientId": "<YOUR_CLIENT_ID>",
  "clientSecret": "<YOUR_CLIENT_SECRET>",
  "userAccessType": "TOAST_MACHINE_CLIENT",
  "domain": "ws-api.toasttab.com",
  "initialSyncStart": "2024-01-01T00:00:00.000Z",
  "key": "<BASE64_FERNET_KEY>"
}
```

> Do not commit `configuration.json` — it contains live credentials.

### Step 5: Test locally

```bash
python connector.py
```

This runs the connector in debug mode and creates `files/warehouse.db` (a DuckDB database) with all synced data.

To inspect the results:
```bash
pip install duckdb
python -c "
import duckdb
conn = duckdb.connect('files/warehouse.db', read_only=True)
tables = conn.execute(\"SELECT table_name FROM information_schema.tables WHERE table_schema='tester' ORDER BY table_name\").fetchall()
for (t,) in tables:
    count = conn.execute(f'SELECT COUNT(*) FROM tester.\"{t}\"').fetchone()[0]
    if count > 0:
        print(f'{t}: {count} rows')
"
```

To reset state and do a full re-sync:
```bash
rm -rf files/
python connector.py
```

> When iterating locally, set `TOAST_LOCAL_DEBUG=1` to force a synchronous checkpoint after every modifier upsert. This works around a DuckDB-tester limitation where the final batch flush fails on tables with tens of thousands of rows. **Do not enable this in production** — it triggers ~17k extra checkpoint round-trips per sync.

### Step 6: Deploy to Fivetran

1. Get your Fivetran API key from **Settings > API Key** in the [Fivetran Dashboard](https://fivetran.com/dashboard)
2. Base64-encode it: `echo -n "key:secret" | base64`
3. Deploy:
   ```bash
   fivetran deploy \
     --api-key YOUR_BASE64_API_KEY \
     --destination YOUR_DESTINATION_NAME \
     --connection YOUR_CONNECTION_NAME \
     --configuration configuration.json
   ```
4. Go to the Fivetran Dashboard and start the initial sync

> To redeploy after code changes, run the same `fivetran deploy` command with the same connection name.

---

## Feature parity vs. the Fivetran community connector

| Area | Fivetran community connector | This connector |
|------|------------------------------|----------------|
| Total tables | ~35 | **55** |
| Orders + nested checks/selections/discounts/taxes | Yes | Yes |
| Labor (jobs, employees, shifts, time entries, breaks) | Yes | Yes |
| Cash management (entries, deposits) | Yes | Yes |
| Basic configuration (menus, dining options, discounts, tables, etc.) | Yes | Yes |
| **Menu v2** (rich menus, modifier groups, modifier options, premodifier groups, item↔modifier-group refs) | **No** | **Yes** (8 tables) |
| **Tax rates, void reasons, service charges, break types, no-sale reasons** | **No** | **Yes** |
| **Printers, premodifiers, premodifier groups, cash drawers** | **No** | **Yes** |
| **Restaurant detail, tip withholding** | **No** | **Yes** |
| **Inventory** | **No** | **Yes** |
| Employee job references & wage overrides as child tables | Partial | Yes |
| Fernet-encrypted token cache in state | Yes | Yes |
| Soft delete propagation via `op.delete()` | Yes | Yes |

---

## Tables Synced (55 total)

### Restaurant & details (3 tables)
| Table | Notes |
|-------|-------|
| `restaurant` | One row per restaurant in the partner account |
| `restaurant_detail` | Single-object endpoint, one row per restaurant |
| `tip_withholding` | Single-object endpoint, one row per restaurant |

### Labor (7 tables)
| Table | Notes |
|-------|-------|
| `job` | Job titles per restaurant |
| `employee` | Employees per restaurant |
| `employee_job_reference` | Child of `employee` — many-to-many job assignments |
| `employee_wage_override` | Child of `employee` — wage overrides |
| `shift` | 30-day windowed |
| `time_entry` | 30-day windowed via `modifiedStartDate` |
| `break` | Child of `time_entry` |

### Cash management (2 tables)
| Table | Notes |
|-------|-------|
| `cash_entry` | 30-day windowed |
| `cash_deposit` | 30-day windowed |

### Configuration (20 tables)
| Table | Endpoint |
|-------|----------|
| `alternate_payment_types` | `/config/v2/alternatePaymentTypes` |
| `dining_option` | `/config/v2/diningOptions` |
| `discounts` | `/config/v2/discounts` |
| `menu` | `/config/v2/menus` |
| `menu_group` | `/config/v2/menuGroups` |
| `menu_item` | `/config/v2/menuItems` |
| `restaurant_service` | `/config/v2/restaurantServices` |
| `revenue_center` | `/config/v2/revenueCenters` |
| `sale_category` | `/config/v2/salesCategories` |
| `service_area` | `/config/v2/serviceAreas` |
| `tables` | `/config/v2/tables` |
| `tax_rate` | `/config/v2/taxRates` |
| `void_reason` | `/config/v2/voidReasons` |
| `service_charge` | `/config/v2/serviceCharges` |
| `break_type` | `/config/v2/breakTypes` |
| `no_sale_reason` | `/config/v2/noSaleReasons` |
| `printer` | `/config/v2/printers` |
| `premodifier_group` | `/config/v2/preModifierGroups` |
| `premodifier` | `/config/v2/preModifiers` |
| `cash_drawer` | `/config/v2/cashDrawers` |

### Orders & nested entities (14 tables)
| Table | Notes |
|-------|-------|
| `orders` | 30-day windowed |
| `orders_check` | Child of `orders` |
| `orders_check_applied_discount` | Discounts applied at the check level |
| `orders_check_applied_discount_combo_item` | Combo items linked to a check discount |
| `orders_check_applied_discount_trigger` | Discount triggers |
| `orders_check_applied_service_charge` | Service charges applied to a check |
| `orders_check_payment` | Junction: check ↔ payment |
| `orders_check_selection` | Line items on a check |
| `orders_check_selection_applied_discount` | Selection-level discounts |
| `orders_check_selection_applied_discount_trigger` | Selection-discount triggers |
| `orders_check_selection_applied_tax` | Taxes applied to a selection |
| `orders_check_selection_modifier` | Modifiers on a selection |
| `orders_pricing_feature` | Pricing feature flags on orders |
| `payment` | Payments |

### Menu v2 (8 tables)
| Table | Notes |
|-------|-------|
| `menu_v2` | One row per restaurant |
| `menu_v2_menu` | Menus |
| `menu_v2_menu_group` | Menu groups |
| `menu_v2_menu_item` | Menu items |
| `menu_v2_modifier_group` | Modifier groups |
| `menu_v2_modifier_option` | Modifier options |
| `menu_v2_premodifier_group` | Premodifier groups |
| `menu_v2_item_modifier_group_ref` | Junction: menu item ↔ modifier group |

### Inventory (1 table)
| Table | Notes |
|-------|-------|
| `inventory` | Inventory items per restaurant |

---

## Sync Strategy

| Data Type | Strategy |
|-----------|----------|
| Restaurant list, configuration endpoints, menus, jobs, employees | First-pass full sync, then `lastModified` filter on subsequent passes |
| `restaurant_detail`, `tip_withholding`, `menu_v2`, `inventory` | First-pass-only single-object pull |
| Orders, checks, selections, payments, applied discounts/taxes/charges | 30-day windowed via `startDate` / `endDate` |
| Shifts, cash entries, cash deposits | 30-day windowed via `startDate` / `endDate` |
| Time entries | 30-day windowed via `modifiedStartDate` / `modifiedEndDate` |
| State checkpointing | After each 30-day window — sync resumes seamlessly on interruption |

---

## Authentication

The connector requests an access token from Toast using `clientId`, `clientSecret`, and `userAccessType` (typically `TOAST_MACHINE_CLIENT`). The token is **Fernet-encrypted** with the `key` from configuration and cached in connector state, so subsequent syncs reuse it until expiry. See `make_headers(configuration, base_url, state, key)`. For Fernet details, see the [cryptography library docs](https://cryptography.io/en/latest/fernet/).

---

## Error Handling

| Status | Behavior |
|--------|----------|
| 401 Unauthorized | Refresh token and retry up to 3 times before logging severe and skipping the endpoint |
| 403 Forbidden | Log warning and skip the endpoint |
| 429 Too Many Requests | Back off using `Retry-After` (or default delay) and retry |
| 400 / 409 | Log and skip — typically indicates a request the API rejected for that restaurant |
| 5xx | Exponential back-off with retries |

---

## Architecture

```
connector.py            Entry point: schema(), update(), and all sync logic
configuration.json      Credentials + Fernet key (gitignored)
requirements.txt        Python deps (cryptography for Fernet)
files/                  Local debug output (warehouse.db, state.json) — gitignored
```

The `connector.py` file contains:
- `schema()` — 55 table definitions with composite primary keys (most tables key by `id` + `restaurant_id`)
- `update()` — top-level sync orchestrator
- `sync_items()` — windowed iteration loop driving all per-restaurant fetches
- `make_headers()` — Fernet-encrypted token cache + refresh
- `process_config()`, `process_labor()`, `process_orders()`, `process_cash()`, `process_menu_v2()`, `process_inventory()`, `process_restaurant_detail()`, `process_tip_withholding()` — endpoint-specific handlers
- `flatten_dict()`, `extract_fields()`, `stringify_lists()` — JSON normalization helpers

---

## API References

### Toast
- [Toast Standard API documentation](https://doc.toasttab.com/)
- [Toast API authentication](https://doc.toasttab.com/doc/devguide/authentication.html)

### Fivetran
- [Connector SDK Documentation](https://fivetran.com/docs/connector-sdk)
- [Connector SDK Setup Guide](https://fivetran.com/docs/connector-sdk/setup-guide)
- [Connector SDK Technical Reference](https://fivetran.com/docs/connector-sdk/technical-reference)
- [Connector SDK Best Practices](https://fivetran.com/docs/connector-sdk/best-practices)
- [Connector SDK GitHub](https://github.com/fivetran/fivetran_connector_sdk)
- [Fivetran community Toast connector](https://github.com/fivetran/fivetran_connector_sdk/tree/main/connectors/toast) — partial-coverage reference (~35 tables)
