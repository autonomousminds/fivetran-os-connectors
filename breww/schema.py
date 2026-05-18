"""Schema definitions for all 40 Breww tables. Every resource is keyed on `id`."""


def get_schema() -> list:
    return [
        # Reference (full-sync, small/lookup)
        {"table": "business_details", "primary_key": ["id"]},
        {"table": "sites", "primary_key": ["id"]},
        {"table": "locations", "primary_key": ["id"]},
        {"table": "users", "primary_key": ["id"]},

        # Commercial / CRM
        {"table": "orders", "primary_key": ["id"]},
        {"table": "order_lines", "primary_key": ["id"]},
        {"table": "order_adjustment_lines", "primary_key": ["id"]},
        {"table": "customers_suppliers", "primary_key": ["id"]},
        {"table": "contacts", "primary_key": ["id"]},
        {"table": "customer_types", "primary_key": ["id"]},
        {"table": "customer_delivery_windows", "primary_key": ["id"]},
        {"table": "credit_notes", "primary_key": ["id"]},
        {"table": "credit_note_lines", "primary_key": ["id"]},
        {"table": "credit_note_allocations", "primary_key": ["id"]},
        {"table": "customer_payments", "primary_key": ["id"]},
        {"table": "payments", "primary_key": ["id"]},
        {"table": "tax_rates", "primary_key": ["id"]},
        {"table": "deals", "primary_key": ["id"]},
        {"table": "crm_activities", "primary_key": ["id"]},
        {"table": "crm_activity_types", "primary_key": ["id"]},

        # Inventory / supply
        {"table": "products", "primary_key": ["id"]},
        {"table": "stock_items", "primary_key": ["id"]},
        {"table": "stock_received", "primary_key": ["id"]},
        {"table": "inventory_receipts", "primary_key": ["id"]},
        {"table": "purchase_orders", "primary_key": ["id"]},
        {"table": "supplier_invoices", "primary_key": ["id"]},
        {"table": "container_types", "primary_key": ["id"]},
        {"table": "nr_container_brands", "primary_key": ["id"]},
        {"table": "goods_in_document_pools", "primary_key": ["id"]},
        {"table": "fulfillments", "primary_key": ["id"]},

        # Production
        {"table": "drinks", "primary_key": ["id"]},
        {"table": "drink_batches", "primary_key": ["id"]},
        {"table": "drink_batch_actions", "primary_key": ["id"]},
        {"table": "drink_batch_stock_items_used", "primary_key": ["id"]},
        {"table": "ingredient_batches", "primary_key": ["id"]},
        {"table": "ingredient_batch_actions", "primary_key": ["id"]},
        {"table": "ingredient_batch_stock_items_used", "primary_key": ["id"]},
        {"table": "fermentation_readings", "primary_key": ["id"]},
        {"table": "vessels", "primary_key": ["id"]},
        {"table": "planned_packagings", "primary_key": ["id"]},

        # Child tables — extracted from nested arrays that have no top-level
        # API endpoint (see CHILD_EXTRACTIONS in helpers.py).
        {"table": "purchase_order_entries", "primary_key": ["id"]},
        # A product can list the same drink in multiple container formats, so
        # the PK includes container_type_id to keep variants distinct.
        {"table": "product_component_drinks", "primary_key": ["product_id", "drink_id", "container_type_id"]},
        {"table": "product_component_stock_items", "primary_key": ["product_id", "stock_item_id"]},
        {"table": "order_payments_refunds", "primary_key": ["id"]},
    ]
