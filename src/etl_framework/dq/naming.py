from __future__ import annotations

def prefix(layer: str, dataset: str, template: str) -> str:
    return template.format(layer=layer, dataset=dataset)

def results_table(prefix_: str, category: str) -> str:
    return f"{prefix_}_{category}_results"

def failed_table(prefix_: str, category: str) -> str:
    return f"{prefix_}_{category}_failed_records"

def dashboard_table(prefix_: str) -> str:
    return f"{prefix_}_dashboard"

def quarantine_table(db: str, layer: str, dataset: str, table_template: str) -> str:
    return f"{db}.{table_template.format(layer=layer, dataset=dataset)}"
