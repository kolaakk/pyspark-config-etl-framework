import json
import tempfile
from etl_framework.config import load_config


def test_load_config_parses_dq_publish_guard():
    cfg = {
        "job_name": "test_job",
        "source": {"format": "parquet", "path": "/tmp/x", "options": {}},
        "transform": {"function": "transformations.customer.transform", "params": {}},
        "target": {"write_strategy": "write", "mode": "append", "table": "silver.t"},
        "dq": {
            "enabled": True,
            "layer": "silver",
            "dataset": "customers",
            "publish_guard": {"enabled": True, "block_on_post_fail": True}
        }
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(cfg, f)
        path = f.name

    job_cfg = load_config(path)
    assert job_cfg.dq.enabled is True
    assert job_cfg.dq.layer == "silver"
    assert job_cfg.dq.publish_guard.enabled is True
    assert job_cfg.dq.publish_guard.block_on_post_fail is True
