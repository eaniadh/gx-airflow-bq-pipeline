# scripts/gx_checkpoint_register.py
import os
from pathlib import Path
from dotenv import load_dotenv

# --- Load env ---
dotenv_path = Path(__file__).resolve().parent.parent / "keys" / ".env"
print(f"dotenv_path: {dotenv_path}")
load_dotenv(dotenv_path=dotenv_path)
current_script_dir = Path(__file__).resolve().parent

GX_DIR   = os.getenv("GX_CONTEXT_DIR", str(current_script_dir.parent / "gx"))
DS_NAME  = os.getenv("GX_DATASOURCE", "bq_ds")
TABLE    = os.getenv("TABLE_EXT", "orders_ext")
SUITE_FULL  = os.getenv("GX_SUITE", "orders_basic_suite")
SUITE_CROSS = "orders_cross_suite"
ASSET_FULL  = TABLE
ASSET_CROSS = "orders_cross_checks"

checkpoints_dir = Path(GX_DIR) / "checkpoints"
checkpoints_dir.mkdir(parents=True, exist_ok=True)

def write_checkpoint_yaml(name: str, suite: str, asset_name: str):
    """
    Writes a classic Checkpoint YAML compatible with GE 1.7.x.
    """
    yml = f"""\
name: {name}
config_version: 1.0
class_name: Checkpoint
run_name_template: "%Y%m%d_%H%M%S"
validations:
  - batch_request:
      datasource_name: {DS_NAME}
      data_asset_name: {asset_name}
      options: {{}}
    expectation_suite_name: {suite}
result_format:
  result_format: BASIC
action_list:
  - name: store_validation_result
    action:
      class_name: StoreValidationResultAction
  - name: store_evaluation_params
    action:
      class_name: StoreEvaluationParametersAction
  - name: update_data_docs
    action:
      class_name: UpdateDataDocsAction
"""
    path = checkpoints_dir / f"{name}.yml"
    path.write_text(yml)
    print(f"[GX] Wrote checkpoint: {path}")

# Write both checkpoints
write_checkpoint_yaml("gx_bq_checkpoint", SUITE_FULL,  ASSET_FULL)
write_checkpoint_yaml("gx_bq_cross_checkpoint", SUITE_CROSS, ASSET_CROSS)

print("[GX] Checkpoints ready.")
