from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import os

# import planner (fail-safe health)
try:
    import planner as core
    _planner_import_error = None
except Exception as e:
    core = None
    _planner_import_error = str(e)

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True, "service": "backfill-planner", "port": os.environ.get("PORT")}

@app.get("/healthz")
def healthz():
    if _planner_import_error:
        return {"ok": False, "error": _planner_import_error}
    return {"ok": True}

class PlanRequest(BaseModel):
    project: str = Field(..., description="GCP project id")
    # Keep the original field name for backward compatibility
    root_bronze_table: str = Field(..., description="Root producing job name in Marquez (e.g. gx_pipeline__orders.gx_validate.basic)")
    date: str = Field(..., description="YYYY-MM-DD")
    created_by: str = Field("unknown", description="Requester email or id")

@app.post("/plan")
def plan(req: PlanRequest):
    if _planner_import_error:
        raise HTTPException(status_code=500, detail=f"Planner import failed: {_planner_import_error}")
    try:
        return core.build_plan(
            project=req.project,
            root_bronze_table=req.root_bronze_table,
            date_str=req.date,
            created_by=req.created_by,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))
