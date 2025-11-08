import urllib.parse
import requests
from typing import Dict, List, Tuple

def _is_bootstrap(job_name: str) -> bool:
    # Treat any job beginning with "bootstrap." as a system job
    return job_name.startswith("bootstrap.")

def fetch_lineage(marquez_url: str, namespace: str, root: str, depth: int = 20) -> dict:
    """
    Prefer JOB detail → synthesize a small graph (JOB + DATASET nodes).
    We do NOT guess based on dots; many job names contain dots.
    Bootstrap jobs are not valid roots for planning.
    """
    job_name = root.split(":", 1)[-1]

    # If someone passes a bootstrap.* job as the root, stop early
    if _is_bootstrap(job_name):
        raise ValueError(f"Bootstrap/system job is not a valid root: {job_name}")

    job_enc = urllib.parse.quote(job_name, safe="")
    job_url = f"{marquez_url}/api/v1/namespaces/{namespace}/jobs/{job_enc}"
    r = requests.get(job_url, timeout=30)

    if r.status_code == 200:
        j = r.json()
        nodes, edges = [], []
        job_node_id = f"JOB:{j['id']['namespace']}:{j['id']['name']}"
        nodes.append({"id": job_node_id, "type": "JOB", "name": j["id"]["name"]})

        for ds in j.get("inputs", []):
            ds_name = f"{ds['namespace']}:{ds['name']}"
            ds_id = f"DATASET:{ds_name}"
            nodes.append({"id": ds_id, "type": "DATASET", "name": ds_name})
            edges.append({"source": ds_id, "target": job_node_id})

        for ds in j.get("outputs", []):
            ds_name = f"{ds['namespace']}:{ds['name']}"
            ds_id = f"DATASET:{ds_name}"
            nodes.append({"id": ds_id, "type": "DATASET", "name": ds_name})
            edges.append({"source": job_node_id, "target": ds_id})

        return {"graph": {"nodes": nodes, "edges": edges}}

    if r.status_code == 404:
        # Your Marquez doesn’t expose /lineage; fail clearly instead of guessing.
        raise ValueError(f"Job not found in Marquez: namespace={namespace}, job={job_name}")

    r.raise_for_status()


def extract_job_ios(lineage_graph: dict) -> Tuple[List[Dict], List[str]]:
    """
    Convert the synthesized lineage graph into:
      - io_list: [{job_name, inputs[], outputs[]}]
      - downstream: list of dataset names produced downstream
    Any edges to/from bootstrap.* jobs are ignored.
    """
    nodes = lineage_graph.get("graph", {}).get("nodes", [])
    edges = lineage_graph.get("graph", {}).get("edges", [])
    node_by_id = {n["id"]: n for n in nodes}

    # Map job node id → job name, and track which job nodes are bootstrap/system
    job_name_by_id: Dict[str, str] = {
        n["id"]: n["name"] for n in nodes if n.get("type") == "JOB"
    }
    bootstrap_job_ids = {
        jid for jid, jname in job_name_by_id.items() if _is_bootstrap(jname)
    }

    job_ios: Dict[str, Dict[str, set]] = {}
    downstream = set()

    for e in edges:
        src = node_by_id.get(e["source"])
        dst = node_by_id.get(e["target"])
        if not src or not dst:
            continue

        # Skip any edge where either endpoint is a bootstrap job node
        if src.get("type") == "JOB" and e["source"] in bootstrap_job_ids:
            continue
        if dst.get("type") == "JOB" and e["target"] in bootstrap_job_ids:
            continue

        if src.get("type") == "JOB" and dst.get("type") == "DATASET":
            job_name = job_name_by_id[src["id"]]
            if _is_bootstrap(job_name):
                continue
            job_ios.setdefault(job_name, {"inputs": set(), "outputs": set()})
            job_ios[job_name]["outputs"].add(dst["name"])
            downstream.add(dst["name"])

        elif src.get("type") == "DATASET" and dst.get("type") == "JOB":
            job_name = job_name_by_id[dst["id"]]
            if _is_bootstrap(job_name):
                continue
            job_ios.setdefault(job_name, {"inputs": set(), "outputs": set()})
            job_ios[job_name]["inputs"].add(src["name"])

    io_list = [
        {
            "job_name": j,
            "inputs": sorted(v["inputs"]),
            "outputs": sorted(v["outputs"]),
        }
        for j, v in job_ios.items()
        if not _is_bootstrap(j)
    ]

    return io_list, sorted(downstream)
