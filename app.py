import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

APP_TITLE = "Global CFM AsyncAPI Explorer"
DATA_PATH = Path(__file__).parent / "Global_CFM.json"

app = FastAPI(title=APP_TITLE)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- Helpers ----------
def deep_get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    for p in path:
        if not isinstance(cur, dict) or p not in cur:
            return default
        cur = cur[p]
    return cur

def read_json() -> Dict[str, Any]:
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Global_CFM.json not found at {DATA_PATH}")
    with open(DATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

@lru_cache(maxsize=1)
def get_components() -> Dict[str, Any]:
    data = read_json()
    comps = deep_get(data, ["components", "schemas"], {}) or {}
    if not isinstance(comps, dict):
        comps = {}
    return comps

def normalize_desc(desc: Optional[str], fallback_name: str) -> str:
    if isinstance(desc, str) and desc.strip():
        return desc.strip()
    return f'No description provided for "{fallback_name}".'

def ref_name(ref: str) -> str:
    if not isinstance(ref, str):
        return ""
    parts = ref.split("/")
    return parts[-1] if parts else ref

def as_str_list(v) -> List[str]:
    if v is None:
        return []
    if isinstance(v, list):
        return [str(x) for x in v]
    return [str(v)]

def clean_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    prefixes = ["definedAt", "is", "has", "x"]
    for p in prefixes:
        if name.startswith(p):
            name = name[len(p):]
            break
    words = []
    current = ""
    for c in name:
        if c.isupper() and current:
            words.append(current)
            current = c
        else:
            current += c
    if current:
        words.append(current)
    return "".join(word.capitalize() for word in words)

# ---------- Schema resolution ----------
def resolve_schema(schema_name: str, visited: Optional[Tuple[str, ...]] = None, depth: int = 0) -> Dict[str, Any]:
    visited = visited or tuple()
    if schema_name in visited:
        return {"title": schema_name, "type": "object", "description": f"Circular reference: {' → '.join(visited + (schema_name,))}", "attributes": []}

    comps = get_components()
    raw = comps.get(schema_name)
    if raw is None:
        return {"title": schema_name, "type": "object", "description": f"Unknown schema '{schema_name}'.", "attributes": []}

    title = raw.get("title", schema_name)
    stype = raw.get("type", "object")
    desc = normalize_desc(raw.get("description"), title)
    attrs: List[Dict[str, Any]] = []

    def add_property(name: str, node: Dict[str, Any]):
        ptype = node.get("type")
        examples = node.get("examples") or []
        description = normalize_desc(node.get("description"), name)
        children = []

        if "$ref" in node:
            ref = ref_name(node["$ref"])
            if depth < 2:
                child = resolve_schema(ref, visited + (schema_name,), depth + 1)
                children = child.get("attributes", [])
            else:
                children = [{"name": f"(ref) {ref}", "type": "object", "description": "Reference (collapsed)", "examples": []}]

        if ptype == "array":
            items = node.get("items", {})
            if "$ref" in items:
                iref = ref_name(items["$ref"])
                ptype = f"array of {iref}"
                if depth < 2:
                    child = resolve_schema(iref, visited + (schema_name,), depth + 1)
                    children = child.get("attributes", [])
                else:
                    children = [{"name": f"(ref) {iref}", "type": "object", "description": "Reference (collapsed)", "examples": []}]
            else:
                ptype = f"array of {items.get('type','object')}"
                if isinstance(items, dict) and items.get("properties"):
                    for subname, subnode in items.get("properties").items():
                        children.append(build_attr(subname, subnode, schema_name, visited, depth))

        x_since = node.get("x-since-version") or raw.get("x-since-version")
        x_ftype = node.get("x-field-type") or raw.get("x-field-type")
        x_tag = node.get("x-tag") or raw.get("x-tag")

        attrs.append({
            "name": clean_name(name),
            "type": ptype or "object",
            "description": description,
            "examples": examples if isinstance(examples, list) else [examples],
            "xSinceVersion": x_since,
            "xFieldType": x_ftype,
            "xTag": x_tag,
            "children": children
        })

    def build_attr(name: str, node: Dict[str, Any], owner: str, visited_tuple: Tuple[str, ...], dpth: int) -> Dict[str, Any]:
        ptype = node.get("type", "object")
        description = normalize_desc(node.get("description"), name)
        examples = node.get("examples") or []
        children: List[Dict[str, Any]] = []

        if "$ref" in node:
            r = ref_name(node["$ref"])
            if dpth < 2:
                child_schema = resolve_schema(r, visited_tuple + (owner,), dpth + 1)
                children = child_schema.get("attributes", [])
            else:
                children = [{"name": f"(ref) {r}", "type": "object", "description": "Reference (collapsed)", "examples": []}]
            ptype = r

        if node.get("properties"):
            for subname, subnode in node.get("properties").items():
                children.append(build_attr(subname, subnode, owner, visited_tuple, dpth))

        if ptype == "array" or node.get("type") == "array":
            items = node.get("items", {})
            it_type = items.get("type", "object")
            if "$ref" in items:
                r = ref_name(items["$ref"])
                ptype = f"array of {r}"
                if dpth < 2:
                    child_schema = resolve_schema(r, visited_tuple + (owner,), dpth + 1)
                    children = child_schema.get("attributes", [])
                else:
                    children = [{"name": f"(ref) {r}", "type": "object", "description": "Reference (collapsed)", "examples": []}]
            else:
                ptype = f"array of {it_type}"

        return {
            "name": clean_name(name),
            "type": ptype,
            "description": description,
            "examples": examples if isinstance(examples, list) else [examples],
            "children": children
        }

    for pname, pnode in (raw.get("properties") or {}).items():
        add_property(pname, pnode)

    if "allOf" in raw and isinstance(raw["allOf"], list):
        for member in raw["allOf"]:
            if "$ref" in member:
                base = ref_name(member["$ref"])
                if depth < 2:
                    child = resolve_schema(base, visited + (schema_name,), depth + 1)
                    attrs.append({
                        "name": f"(allOf) {clean_name(base)}",
                        "type": "object",
                        "description": f"Inherits from {base}.",
                        "examples": [],
                        "children": child.get("attributes", [])
                    })
                else:
                    attrs.append({"name": f"(allOf) {clean_name(base)}", "type": "object", "description": "Inheritance (collapsed).", "examples": [], "children":[]})

    return {
        "title": title,
        "type": stype,
        "description": desc,
        "xSinceVersion": raw.get("x-since-version"),
        "xFieldType": raw.get("x-field-type"),
        "xTag": raw.get("x-tag"),
        "attributes": attrs
    }

# ---------- Recursive $ref finder ----------
def find_all_refs(node: dict) -> List[str]:
    refs = []
    if not isinstance(node, dict):
        return refs
    if "$ref" in node:
        refs.append(ref_name(node["$ref"]))
    if "items" in node and isinstance(node["items"], dict):
        refs.extend(find_all_refs(node["items"]))
    if "properties" in node:
        for p in node["properties"].values():
            refs.extend(find_all_refs(p))
    for key in ("allOf", "oneOf", "anyOf"):
        if key in node and isinstance(node[key], list):
            for item in node[key]:
                refs.extend(find_all_refs(item))
    return refs

# ---------- API ----------
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/tree")
def get_tree():
    comps = get_components()
    nodes = []
    for name, sch in comps.items():
        text = sch.get("title") or name
        nodes.append({
            "id": name,
            "text": text,
            "tag": sch.get("x-tag"),
            "type": sch.get("type", "object"),
            "xFieldType": (sch.get("x-field-type") or "OTHERS").upper()
        })
    nodes.sort(key=lambda x: x["text"].lower())
    return nodes

# ---------- ✅ Fixed Versions Endpoint ----------
@app.get("/versions")
def get_versions():
    """Return all unique 'x-since-version' from schemas, fallback to info.version"""
    data = read_json()
    comps = deep_get(data, ["components", "schemas"], {}) or {}
    versions_set = set()

    # Collect all x-since-version from schemas
    for schema in comps.values():
        v = schema.get("x-since-version")
        if v and v != "Unknown":
            versions_set.add(str(v))

    # Fallback to info.version if no versions found
    info_version = data.get("info", {}).get("version")
    if info_version:
        versions_set.add(str(info_version))

    # Return sorted versions (numerical sorting for versions like 1.2, 1.10)
    def version_key(s):
        return [int(p) if p.isdigit() else p for p in s.split(".")]
    
    return sorted(versions_set, key=version_key)

@app.get("/schema/{name}")
def get_schema(name: str):
    comps = get_components()
    if name not in comps:
        raise HTTPException(404, detail=f"Schema '{name}' not found")

    resolved = resolve_schema(name)

    refs = list(set(find_all_refs(comps[name])))

    ref_by = []
    for other, schema in comps.items():
        if other == name:
            continue
        if name in find_all_refs(schema):
            ref_by.append(other)
    ref_by = list(set(ref_by))

    resolved["references"] = refs
    resolved["referencedBy"] = ref_by
    resolved["relationshipSummary"] = {
        "referencedByCount": len(ref_by),
        "referencesCount": len(refs)
    }

    return resolved

@app.get("/search")
def search(q: str = ""):
    q = (q or "").strip().lower()
    comps = get_components()
    results = []
    if not q:
        return results
    for name, sch in comps.items():
        hay = " ".join(filter(None, [
            name,
            sch.get("title", ""),
            sch.get("description", ""),
            sch.get("x-tag", ""),
            sch.get("x-field-type", ""),
        ])).lower()
        if q in hay:
            results.append({"id": name, "text": sch.get("title") or name})
    results.sort(key=lambda x: x["text"].lower())
    return results

# ---------- Static UI ----------
STATIC_DIR = Path(__file__).parent / "static"
STATIC_DIR.mkdir(exist_ok=True)

INDEX_FILE = STATIC_DIR / "index.html"

@app.get("/", response_class=HTMLResponse)
def index():
    if INDEX_FILE.exists():
        return INDEX_FILE.read_text(encoding="utf-8")
    return HTMLResponse(f"<h1>{APP_TITLE}</h1><p>UI not found.</p>", status_code=200)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# ---------- Run ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="127.0.0.1", port=8000, reload=True)
