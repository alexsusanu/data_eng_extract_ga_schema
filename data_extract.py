#!/usr/bin/env python3
import json, sys, gzip, pathlib, logging, datetime
from json import JSONDecodeError
from datetime import timezone

j = lambda o: json.dumps(o, separators=(",", ":"))
cast_int = lambda v: (int(v), True) if isinstance(v, str) and v.isdigit() else (v, isinstance(v, int))
smart_open = lambda p: sys.stdin if p=="-" else gzip.open(p,"rt") if p.endswith(".gz") else open(p,"r")

if len(sys.argv)!=2: sys.exit("Usage: data_extract.py FILENAME[.gz]")
in_path=sys.argv[1]
log_pt="stdin_error.log" if in_path=="-" else pathlib.Path(in_path).with_suffix("").name + "_error.log"
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
fh=logging.FileHandler(log_pt, "w"); fh.setFormatter(logging.Formatter("%(message)s")); logging.getLogger().addHandler(fh)
log_json = lambda **kv: logging.info(j(kv))

REQ_TOP       = ("fullVisitorId","visitId","visitStartTime","visitNumber","hits")
DIGITSTR_TOP  = {"visitId","visitStartTime","visitNumber"}
DIGITSTR_HIT  = {"hitNumber","time"}

def validate(s):
    for k in REQ_TOP:
        if k not in s: return False, f"{k}_missing"
    if not isinstance(s["fullVisitorId"], str): return False, "fullVisitorId_not_str"
    for k in DIGITSTR_TOP:
        s[k], ok = cast_int(s[k])
        if not ok: return False, f"{k}_not_int"
    if not isinstance(s["hits"], list): return False, "hits_not_list"
    for i, h in enumerate(s["hits"], 1):
        if not isinstance(h, dict): return False, f"hit_{i}_not_object"
        for k in DIGITSTR_HIT:
            if k not in h: return False, f"hit_{i}_{k}_missing"
            h[k], ok = cast_int(h[k])
            if not ok: return False, f"hit_{i}_{k}_not_int"
        if not isinstance(h.get("type"), str): return False, f"hit_{i}_type_not_str"
    return True, None

def split_stream(src, v_out, h_out):
    stats = {"visits":0,"hits":0,"bad":0}
    for ln, raw in enumerate(src, 1):
        raw = raw.strip()
        if not raw: continue
        try:
            ses = json.loads(raw)
        except JSONDecodeError as e:
            log_json(level="WARN", event="bad_json", line=ln, err=str(e)); stats["bad"]+=1; continue
        ok, why = validate(ses)
        if not ok:
            log_json(level="WARN", event="schema_fail", line=ln, reason=why); stats["bad"]+=1; continue

        visit_iso = datetime.datetime.fromtimestamp(ses["visitStartTime"], tz=timezone.utc).isoformat()
        v_out.write(j({
            "full_visitor_id":  ses["fullVisitorId"],
            "visit_id":         str(ses["visitId"]),
            "visit_number":     ses["visitNumber"],
            "visit_start_time": visit_iso,
            "browser":          ses.get("device",{}).get("browser"),
            "country":          ses.get("geoNetwork",{}).get("country")
        }) + "\n"); stats["visits"]+=1

        for h in ses["hits"]:
            hit_ts = datetime.datetime.fromtimestamp(
            ses["visitStartTime"] + h["time"]/1000.0,
            tz=timezone.utc).isoformat(timespec="milliseconds")
            page = h.get("page",{})
            h_out.write(j({
                "full_visitor_id":  ses["fullVisitorId"],
                "visit_id":         str(ses["visitId"]),
                "hit_number":       h["hitNumber"],
                "hit_type":         h["type"],
                "hit_timestamp":    hit_ts,
                "page_path":        page.get("pagePath"),
                "page_title":       page.get("pageTitle"),
                "hostname":         page.get("hostname")
            }) + "\n"); stats["hits"]+=1

    return stats

with smart_open(in_path) as src, open("visits.json","w") as v_f, open("hits.json","w") as h_f:
    st = split_stream(src, v_f, h_f)

print(f"[DONE] visits={st['visits']} hits={st['hits']} bad={st['bad']}", file=sys.stderr)
