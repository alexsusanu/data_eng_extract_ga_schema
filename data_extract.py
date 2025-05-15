#!/usr/bin/env python3
import json, sys, gzip, pathlib, logging
from json import JSONDecodeError

j          = lambda o: json.dumps(o, separators=(",", ":"))
cast_int   = lambda v: (int(v), True) if isinstance(v, str) and v.isdigit() else (v, isinstance(v, int))
smart_open = lambda p: sys.stdin if p == "-" else gzip.open(p, "rt") if p.endswith(".gz") else open(p, "r")

if len(sys.argv) != 2:
    sys.exit("Usage: data_extract.py FILENAME[.gz]")

in_path = sys.argv[1]
log_pt  = "stdin_error.log" if in_path == "-" else pathlib.Path(in_path).with_suffix("").name + "_error.log"
logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(message)s")
fh = logging.FileHandler(log_pt, "w"); fh.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(fh)
log_json = lambda **kv: logging.info(j(kv))

REQ_TOP       = ("fullVisitorId", "visitId", "visitStartTime", "hits")
DIGITSTR_TOP  = {"visitId", "visitStartTime"}
DIGITSTR_HIT  = {"hitNumber"}

def validate(s):
    for k in REQ_TOP:
        if k not in s:                    return False, f"{k}_missing"
    if not isinstance(s["fullVisitorId"], str):
        return False, "fullVisitorId_not_str"
    for k in DIGITSTR_TOP:
        s[k], ok = cast_int(s[k]);
        if not ok:                        return False, f"{k}_not_int"
    if not isinstance(s["hits"], list):   return False, "hits_not_list"

    for i, h in enumerate(s["hits"], 1):
        if not isinstance(h, dict):       return False, f"hit_{i}_not_object"
        for k in DIGITSTR_HIT:
            if k in h:
                h[k], ok = cast_int(h[k])
                if not ok:                return False, f"hit_{i}_{k}_not_int"
        if "hitNumber" not in h or "type" not in h:
            return False, f"hit_{i}_missing_fields"
        if not isinstance(h["type"], str):
            return False, f"hit_{i}_type_not_str"
    return True, None

def split_stream(src, v_out, h_out):
    stats = {"visits": 0, "hits": 0, "bad": 0}
    for ln, raw in enumerate(src, 1):
        raw = raw.strip()
        if not raw:
            continue
        try:
            ses = json.loads(raw)
        except JSONDecodeError as e:
            log_json(level="WARN", event="bad_json", line=ln, err=str(e)); stats["bad"] += 1; continue

        ok, why = validate(ses)
        if not ok:
            log_json(level="WARN", event="schema_fail", line=ln, reason=why); stats["bad"] += 1; continue

        v_out.write(j({k: v for k, v in ses.items() if k != "hits"}) + "\n")
        stats["visits"] += 1
        for h in ses["hits"]:
            h_out.write(j({**h,
                           "fullVisitorId":  ses["fullVisitorId"],
                           "visitId":        ses["visitId"],
                           "visitStartTime": ses["visitStartTime"]}) + "\n")
            stats["hits"] += 1
    return stats

with smart_open(in_path) as src, open("visits.json", "w") as v_f, open("hits.json", "w") as h_f:
    st = split_stream(src, v_f, h_f)

print(f"[DONE] visits={st['visits']} hits={st['hits']} bad={st['bad']}", file=sys.stderr)