Split a Google Analytics into two flat:

visits.json – one JSON line per session 

hits.json – one JSON line per individual hit

Any malformed line is skipped and recorded in an auto-named error log:

`<filename>_error.log`

## Requirements

Python 3.8+ (uses only the standard library: json, gzip, pathlib, logging).

## Running the script

with plain JSON file

`python data_extract.py ga_sessions_20160801.json`

gzip-compressed file

`python data_extract.py ga_sessions_20160801.json.gz`

read from STDIN

`cat ga_sessions_20160801.json | python data_extract.py - `

`
gunzip -c ga_sessions_20160801.json.gz  | python data_extract.py -
`

---------------------------------------------------------

## What counts as “bad” and goes to the log?

broken JSON – any line that can’t be parsed.

schema failures

**missing required keys -> fullVisitorId, visitId, visitStartTime, hits**

wrong types:

fullVisitorId -> must be a string

visitId, visitStartTime, hitNumber -> ints or digit-string(digit-strings are auto-cast)

hits -> must be a list

each hit must contain hitNumber & type (type must be a string)

example:
```
[WARN] line 1707: hit_1_hitNumber_not_int
[WARN] line 1882: visitId_not_int
```