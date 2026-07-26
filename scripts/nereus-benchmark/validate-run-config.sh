#!/usr/bin/env bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 DRIVER_YAML" >&2
  exit 2
fi

python3 - "$1" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
def field(name):
    match = re.search(rf"^\s+{re.escape(name)}:\s*([^#\s]+)", text, re.MULTILINE)
    return match.group(1) if match else None

values = {name: field(name) for name in
          ("campaignId", "blockId", "runId", "stage", "repetition", "seed",
           "namespaceSuffix", "managedLedgerStorageClassName", "compressionType",
           "subscriptionType")}
required = {"campaignId", "blockId", "runId", "stage", "repetition", "seed"}
if any(values[name] is None for name in required):
    raise SystemExit("run must contain campaignId/blockId/runId/stage/repetition/seed")
if values["stage"] not in "ABCDE":
    raise SystemExit("run.stage must be A-E")
if values["runId"] != values["namespaceSuffix"]:
    raise SystemExit("run.runId must equal client.namespaceSuffix")
expected = "bookkeeper" if values["stage"] in "AB" else "nereus"
if values["managedLedgerStorageClassName"] != expected:
    raise SystemExit(f"stage {values['stage']} requires storage class {expected}")
if values["compressionType"] not in ("NONE", "LZ4"):
    raise SystemExit("compressionType must be explicit NONE or LZ4")
if values["subscriptionType"] != "Shared":
    raise SystemExit("formal campaign requires Shared subscription")
print(f"validated {path}: stage={values['stage']} runId={values['runId']} seed={values['seed']}")
PY
