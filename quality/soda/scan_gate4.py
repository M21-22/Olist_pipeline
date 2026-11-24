"""Run the local SodaCL scan and expose only check identities and outcomes."""

import json
import os
import sys
from pathlib import Path

from ruamel.yaml import YAML
from soda.scan import Scan


def main(result_path):
    root = Path(__file__).resolve().parent
    scan = Scan()
    scan.set_data_source_name("warehouse")
    scan.add_variables({
        name: json.dumps(os.environ[name], ensure_ascii=False).replace("$", r"\u0024")
        for name in ("SODA_HOST", "SODA_READER_PASSWORD")
    })
    scan.add_configuration_yaml_file(str(root / "configuration.yml"))
    checks_path = root / "checks" / "marts.yml"
    expected = sum(len(section) for section in YAML(typ="safe").load(checks_path).values())
    scan.add_sodacl_yaml_file(str(checks_path))
    exit_code = scan.execute()
    checks = scan.get_scan_results().get("checks", [])
    result_path.write_text(json.dumps([
        {key: check.get(key) for key in ("identity", "name", "table", "outcome")}
        for check in checks
    ]))
    for check in checks:
        print(f"{check.get('outcome')}: {check.get('name')}")
    if exit_code >= 3:
        print(scan.get_error_logs_text(), file=sys.stderr)
    if not expected or len(checks) != expected:
        print(f"Soda evaluated {len(checks)} of {expected} checks.", file=sys.stderr)
        return 3
    return exit_code


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1])))
