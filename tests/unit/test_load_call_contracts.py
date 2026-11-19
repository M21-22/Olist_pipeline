"""AST regression checks: caller positional arity matches local function signatures."""
import ast
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
AIRFLOW_ROOT = REPO / "airflow" if (REPO / "airflow" / "include").is_dir() else REPO
SOURCE = AIRFLOW_ROOT / "include" / "load.py"


def test_local_loader_calls_match_function_signatures():
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    for name in ("_copy_raw", "_load_transaction"):
        fn = functions[name]
        assert not fn.args.vararg, f"{name}: unexpected variadic args"
        expected = len(fn.args.posonlyargs) + len(fn.args.args)
        calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
        ]
        assert calls, f"{name}: no call site found"
        for call in calls:
            assert not call.keywords, f"{name}: expand this test for keyword calls"
            assert len(call.args) == expected, (
                f"{name} at line {call.lineno}: "
                f"expected {expected} arguments, got {len(call.args)}"
            )
