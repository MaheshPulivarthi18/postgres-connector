"""The README's Python examples must run on every supported Python, including 3.8.

`list["Order"]` in a class annotation is evaluated when the class is defined, and
subscripting a builtin type only works from Python 3.9, so the docs use typing.List.
"""
import ast
import re
from pathlib import Path

README = Path(__file__).resolve().parent.parent / "README.md"
BUILTIN_GENERICS = {"list", "dict", "set", "frozenset", "tuple", "type"}


def python_blocks():
    return re.findall(r"```python\n(.*?)```", README.read_text(encoding="utf-8"), flags=re.S)


def builtin_subscripts(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.AnnAssign):
            for sub in ast.walk(node.annotation):
                if isinstance(sub, ast.Subscript) and isinstance(sub.value, ast.Name) and sub.value.id in BUILTIN_GENERICS:
                    yield node.lineno, sub.value.id


def test_readme_has_python_examples():
    assert python_blocks()


def test_readme_annotations_work_on_python_38():
    problems = []
    for i, block in enumerate(python_blocks()):
        # Examples use top-level await; wrap so they parse as a coroutine body.
        source = "async def _example():\n" + "".join(f"    {line}\n" for line in block.splitlines())
        tree = ast.parse(source)
        uses_future = any(
            isinstance(n, ast.ImportFrom) and n.module == "__future__" for n in ast.walk(tree)
        )
        if uses_future:
            continue
        problems += [f"block {i}, line {line}: {name}[...]" for line, name in builtin_subscripts(tree)]
    assert not problems, "Use typing.List/Dict in README examples: " + "; ".join(problems)
