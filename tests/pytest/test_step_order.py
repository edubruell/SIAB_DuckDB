"""
The guard on the hand-written step order, Python side.

The pipeline's step order is spelled out by hand in five scripts, two of them
Python: python/main.py and tests/pytest/make_py_dumps.py. Nothing generates
them from one definition, and the user ruled on 2026-09-21c that the
duplication stays (contention C7). What replaces a generator is
tests/step_order.json and these tests: the file states the order once, each
script declares the steps it leaves out and the arguments it passes
differently, and a change made in some scripts and not the others fails here.

The failure this is for is the silent one. A step added to one arm and not the
other, or an argument changed in one script, leaves every comparison running
and no longer testing what it claims to test. The comparisons cannot see it:
they check values, and a pipeline that skipped a step still produces values.

tests/testthat/test-step_order.R is the same guard over the three R scripts,
reading the same file.
"""

from __future__ import annotations

import json
import re

import pytest

from conftest import PROJECT

DEFINITION = json.loads((PROJECT / "tests" / "step_order.json").read_text())
CANONICAL: list[str] = DEFINITION["steps"]
SCRIPTS = {name: spec for name, spec in DEFINITION["scripts"].items()
           if spec["arm"] == "python"}


def _uncommented(path: str) -> list[str]:
    """The script's lines with the comment-only ones dropped.

    Both scripts declare a skipped step by commenting its call out, and both
    carry long explanatory comments that name steps they do not call, so a
    parser that reads comments would see calls that do not happen.
    """
    text = (PROJECT / path).read_text().splitlines()
    return [line for line in text if not line.lstrip().startswith("#")]


def step_calls(path: str) -> list[str]:
    """The sequence of step calls a script makes, in the order it makes them.

    Both scripts run every step through a local `run()` helper, which takes the
    step as its first argument, after the store in the dump writer. Names that
    are not steps, the helpers' own parameters among them, are dropped.
    """
    pattern = re.compile(r"\brun\(\s*(?:con\s*,\s*)?([A-Za-z_][A-Za-z_0-9]*)")
    called = pattern.findall("\n".join(_uncommented(path)))
    return [name for name in called if name in CANONICAL]


def handling_arguments(path: str) -> list[str]:
    """Every value a script passes as `handling=`.

    That is the one argument the runners and the dump writers deliberately
    disagree on: the dumps sort parallel episodes on tenure, because the wage
    setting sorts on draws that differ between the arms by construction.
    """
    hits = re.findall(r"handling\s*=\s*\"([a-z]+)\"", "\n".join(_uncommented(path)))
    return sorted(set(hits))


def test_the_definition_lists_each_step_once():
    assert len(set(CANONICAL)) == len(CANONICAL)
    assert CANONICAL


@pytest.mark.parametrize("path", sorted(SCRIPTS))
def test_the_script_runs_the_steps_in_the_canonical_order(path):
    skipped = set(SCRIPTS[path]["skips"])
    assert skipped <= set(CANONICAL), "a skip names a step the definition does not list"
    assert step_calls(path) == [s for s in CANONICAL if s not in skipped]


@pytest.mark.parametrize("path", sorted(SCRIPTS))
def test_the_script_passes_the_arguments_it_declares(path):
    assert handling_arguments(path) == [SCRIPTS[path]["arguments"]["handling"]]
