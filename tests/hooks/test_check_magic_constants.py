"""Unit tests for scripts/hooks/check_magic_constants.py.

Covers the three invariants that matter for enforcement:
    1. Hard-exempt literals (0/1/-1/2/-2, True/False/None, empty/ws strings,
       encoding names, file modes, dunders, docstrings, empty bytes, ...)
       must NEVER appear as violations.
    2. Non-exempt literals MUST appear as violations — including strings,
       floats, bytes, integers, and nested container members.
    3. '# magic-ok: <reason>' on the SAME line flips has_bypass=True but
       still records the violation (so the registry hook can audit it).
"""

import sys
from pathlib import Path

import pytest

HOOKS_DIR = Path(__file__).resolve().parents[2] / "scripts" / "hooks"
if str(HOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(HOOKS_DIR))

import check_magic_constants as detector  # noqa: E402


def _scan(src):
    return detector.scan(src, filename="t.py")


def _open_values(src):
    """Return the set of value_repr for UNBYPASSED violations only."""
    return {v["value_repr"] for v in _scan(src) if not v["has_bypass"]}


# ---------------------------------------------------------------- exemptions


@pytest.mark.parametrize(
    "literal",
    [
        "0",
        "1",
        "-1",
        "2",
        "-2",
        "True",
        "False",
        "None",
        "...",
    ],
)
def test_exempt_numeric_and_singletons(literal):
    assert _open_values(f"x = {literal}") == set()


@pytest.mark.parametrize(
    "literal",
    [
        '""',
        "''",
        '" "',
        '"\\n"',
        '"\\t"',
        '"\\r"',
        '"utf-8"',
        '"utf-16"',
        '"ascii"',
        '"latin-1"',
        '"cp1252"',
        '"r"',
        '"w"',
        '"a"',
        '"rb"',
        '"wb"',
        '"wt"',
        '"r+"',
        '"__main__"',
        '"__name__"',
        '"__init__"',
        "b''",
    ],
)
def test_exempt_strings_and_dunders(literal):
    assert _open_values(f"x = {literal}") == set()


def test_module_docstring_is_not_a_violation():
    src = '"""Module docstring goes here and is long enough to stand out."""\n'
    assert _open_values(src) == set()


def test_function_and_class_docstrings_are_not_violations():
    src = (
        "def f():\n"
        '    """Function docstring stays exempt."""\n'
        "    return 0\n"
        "class C:\n"
        '    """Class docstring stays exempt too."""\n'
        "    pass\n"
    )
    assert _open_values(src) == set()


# ---------------------------------------------------------------- violations


def test_integer_literal_is_a_violation():
    assert _open_values("x = 42") == {"42"}


def test_float_literal_is_a_violation():
    assert _open_values("x = 3.14") == {"3.14"}


def test_string_literal_is_a_violation():
    assert _open_values('x = "hello"') == {"'hello'"}


def test_bytes_literal_is_a_violation():
    assert _open_values("x = b'abc'") == {"b'abc'"}


def test_nested_literals_inside_containers_are_flagged():
    src = "x = {'key': [100, 'v']}"
    # 'key' is a dict KEY (role-exempt, Path 1).
    # 100 and 'v' are VALUES — still flagged.
    assert _open_values(src) == {"100", "'v'"}


def test_negative_large_integer_is_flagged():
    # AST parses `-999` as UnaryOp(USub, Constant(999)), so the Constant
    # node holds 999. The unary minus is reconstructed by the user reading
    # the context line. We just verify the magnitude is caught.
    assert _open_values("x = -999") == {"999"}


# ---------------------------------------------------------------- role exemptions (Path 1)


def test_dict_literal_keys_are_exempt():
    src = "x = {'a': 1, 'b': 2}"
    # 1 and 2 are hard-exempt numbers; 'a' and 'b' are dict KEYS.
    assert _open_values(src) == set()


def test_subscript_string_index_is_exempt():
    src = "x = row['total_cost']"
    assert _open_values(src) == set()


def test_subscript_assignment_key_is_exempt():
    src = "row['total_cost'] = 100"
    # Key is exempt; 100 is a policy value, still flagged.
    assert _open_values(src) == {"100"}


def test_dict_get_first_arg_is_exempt():
    src = "v = d.get('total_cost', 0)"
    # 'total_cost' exempt; 0 hard-exempt.
    assert _open_values(src) == set()


def test_dict_pop_and_setdefault_are_exempt():
    src = "a = d.pop('k1')\nb = d.setdefault('k2', [])\n"
    assert _open_values(src) == set()


def test_logger_call_messages_are_exempt():
    src = (
        "logger.info('starting run')\n"
        "log.warning('slow operation')\n"
        "self.logger.error('boom %s', x)\n"
    )
    assert _open_values(src) == set()


def test_logger_kwarg_values_are_exempt():
    src = "logger.info(msg='hello world')"
    assert _open_values(src) == set()


def test_exception_messages_are_exempt():
    src = "raise ValueError('bad input')\nraise RuntimeError('bang')\nraise KeyError('missing')\n"
    assert _open_values(src) == set()


def test_custom_exception_by_name_suffix_is_exempt():
    src = "raise MyDomainError('context')"
    assert _open_values(src) == set()


def test_attribute_exception_suffix_is_exempt():
    src = "raise self.custom.DomainError('context')"
    assert _open_values(src) == set()


def test_fstring_literal_segments_are_exempt():
    # The "Invoice: " and "!" Constants are JoinedStr children.
    src = "msg = f'Invoice: {n}!'"
    assert _open_values(src) == set()


def test_fstring_format_spec_is_exempt():
    src = "msg = f'{x:.2f}'"
    assert _open_values(src) == set()


def test_short_punctuation_strings_are_exempt():
    src = "a = ': '\nb = ','\nc = ')'\nd = ' | '\n"
    assert _open_values(src) == set()


def test_alphanumeric_short_string_is_still_flagged():
    # "ok" is 2 chars but alphabetic — NOT punctuation-exempt.
    assert _open_values("x = 'ok'") == {"'ok'"}


def test_assignment_value_is_still_flagged_after_path1():
    # Assignment-RHS is the Budget Marine bug class — MUST stay flagged.
    assert _open_values("doc_type = '7400-000'") == {"'7400-000'"}


def test_comparison_rhs_is_still_flagged():
    src = "if consignee == 'Budget Marine':\n    pass\n"
    assert _open_values(src) == {"'Budget Marine'"}


def test_return_string_is_still_flagged():
    src = "def resolve():\n    return '4000-000'\n"
    assert _open_values(src) == {"'4000-000'"}


def test_list_of_strings_is_still_flagged():
    # Lists are value containers, not dict keys.
    src = "COLS = ['total_cost', 'freight']"
    assert _open_values(src) == {"'total_cost'", "'freight'"}


def test_nested_dict_values_are_still_flagged():
    src = "x = {'outer': {'inner': 'value'}}"
    # 'outer' and 'inner' are keys (exempt); 'value' is a value (flagged).
    assert _open_values(src) == {"'value'"}


# ---------------------------------------------------------------- bypass


def test_magic_ok_flips_has_bypass_but_still_records():
    src = "x = 42  # magic-ok: answer to everything\n"
    vs = _scan(src)
    assert len(vs) == 1
    assert vs[0]["has_bypass"] is True
    assert vs[0]["bypass_reason"] == "answer to everything"


def test_magic_ok_without_reason_does_not_bypass():
    # Reason is required; no reason => no bypass.
    src = "x = 42  # magic-ok:\n"
    vs = _scan(src)
    assert vs[0]["has_bypass"] is False


def test_magic_ok_only_applies_to_its_own_line():
    src = "x = 42  # magic-ok: rationale\ny = 99\n"
    vs = _scan(src)
    bypassed = [v for v in vs if v["has_bypass"]]
    open_ = [v for v in vs if not v["has_bypass"]]
    assert len(bypassed) == 1 and bypassed[0]["value_repr"] == "42"
    assert len(open_) == 1 and open_[0]["value_repr"] == "99"


# ---------------------------------------------------------------- misc


def test_syntax_error_reports_but_does_not_crash():
    vs = _scan("def (:\n")
    assert len(vs) == 1
    assert vs[0]["kind"] == "syntax_error"


def test_empty_source_has_no_violations():
    assert _scan("") == []


def test_ellipsis_is_exempt():
    assert _open_values("def f(): ...") == set()
