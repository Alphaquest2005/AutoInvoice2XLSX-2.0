"""Tests for legacy pipeline/ code.

These tests cover the pre-v2 pipeline modules that haven't been ported to the
hexagonal src/autoinvoice layout yet (xlsx_validator, workflow/variance_fixer,
bl_xlsx_generator, etc.). They exist to prevent regressions in the three bugs
discovered while processing BL #TSCW18489131:

1. is_grouped false-positive on plain "SUBTOTAL" label
2. _update_variance_row overwriting the VARIANCE CHECK formula with a number
3. _force_adjustment stacking correction terms across repeated runs
"""
