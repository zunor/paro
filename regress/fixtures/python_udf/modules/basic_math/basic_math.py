# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

def add_one(value: int) -> int:
    return value + 1


def shift_all(values, delta: int = 10):
    return [value + delta for value in values]
