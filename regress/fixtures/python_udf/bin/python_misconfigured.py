#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import sys


def main() -> int:
    sys.stderr.write("simulated misconfigured python runtime\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
