#!/usr/bin/env python3
# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import sys


def main() -> int:
    if len(sys.argv) >= 3 and sys.argv[1] == "-c":
        namespace = {"__name__": "__main__", "sys": sys}
        exec(sys.argv[2], namespace)
        return 0

    if len(sys.argv) >= 3 and sys.argv[1] == "-m" and sys.argv[2] == "paro_runtime_worker":
        sys.stderr.write("simulated worker bootstrap crash\n")
        return 97

    sys.stderr.write(f"unsupported simulated python invocation: {sys.argv[1:]}\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
