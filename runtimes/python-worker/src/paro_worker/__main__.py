# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

import os
import sys
import threading

from .protocol import encode
from .server import Server


def main():
    if sys.version_info < (3, 11):
        raise RuntimeError("Paro requires Python >=3.11")
    token = os.environ.pop("PARO_WORKER_TOKEN")
    epoch = os.environ.pop("PARO_WORKER_EPOCH")
    server = Server(token, epoch)
    print(encode(port=server.port, epoch=epoch).decode(), flush=True)

    def watch_host():
        sys.stdin.buffer.read()
        server.stop()

    threading.Thread(target=watch_host, daemon=True).start()
    server.serve()


if __name__ == "__main__":
    main()
