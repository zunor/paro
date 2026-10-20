# Copyright 2024-2026 Zunor
# SPDX-License-Identifier: Apache-2.0

"""Own the handler from birth through reap, outside its process group."""

import argparse
import os
import selectors
import signal
import subprocess
import sys
import time

from .protocol import TIMEOUT


def supervise(channel):
    revoked = False

    def revoke(_signal, _frame):
        nonlocal revoked
        revoked = True

    for handled in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(handled, revoke)
    response = int(os.environ["PARO_RESPONSE_FD"])
    with os.fdopen(channel, "rb", buffering=0) as lease, selectors.DefaultSelector() as events:
        events.register(lease, selectors.EVENT_READ)
        if revoked or events.select(0):
            return
        child = subprocess.Popen(
            [sys.executable, "-m", "paro_worker.handler"],
            stdin=sys.stdin.buffer, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            start_new_session=True, pass_fds=(response,),
        )
        os.close(response)
        try:
            deadline = time.monotonic() + TIMEOUT + 10
            while not revoked and time.monotonic() < deadline:
                # The channel carries no data. EOF revokes the service's lease.
                # Do not poll/reap the leader here: its PID must stay reserved
                # until after killpg, even when it exits before its descendants.
                if events.select(0.05):
                    break
        finally:
            # Always terminate descendants, including those left behind after
            # the handler exits. Only this owner signals the process group.
            try:
                os.killpg(child.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            child.wait()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("channel", type=int)
    arguments = parser.parse_args()
    if arguments.channel < 3:
        parser.error("invalid lifetime descriptor")
    supervise(arguments.channel)


if __name__ == "__main__":
    main()
