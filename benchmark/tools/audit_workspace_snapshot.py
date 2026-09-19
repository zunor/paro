#!/usr/bin/env python3
"""Verify C0 backups using an isolated Git index and untracked archive bytes.

The original index and worktree are never written. Private snapshot contents
stay outside Git; only the returned inventory is suitable for review.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.repository.resolve()
    snapshot = args.snapshot.resolve()

    def git(*words, env=None):
        return subprocess.check_output(["git", "-C", str(root), *words], env=env)

    head = (snapshot / "head.txt").read_text().strip()
    if git("rev-parse", "HEAD").decode().strip() != head:
        raise ValueError("source HEAD changed since snapshot")
    for filename, words in (
        ("staged.patch", ("diff", "--cached", "--binary")),
        ("unstaged.patch", ("diff", "--binary")),
    ):
        if (snapshot / filename).read_bytes() != git(*words):
            raise ValueError(f"source {filename} changed since snapshot")
    inventory = []
    with tempfile.TemporaryDirectory(prefix="paro-index-restore-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        git("read-tree", head, env=env)
        for filename in ("staged.patch", "unstaged.patch"):
            patch = snapshot / filename
            if patch.stat().st_size:
                git("apply", "--cached", "--binary", str(patch), env=env)
        for raw in git("diff", "--cached", "--name-only", "-z", head, env=env).split(b"\0"):
            if not raw:
                continue
            name = os.fsdecode(raw)
            path = root / name
            exists = path.exists() or path.is_symlink()
            if exists:
                actual = os.fsencode(os.readlink(path)) if path.is_symlink() else path.read_bytes()
                restored = git("show", ":" + name, env=env)
                if actual != restored:
                    raise ValueError(f"restored tracked bytes differ: {name}")
            else:
                actual = b""
                if git("ls-files", "--", name, env=env):
                    raise ValueError(f"deleted file was restored: {name}")
            inventory.append({"path": name, "tracked": True, "exists": exists,
                              "sha256": digest(actual), "bytes": len(actual),
                              "owner": "pre-existing/unresolved; preserved, not integrated"})
    with tarfile.open(snapshot / "untracked.tar.gz") as archive:
        archived = set()
        expected = set((snapshot / "untracked.z").read_bytes().split(b"\0")) - {b""}
        apple_metadata = 0
        for member in archive:
            name = member.name.removeprefix("./")
            if Path(name).is_absolute() or ".." in Path(name).parts:
                raise ValueError("unsafe archive path")
            if os.fsencode(name) not in expected and Path(name).name.startswith("._"):
                # BSD tar stores macOS xattrs in auxiliary AppleDouble members.
                payload = archive.extractfile(member).read()
                if payload[:4] != bytes.fromhex("00051607"):
                    raise ValueError("unrecognized auxiliary archive member")
                apple_metadata += 1
                continue
            archived.add(os.fsencode(name))
            path = root / name
            if member.issym():
                actual = os.fsencode(os.readlink(path))
                restored = os.fsencode(member.linkname)
            elif member.isfile():
                actual = path.read_bytes()
                restored = archive.extractfile(member).read()
            else:
                raise ValueError(f"unexpected untracked member kind: {name}")
            if actual != restored:
                raise ValueError(f"untracked archive differs: {name}")
            inventory.append({"path": name, "tracked": False, "sha256": digest(actual),
                              "bytes": len(actual), "owner": "pre-existing/unresolved; preserved"})
        if archived != expected:
            raise ValueError("untracked inventory/archive mismatch")
    output = {
        "schema_version": 1, "repository": str(root), "head": head,
        "staged_sha256": digest((snapshot / "staged.patch").read_bytes()),
        "unstaged_sha256": digest((snapshot / "unstaged.patch").read_bytes()),
        "untracked_archive_sha256": digest((snapshot / "untracked.tar.gz").read_bytes()),
        "index_sha256": digest((snapshot / "index").read_bytes()),
        "restore_verified": True, "apple_metadata_members": apple_metadata,
        "files": inventory,
    }
    args.output.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({"repository": str(root), "verified_files": len(inventory),
                      "restore_verified": True}))


if __name__ == "__main__":
    main()
