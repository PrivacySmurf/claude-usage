"""Production-bound tests for the LaunchAgent startup script."""

import os
import plistlib
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
START_SCRIPT = REPO_ROOT / "start.sh"


class TestStartup(unittest.TestCase):
    def test_tracked_quota_launchagent_invokes_claude_quota_provider(self):
        plist_path = (
            REPO_ROOT / "Library" / "LaunchAgents" /
            "ai.ccagents.claude-rate-limit-poll.plist"
        )
        with plist_path.open("rb") as handle:
            launchagent = plistlib.load(handle)

        self.assertEqual(
            launchagent["ProgramArguments"],
            [
                "/usr/bin/python3",
                "/Users/zt_mini/.cc-agents/tools/claude-usage/cli.py",
                "scan",
                "--provider",
                "claude-quotas",
            ],
        )

    def test_tracked_dashboard_launchagent_invokes_start_script(self):
        plist_path = (
            REPO_ROOT / "Library" / "LaunchAgents" /
            "ai.ccagents.claude-usage.plist"
        )
        self.assertTrue(plist_path.is_file(), "dashboard LaunchAgent must be source-tracked")
        with plist_path.open("rb") as handle:
            launchagent = plistlib.load(handle)

        self.assertEqual(launchagent["Label"], "ai.ccagents.claude-usage")
        self.assertEqual(launchagent["ProgramArguments"][:2], ["/bin/bash", "-l"])
        self.assertTrue(
            launchagent["ProgramArguments"][2].endswith(
                "/tools/claude-usage/start.sh"
            )
        )

    def test_startup_forces_loopback_even_when_host_is_overridden(self):
        self.assertTrue(START_SCRIPT.is_file(), "start.sh must be source-tracked")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            trace = root / "python-trace.txt"
            python_stub = fake_bin / "python3"
            python_stub.write_text(
                "#!/bin/bash\n"
                "printf '%s|%s\\n' \"${HOST-}\" \"$*\" >> \"$TRACE_FILE\"\n"
            )
            python_stub.chmod(0o755)

            env = os.environ.copy()
            env.update({
                "HOME": str(root / "home"),
                "HOST": "0.0.0.0",
                "PATH": f"{fake_bin}:/usr/bin:/bin",
                "TRACE_FILE": str(trace),
            })
            result = subprocess.run(
                ["/bin/bash", str(START_SCRIPT)],
                cwd=REPO_ROOT,
                env=env,
                text=True,
                capture_output=True,
                timeout=10,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            calls = trace.read_text().splitlines()
            self.assertEqual(calls[0], "0.0.0.0|cli.py scan")
            self.assertEqual(calls[1], "127.0.0.1|dashboard.py 9123")
            self.assertNotIn("0.0.0.0|dashboard.py 9123", calls)


if __name__ == "__main__":
    unittest.main()
