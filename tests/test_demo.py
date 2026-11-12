import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class DemoTests(unittest.TestCase):
    def test_real_demo(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'capture.sqlite3'
            process=subprocess.run([sys.executable,'app.py','demo','--output',str(output)],capture_output=True,text=True,timeout=30)
            self.assertEqual(process.returncode,0,process.stderr)
            self.assertIsInstance(json.loads(process.stdout),dict)
            self.assertTrue(output.is_file())
