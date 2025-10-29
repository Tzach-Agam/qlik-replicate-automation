import subprocess
import os
import sys
from pathlib import Path
from time import sleep

# --- Configuration ---
PYTHON_EXE = sys.executable
TEST_DIR = Path(__file__).resolve().parents[2] / "transformation"

# --- State ---
total_runs = 0
passed_runs = 0
failed_runs = 0
results_output = []

print("Starting isolated test run (Dynamic Output & Guaranteed Summary)...")
print("-" * 60)

# --- Find all test files recursively ---
test_files = sorted(TEST_DIR.rglob('*_test.py'))

if not test_files:
    print(f"⚠️  No test files found under: {TEST_DIR}")
    print("   (Tip: ensure filenames end with '_test.py' and directory is correct.)")
else:
    print(f"📂 Found {len(test_files)} test file(s) under: {TEST_DIR}")
    print("-" * 60)

# --- Run each test file ---
for test_file in test_files:
    total_runs += 1

    command = [
        PYTHON_EXE,
        '-u',  # Force unbuffered output
        '-m', 'pytest',
        str(test_file),
        '--tb=short',  # Short traceback format
        '--capture=no',  # Disable pytest output capturing
        '-s',  # Alias for --capture=no
        '--no-header',  # Remove pytest header
        # NOTE: Removed --no-summary because it hides errors!
        # NOTE: Removed -q because it suppresses output
        # NOTE: Removed -v to reduce noise
        '--log-cli-level=ERROR',  # Only show ERROR logs, not INFO
        '--disable-warnings',  # Suppress warnings
    ]

    print(f"\n🚀 Running: {test_file.relative_to(TEST_DIR.parent.parent)}")
    print("-" * 60)

    return_code = 1

    try:
        # Line-buffered output for real-time printing
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,  # Line-buffered
            encoding='utf-8',
            cwd=os.getcwd(),
            env={**os.environ, 'PYTHONUNBUFFERED': '1'}
        )

        # Real-time output reading
        for line in iter(process.stdout.readline, ''):
            if line:
                # Filter out ONLY the initial pytest noise, keep everything else
                skip_line = False

                if line.startswith('platform win32'):
                    skip_line = True
                elif line.startswith('cachedir:'):
                    skip_line = True
                elif line.startswith('rootdir:'):
                    skip_line = True
                elif line.startswith('configfile:'):
                    skip_line = True
                elif line.startswith('plugins:'):
                    skip_line = True
                elif 'collecting ...' in line and 'collected' not in line:
                    skip_line = True
                elif line.strip().startswith('collected') and 'item' in line:
                    skip_line = True
                elif 'WDM:logger' in line:
                    skip_line = True
                elif 'Driver [' in line and '.wdm' in line:
                    skip_line = True
                elif 'Get LATEST' in line and 'chromedriver' in line:
                    skip_line = True

                if not skip_line:
                    print(line, end='', flush=True)

        process.stdout.close()
        return_code = process.wait()

        if return_code == 0:
            passed_runs += 1
            results_output.append(f"✅ PASSED: {test_file.name}")
        elif return_code == 1:
            failed_runs += 1
            results_output.append(f"❌ FAILED: {test_file.name}")
        else:
            failed_runs += 1
            results_output.append(f"⚠️ ERROR: {test_file.name} (Code {return_code})")

    except Exception as e:
        failed_runs += 1
        results_output.append(f"💥 EXCEPTION (Parent Script Crash): {test_file.name} - {e}")
        print(f"Exception occurred: {e}")

    print("-" * 60)
    sleep(1)

# --- FINAL CONSOLIDATED SUMMARY ---
print("\n" * 2)
print("=" * 40)
print("FINAL ISOLATED TEST SUMMARY")
print("=" * 40)

for line in results_output:
    print(line)

print("\n" + "=" * 40)
print(f"Total Runs: {total_runs}")
print(f"Passed:     {passed_runs}")
print(f"Failed:     {failed_runs}")
print("=" * 40)
sys.stdout.flush()