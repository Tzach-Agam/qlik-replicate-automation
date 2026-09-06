import json
import os
import posixpath
import tempfile
import time
import uuid

import ebcdic  # registers cp1047 and other EBCDIC codecs with Python's codec registry
import paramiko

from configurations.config_manager import ConfigurationManager


class VSAMDatabase:
    """A class for interacting with VSAM datasets on z/OS via SSH.

    Unlike IMS, there is no JDBC/SQL layer over VSAM - data is manipulated
    through the mainframe-resident `vsam_util` Java CLI (load/read/clear),
    invoked here over an SSH connection to the z/OS USS shell. The class
    uploads copybooks and JSON record files via SFTP, then runs vsam_util
    commands remotely and parses the results.

    No CDC support is provided here - Full Load only, per RP-14759's current
    scope. There is no equivalent to IMSDatabase.sync_command().
    """

    def __init__(self, config_manager: ConfigurationManager, config_section: str):
        """Initialize the VSAMDatabase instance."""
        self.section = config_manager.get_section(config_section)
        self.ssh = None
        self.sftp = None
        self.remote_dir = self.section.get('vsam_util_dir', '/u/tzacha/vsam/vsam_util')

    def connect(self):
        """Open the SSH + SFTP connection to the z/OS USS shell."""
        try:
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(
                hostname=self.section['host'],
                port=int(self.section.get('ssh_port', 22)),
                username=self.section['user'],
                password=self.section['password'],
                look_for_keys=False,
                allow_agent=False,
                timeout=30,
            )
            self.sftp = self.ssh.open_sftp()
            print(f"Connected successfully to z/OS ({self.section['host']}) via SSH!")
        except Exception as e:
            print("VSAM SSH connection failed:", e)
            raise

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------
    def _run(self, command: str, timeout: int = 120):
        """Run a command in the vsam_util directory and return (exit_status, stdout, stderr).

        vsam_util does not always give a clear success/failure message on its
        own (see RP-14811) - callers should not rely on exit_status alone for
        correctness; always verify important data with read_records().
        """
        if not self.ssh:
            raise Exception("Connection is not established. Call 'connect()' method first.")
        full_command = f"cd {self.remote_dir} && {command}"
        stdin, stdout, stderr = self.ssh.exec_command(full_command, timeout=timeout)
        exit_status = stdout.channel.recv_exit_status()
        out = stdout.read().decode('utf-8', errors='replace')
        err = stderr.read().decode('utf-8', errors='replace')
        return exit_status, out, err

    def upload_file(self, local_path: str, remote_filename: str = None) -> str:
        """Upload a local file into the vsam_util directory. Returns the remote filename used."""
        remote_filename = remote_filename or os.path.basename(local_path)
        remote_path = posixpath.join(self.remote_dir, remote_filename)
        self.sftp.put(local_path, remote_path)
        return remote_filename

    def _upload_json(self, records: list, filename_hint: str) -> str:
        """Write records to a temp local JSON file and upload it. Returns remote filename."""
        unique = f"{filename_hint}_{uuid.uuid4().hex[:8]}.json"
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
            json.dump(records, f, indent=2)
            local_path = f.name
        try:
            return self.upload_file(local_path, unique)
        finally:
            os.remove(local_path)

    # ------------------------------------------------------------------
    # vsam_util operations
    # ------------------------------------------------------------------
    def clear(self, dataset: str):
        """Clear (empty) a VSAM dataset. Does not delete/redefine the cluster itself."""
        exit_status, out, err = self._run(f"./vsam_util.sh clear {dataset}")
        print(f"clear({dataset}) -> exit={exit_status}\n{out}{err}")
        return exit_status, out, err

    def load_records(self, dataset: str, copybook_local_path: str, records: list,
                      clear_first: bool = True, upload_copybook: bool = True):
        """Load a list of record dicts (matching the vsam_util JSON schema, e.g.
        [{"STRUCT_SEGMENT": {...}}, ...]) into a VSAM dataset.

        :param dataset: fully-qualified VSAM dataset name, e.g. TZACHA.KSDS.ALLTYPES
        :param copybook_local_path: local path to the .cpy file (uploaded unless already present)
        :param records: list of record dicts, already shaped to the copybook's JSON schema
        :param clear_first: whether to clear the dataset before loading (recommended for
                             deterministic test runs)
        :param upload_copybook: set False to skip re-uploading a copybook already on the mainframe
        """
        if clear_first:
            self.clear(dataset)

        copybook_filename = os.path.basename(copybook_local_path)
        if upload_copybook:
            self.upload_file(copybook_local_path, copybook_filename)

        json_filename = self._upload_json(records, os.path.splitext(copybook_filename)[0])

        exit_status, out, err = self._run(
            f"./vsam_util.sh load {dataset} {copybook_filename} {json_filename}"
        )
        print(f"load_records({dataset}, {len(records)} records) -> exit={exit_status}\n{out}{err}")
        if exit_status != 0:
            raise RuntimeError(f"vsam_util load failed for {dataset}: {err or out}")
        return exit_status, out, err

    def read_records(self, dataset: str, copybook_local_path: str, upload_copybook: bool = False) -> list:
        """Read all records currently in a VSAM dataset back as a list of dicts.

        Use this for independent verification after load_records() - do not trust
        vsam_util's exit status alone as proof the data is correct (RP-14811).
        """
        copybook_filename = os.path.basename(copybook_local_path)
        if upload_copybook:
            self.upload_file(copybook_local_path, copybook_filename)

        remote_output = f"readback_{uuid.uuid4().hex[:8]}.json"
        exit_status, out, err = self._run(
            f"./vsam_util.sh read {dataset} {copybook_filename} {remote_output}"
        )
        if exit_status != 0:
            raise RuntimeError(f"vsam_util read failed for {dataset}: {err or out}")

        with tempfile.NamedTemporaryFile(mode='r', suffix='.json', delete=False) as f:
            local_path = f.name
        try:
            self.sftp.get(posixpath.join(self.remote_dir, remote_output), local_path)
            # vsam_util writes its "read" output in EBCDIC (the JVM's platform default on
            # z/OS USS), not UTF-8 - decode as cp1047 before parsing as JSON.
            with open(local_path, 'rb') as f:
                raw = f.read()
            return json.loads(raw.decode('cp1047'))
        finally:
            os.remove(local_path)
            self._run(f"rm -f {remote_output}")

    def close(self):
        """Close the SSH/SFTP connection."""
        try:
            if self.sftp:
                self.sftp.close()
            if self.ssh:
                self.ssh.close()
            print("VSAM SSH connection closed.")
        except Exception as e:
            print("Error closing VSAM connection:", e)