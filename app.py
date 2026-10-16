#!/usr/bin/env python3
"""
SaaS Cloud Controller over Local LAN with Hadoop HDFS Backend.

This Flask application serves as an enterprise-grade cloud storage gateway,
allowing local LAN clients to authenticate, inspect, upload, download, and delete
files stored inside a Hadoop HDFS cluster (/cloudlab/group1).
"""

import os
import re
import socket
import subprocess
from datetime import datetime
from functools import wraps

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    send_file,
    after_this_request,
    has_request_context
)
from werkzeug.utils import secure_filename

# ---------------------------------------------------------------------------
# Application Configuration & Constants
# ---------------------------------------------------------------------------

app = Flask(__name__)

# Base paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_DIR = os.path.join(BASE_DIR, "temp_uploads")
AUDIT_LOG_FILE = os.path.join(BASE_DIR, "audit.log")

# Ensure required local directories exist
os.makedirs(TEMP_DIR, exist_ok=True)

# Security: Session Secret Key & Maximum Content Length
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "cloudlab-secret-key-fallback-change-in-prod"
)
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB limit enforced

# HDFS Cluster Target Directory
HDFS_BASE_DIR = os.environ.get("HDFS_BASE_DIR", "/cloudlab/group1")

# Lab Authentication Credentials (configured via environment variables with lab fallbacks)
LAB_ADMIN_USER = os.environ.get("LAB_ADMIN_USER", "cloudadmin")
LAB_PASSWORD = os.environ.get("LAB_PASSWORD", "cloudlab2026")


# ---------------------------------------------------------------------------
# Helper Utilities
# ---------------------------------------------------------------------------

def get_lan_ip() -> str:
    """Detect the host's primary local LAN IP address."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"


def format_file_size(bytes_val: int) -> str:
    """Convert raw byte counts into human-readable size units."""
    if not isinstance(bytes_val, (int, float)) or bytes_val < 0:
        return "0 B"
    size = float(bytes_val)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0 or unit == "TB":
            return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
        size /= 1024.0
    return f"{size:.1f} PB"


def validate_filename(raw_filename: str) -> tuple:
    """
    Validate and sanitize filename input.
    Explicitly rejects empty filenames, path traversal characters ('/', '\\', '..').
    """
    if not raw_filename or not raw_filename.strip():
        return False, "", "Filename cannot be empty."

    if "/" in raw_filename or "\\" in raw_filename:
        return False, "", "Path traversal violation: Forward slash ('/') and backslash ('\\') are forbidden."

    if ".." in raw_filename:
        return False, "", "Path traversal violation: Parent directory references ('..') are forbidden."

    cleaned_name = secure_filename(raw_filename.strip())
    if not cleaned_name:
        return False, "", "Filename contains only invalid or illegal characters."

    return True, cleaned_name, ""


def audit_log(action: str, filename: str = "-", status: str = "SUCCESS", details: str = ""):
    """
    Record an event in the local audit.log file.
    Logs: timestamp, username, client IP (request.remote_addr), action, filename, status, details.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    username = session.get("username", "anonymous") if has_request_context() else "system"
    client_ip = request.remote_addr if has_request_context() and request.remote_addr else "127.0.0.1"

    action_clean = action.upper().strip()
    file_clean = filename.strip() if filename else "-"
    details_clean = details.replace("\n", " ").strip() if details else "-"

    log_entry = (
        f"[{timestamp}] IP={client_ip} | USER={username} | ACTION={action_clean} "
        f"| FILE={file_clean} | STATUS={status} | DETAILS={details_clean}\n"
    )

    try:
        with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(log_entry)
    except Exception as err:
        app.logger.error(f"Failed to append to audit.log: {err}")


def get_recent_audit_logs(limit: int = 25) -> list:
    """
    Read and parse recent entries from audit.log in reverse chronological order.
    """
    if not os.path.exists(AUDIT_LOG_FILE):
        return []

    entries = []
    log_pattern = re.compile(
        r"^\[(.*?)\]\s+IP=(.*?)\s+\|\s+USER=(.*?)\s+\|\s+ACTION=(.*?)\s+\|\s+FILE=(.*?)\s+\|\s+STATUS=(.*?)\s+\|\s+DETAILS=(.*)$"
    )

    try:
        with open(AUDIT_LOG_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()

        for line in reversed(lines[-limit:]):
            line = line.strip()
            if not line:
                continue

            match = log_pattern.match(line)
            if match:
                entries.append({
                    "timestamp": match.group(1),
                    "ip": match.group(2),
                    "user": match.group(3),
                    "action": match.group(4),
                    "file": match.group(5),
                    "status": match.group(6),
                    "details": match.group(7),
                    "raw": line
                })
            else:
                entries.append({
                    "timestamp": "-",
                    "ip": "-",
                    "user": "-",
                    "action": "SYSTEM",
                    "file": "-",
                    "status": "INFO",
                    "details": line,
                    "raw": line
                })
    except Exception as err:
        app.logger.error(f"Failed reading audit.log: {err}")

    return entries


def parse_hdfs_ls(output_text: str) -> list:
    """
    Parse the stdout of `hdfs dfs -ls /cloudlab/group1`.
    Handles empty output and 'Found 0 items' gracefully.
    Returns a list of dictionaries with file metadata.
    """
    files = []
    lines = output_text.strip().splitlines()

    for line in lines:
        line = line.strip()
        # Skip header lines like "Found X items" or blank lines
        if not line or line.startswith("Found "):
            continue

        parts = line.split(None, 7)
        if len(parts) < 8:
            continue

        perms, replication, owner, group, size_str, date_str, time_str, full_path = parts
        filename = os.path.basename(full_path.rstrip("/"))
        if not filename:
            continue

        try:
            size_bytes = int(size_str)
        except ValueError:
            size_bytes = 0

        files.append({
            "name": filename,
            "path": full_path,
            "is_dir": perms.startswith("d"),
            "permissions": perms,
            "replication": replication,
            "owner": owner,
            "group": group,
            "size_bytes": size_bytes,
            "size_formatted": format_file_size(size_bytes),
            "timestamp": f"{date_str} {time_str}"
        })

    return files


# ---------------------------------------------------------------------------
# Authentication Decorator
# ---------------------------------------------------------------------------

def login_required(f):
    """Decorator to enforce active session authentication on protected routes."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("logged_in"):
            flash("Authentication required. Please sign in to access Cloud Controller.", "warning")
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return decorated_function


# ---------------------------------------------------------------------------
# Error Handlers
# ---------------------------------------------------------------------------

@app.errorhandler(413)
def handle_large_file(error):
    """Handle files exceeding the 16 MB MAX_CONTENT_LENGTH limit."""
    audit_log(
        action="ERROR",
        filename="<upload_payload>",
        status="FAILED",
        details="Upload rejected: File size exceeds the 16 MB limit (HTTP 413)"
    )
    flash("Upload failed: File exceeds the maximum allowed limit of 16 MB.", "danger")
    return redirect(url_for("dashboard")), 303


# ---------------------------------------------------------------------------
# Application Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Root route redirecting to dashboard or login."""
    if session.get("logged_in"):
        return redirect(url_for("dashboard"))
    return redirect(url_for("login"))


@app.route("/login", methods=["GET", "POST"])
def login():
    """Authenticate lab users and initialize sessions."""
    if session.get("logged_in"):
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        if not username:
            flash("Username cannot be empty.", "danger")
            return render_template("login.html")

        # Validate lab password against configured LAB_PASSWORD
        # Supports LAB_ADMIN_USER or any valid designated lab username
        if password == LAB_PASSWORD:
            session["logged_in"] = True
            session["username"] = username
            audit_log("LOGIN", "-", "SUCCESS", f"User '{username}' logged in successfully")
            flash(f"Welcome, {username}! Authenticated to SaaS Cloud Controller.", "success")
            next_page = request.args.get("next")
            return redirect(next_page if next_page and next_page.startswith("/") else url_for("dashboard"))
        else:
            audit_log("LOGIN", "-", "FAILED", f"Invalid login attempt for username '{username}'")
            flash("Invalid lab password. Please check your credentials.", "danger")

    return render_template("login.html", default_user=LAB_ADMIN_USER)


@app.route("/logout")
def logout():
    """Terminate the user session and log the event."""
    username = session.get("username", "anonymous")
    audit_log("LOGOUT", "-", "SUCCESS", f"User '{username}' logged out")
    session.clear()
    flash("You have been signed out successfully.", "info")
    return redirect(url_for("login"))


@app.route("/dashboard", methods=["GET"])
@login_required
def dashboard():
    """
    Main SaaS Cloud Controller dashboard:
    - Lists files from HDFS directory `/cloudlab/group1`
    - Handles empty directories gracefully
    - Displays recent audit log entries
    """
    files = []
    hdfs_status = "UNKNOWN"
    total_bytes = 0

    try:
        # Run: hdfs dfs -ls /cloudlab/group1
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", HDFS_BASE_DIR],
            check=True,
            capture_output=True,
            text=True
        )
        files = parse_hdfs_ls(result.stdout)
        total_bytes = sum(f["size_bytes"] for f in files if not f["is_dir"])
        hdfs_status = "CONNECTED"

    except FileNotFoundError:
        hdfs_status = "NOT_INSTALLED"
        err_msg = "HDFS command not found on host. Verify Apache Hadoop is installed and 'hdfs' is in PATH."
        audit_log("ERROR", "-", "FAILED", err_msg)
        flash(err_msg, "danger")

    except subprocess.CalledProcessError as e:
        err_output = (e.stderr or e.stdout or "").strip()
        if "No such file or directory" in err_output:
            # The directory does not exist yet; attempt to create it
            try:
                subprocess.run(
                    ["hdfs", "dfs", "-mkdir", "-p", HDFS_BASE_DIR],
                    check=True,
                    capture_output=True,
                    text=True
                )
                files = []
                hdfs_status = "CONNECTED"
                flash(f"HDFS directory '{HDFS_BASE_DIR}' was initialized.", "info")
            except Exception as init_err:
                hdfs_status = "UNAVAILABLE"
                err_msg = f"HDFS directory '{HDFS_BASE_DIR}' missing and creation failed: {init_err}"
                audit_log("ERROR", "-", "FAILED", err_msg)
                flash(err_msg, "warning")
        else:
            hdfs_status = "ERROR"
            err_msg = f"HDFS error during directory listing: {err_output or str(e)}"
            audit_log("ERROR", "-", "FAILED", err_msg)
            flash(err_msg, "danger")

    except Exception as e:
        hdfs_status = "ERROR"
        err_msg = f"Unexpected error communicating with HDFS: {str(e)}"
        audit_log("ERROR", "-", "FAILED", err_msg)
        flash(err_msg, "danger")

    recent_logs = get_recent_audit_logs(limit=25)
    lan_ip = get_lan_ip()

    return render_template(
        "dashboard.html",
        files=files,
        recent_logs=recent_logs,
        hdfs_base_dir=HDFS_BASE_DIR,
        hdfs_status=hdfs_status,
        total_files=len(files),
        total_bytes_formatted=format_file_size(total_bytes),
        lan_ip=lan_ip,
        username=session.get("username", "Operator")
    )


@app.route("/upload", methods=["POST"])
@login_required
def upload_file():
    """
    Handle file uploads:
    - Validate filename and path safety
    - Save temporarily to ./temp_uploads
    - Check if file exists in HDFS (warning on duplicate overwrite)
    - Push to HDFS via `hdfs dfs -put -f`
    - Delete temporary local file
    """
    if "file" not in request.files:
        flash("No file part detected in request.", "danger")
        return redirect(url_for("dashboard"))

    file_obj = request.files["file"]
    raw_name = file_obj.filename

    is_valid, safe_name, err_message = validate_filename(raw_name)
    if not is_valid:
        audit_log("ERROR", raw_name or "<empty>", "FAILED", f"Upload rejected: {err_message}")
        flash(f"Upload rejected: {err_message}", "danger")
        return redirect(url_for("dashboard"))

    local_path = os.path.join(TEMP_DIR, safe_name)
    hdfs_target_path = f"{HDFS_BASE_DIR}/{safe_name}"

    try:
        # Save file to temporary staging folder
        file_obj.save(local_path)
        file_size = os.path.getsize(local_path)

        # Check if file already exists in HDFS
        duplicate_detected = False
        try:
            check_proc = subprocess.run(
                ["hdfs", "dfs", "-test", "-e", hdfs_target_path],
                capture_output=True,
                text=True
            )
            if check_proc.returncode == 0:
                duplicate_detected = True
        except FileNotFoundError:
            err_msg = "HDFS CLI binary ('hdfs') not found in PATH."
            audit_log("ERROR", safe_name, "FAILED", err_msg)
            flash(err_msg, "danger")
            return redirect(url_for("dashboard"))
        except Exception as check_err:
            app.logger.warning(f"Failed checking file existence in HDFS: {check_err}")

        # Push file to HDFS using `hdfs dfs -put -f`
        subprocess.run(
            ["hdfs", "dfs", "-put", "-f", local_path, hdfs_target_path],
            check=True,
            capture_output=True,
            text=True
        )

        if duplicate_detected:
            audit_log(
                "UPLOAD",
                safe_name,
                "SUCCESS",
                f"File overwritten in HDFS ({format_file_size(file_size)})"
            )
            flash(
                f"Warning: File '{safe_name}' already existed in HDFS and was overwritten with the uploaded version.",
                "warning"
            )
        else:
            audit_log(
                "UPLOAD",
                safe_name,
                "SUCCESS",
                f"File uploaded to HDFS ({format_file_size(file_size)})"
            )
            flash(f"File '{safe_name}' uploaded successfully to HDFS storage.", "success")

    except subprocess.CalledProcessError as e:
        err_output = (e.stderr or e.stdout or str(e)).strip()
        audit_log("ERROR", safe_name, "FAILED", f"HDFS put failed: {err_output}")
        flash(f"Failed pushing '{safe_name}' to HDFS: {err_output}", "danger")

    except FileNotFoundError:
        audit_log("ERROR", safe_name, "FAILED", "HDFS binary not found")
        flash("HDFS binary not found. Could not push file to cluster.", "danger")

    except Exception as e:
        audit_log("ERROR", safe_name, "FAILED", f"Unexpected upload failure: {str(e)}")
        flash(f"Unexpected error while processing upload '{safe_name}': {str(e)}", "danger")

    finally:
        # Guarantee cleanup of local temporary file
        if os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception as rm_err:
                app.logger.error(f"Failed to remove temp file {local_path}: {rm_err}")

    return redirect(url_for("dashboard"))


@app.route("/download/<path:filename>", methods=["GET"])
@login_required
def download_file(filename):
    """
    Handle file download:
    - Validate filename safety
    - Fetch from HDFS to local ./temp_uploads via `hdfs dfs -get -f`
    - Stream to client via `send_file(..., as_attachment=True)`
    - Clean up temporary local file after request finishes
    """
    is_valid, safe_name, err_message = validate_filename(filename)
    if not is_valid:
        audit_log("ERROR", filename, "FAILED", f"Download rejected: {err_message}")
        flash(f"Invalid download target: {err_message}", "danger")
        return redirect(url_for("dashboard"))

    hdfs_source_path = f"{HDFS_BASE_DIR}/{safe_name}"
    local_path = os.path.join(TEMP_DIR, safe_name)

    try:
        # Fetch file from HDFS
        subprocess.run(
            ["hdfs", "dfs", "-get", "-f", hdfs_source_path, local_path],
            check=True,
            capture_output=True,
            text=True
        )

        audit_log("DOWNLOAD", safe_name, "SUCCESS", f"File streamed to client")

        @after_this_request
        def remove_temp_file(response):
            try:
                if os.path.exists(local_path):
                    os.remove(local_path)
            except Exception as rm_err:
                app.logger.error(f"Error removing temp download file {local_path}: {rm_err}")
            return response

        return send_file(local_path, as_attachment=True, download_name=safe_name)

    except subprocess.CalledProcessError as e:
        err_output = (e.stderr or e.stdout or str(e)).strip()
        audit_log("ERROR", safe_name, "FAILED", f"HDFS get failed: {err_output}")
        flash(f"Failed to fetch '{safe_name}' from HDFS: {err_output}", "danger")
        if os.path.exists(local_path):
            os.remove(local_path)
        return redirect(url_for("dashboard"))

    except FileNotFoundError:
        audit_log("ERROR", safe_name, "FAILED", "HDFS binary not found")
        flash("HDFS binary not found. Unable to retrieve file.", "danger")
        if os.path.exists(local_path):
            os.remove(local_path)
        return redirect(url_for("dashboard"))

    except Exception as e:
        audit_log("ERROR", safe_name, "FAILED", f"Unexpected download failure: {str(e)}")
        flash(f"Unexpected error retrieving '{safe_name}': {str(e)}", "danger")
        if os.path.exists(local_path):
            os.remove(local_path)
        return redirect(url_for("dashboard"))


@app.route("/delete/<path:filename>", methods=["POST"])
@login_required
def delete_file(filename):
    """
    Handle file deletion:
    - Validate filename safety
    - Delete from HDFS via `hdfs dfs -rm /cloudlab/group1/<filename>`
    """
    is_valid, safe_name, err_message = validate_filename(filename)
    if not is_valid:
        audit_log("ERROR", filename, "FAILED", f"Delete rejected: {err_message}")
        flash(f"Invalid deletion target: {err_message}", "danger")
        return redirect(url_for("dashboard"))

    hdfs_target_path = f"{HDFS_BASE_DIR}/{safe_name}"

    try:
        # Delete file from HDFS
        subprocess.run(
            ["hdfs", "dfs", "-rm", hdfs_target_path],
            check=True,
            capture_output=True,
            text=True
        )
        audit_log("DELETE", safe_name, "SUCCESS", f"Removed from HDFS storage")
        flash(f"File '{safe_name}' was successfully removed from HDFS storage.", "success")

    except subprocess.CalledProcessError as e:
        err_output = (e.stderr or e.stdout or str(e)).strip()
        audit_log("ERROR", safe_name, "FAILED", f"HDFS rm failed: {err_output}")
        flash(f"Failed to delete '{safe_name}' from HDFS: {err_output}", "danger")

    except FileNotFoundError:
        audit_log("ERROR", safe_name, "FAILED", "HDFS binary not found")
        flash("HDFS binary not found. Unable to delete file.", "danger")

    except Exception as e:
        audit_log("ERROR", safe_name, "FAILED", f"Unexpected delete failure: {str(e)}")
        flash(f"Unexpected error deleting '{safe_name}': {str(e)}", "danger")

    return redirect(url_for("dashboard"))


# ---------------------------------------------------------------------------
# Server Entry Point (Local LAN Binding)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    host_ip = os.environ.get("FLASK_HOST", "0.0.0.0")
    port_num = int(os.environ.get("FLASK_PORT", 5000))
    lan_ip = get_lan_ip()

    print("=" * 70)
    print("  SaaS Cloud Controller - Hadoop HDFS Gateway")
    print("=" * 70)
    print(f"  * LAN Binding IP    : {host_ip}")
    print(f"  * LAN Interface IP  : {lan_ip}")
    print(f"  * Port              : {port_num}")
    print(f"  * HDFS Storage Path : {HDFS_BASE_DIR}")
    print(f"  * Max File Size     : 16 MB")
    print(f"  * Access URL (LAN)  : http://{lan_ip}:{port_num}")
    print("=" * 70)

    # Bound to 0.0.0.0 so reachable across the laboratory LAN
    app.run(host="0.0.0.0", port=5000, debug=False)
