# SaaS Cloud Controller with Hadoop HDFS Backend

A production-grade Python Flask web application that operates as an on-premise **SaaS Cloud Controller** over a local laboratory Local Area Network (LAN). The controller provides an intuitive, web-based management plane over a distributed **Apache Hadoop HDFS** cluster storage backend targeting `/cloudlab/group1`.

---

## 1. System Architecture

```
 +-----------------------------------------------------------------------------------+
 |                             LOCAL LABORATORY LAN (192.168.1.0/24)                 |
 +-----------------------------------------------------------------------------------+
       |                                       |                               |
       v                                       v                               v
 [Lab Workstation A]                 [Lab Workstation B]              [Lab Workstation C]
 (Browser: 192.168.1.101)            (Browser: 192.168.1.102)         (Browser: 192.168.1.103)
       |                                       |                               |
       +-----------------------+---------------+-------------------------------+
                               | HTTP GET/POST (Port 5000)
                               v
+-------------------------------------------------------------------------------------+
|                      CLOUD CONTROLLER HOST (0.0.0.0:5000)                           |
|                                                                                     |
|   +-----------------------------------------------------------------------------+   |
|   |                        Flask Application Engine (app.py)                    |   |
|   |                                                                             |   |
|   |   [Security & Validation Gateways]                                          |   |
|   |   * Session Authentication (@login_required)                                |   |
|   |   * Maximum Upload Limit Enforced: 16 MB (MAX_CONTENT_LENGTH)               |   |
|   |   * Path Traversal Defense (secure_filename, rejects '/', '\\', '..')       |   |
|   |                                                                             |   |
|   |   [Core API Routes]                                                         |   |
|   |   * /login & /logout           -> Session lifecycle management              |   |
|   |   * /dashboard                 -> Reads HDFS ls & audit.log                 |   |
|   |   * /upload                    -> Validates, checks duplicates, hdfs put    |   |
|   |   * /download/<filename>       -> Streams file from HDFS, cleans temp       |   |
|   |   * /delete/<filename>         -> Deletes from HDFS via hdfs rm             |   |
|   +-----------------------------------------------------------------------------+   |
|             |                                                  |                    |
|             v                                                  v                    |
|   +----------------------+                           +--------------------+         |
|   |   ./temp_uploads/    |                           |     audit.log      |         |
|   |   (Transient Staging |                           | (LOGIN, UPLOAD,    |         |
|   |    & Auto-cleaned)   |                           |  DOWNLOAD, DELETE, |         |
|   +----------------------+                           |  ERROR trails)     |         |
|             |                                        +--------------------+         |
|             | Subprocess CLI Bridge                                                 |
|             | subprocess.run(["hdfs", "dfs", ...], check=True, ...)                 |
+-------------|-----------------------------------------------------------------------+
              |
              v RPC Port 9000 / 8020
+-------------------------------------------------------------------------------------+
|                        HADOOP HDFS STORAGE CLUSTER                                  |
|                                                                                     |
|         +-------------------------------------------------------------+             |
|         |                   NameNode (Metadata Master)                |             |
|         |                   Target: /cloudlab/group1                  |             |
|         +-------------------------------------------------------------+             |
|                          |                                   |                      |
|                 Block Replication                   Block Replication               |
|                          v                                   v                      |
|         +---------------------------------+ +-------------------------------+       |
|         |    DataNode 1 (Block Storage)   | |  DataNode 2 (Block Storage)   |       |
|         +---------------------------------+ +-------------------------------+       |
+-------------------------------------------------------------------------------------+
```

---

## 2. Key Features

- **HDFS Subprocess Integration**: Employs `subprocess.run(["hdfs", "dfs", ...], check=True, capture_output=True, text=True)` without requiring fragile third-party WebHDFS or RPC native libraries.
- **Strict Input Validation & Security**:
  - Session-based authentication with parameterized credentials via environment variables.
  - Strict 16 MB payload constraint (`app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024`).
  - Strict directory traversal protection: rejects paths with `/`, `\`, or `..` and sanitizes names with `secure_filename`.
- **Fault-Tolerant Error Handling**: Catches `subprocess.CalledProcessError` and `FileNotFoundError` across all operations and surfaces clean Bootstrap dismissible flash alerts instead of crashing.
- **Enterprise Audit Logging**: Captures every `LOGIN`, `UPLOAD`, `DOWNLOAD`, `DELETE`, and `ERROR` event into `audit.log` with timestamp, client IP (`request.remote_addr`), authenticated username, target file, and operational details.
- **Modern Responsive UI**: Built with Bootstrap 5 and Bootstrap Icons, featuring storage metrics, live audit trail viewer, file type recognition icons, and JavaScript confirmation dialogs (`confirm('Are you sure you want to delete this file?')`).
- **LAN-Ready**: Configured to bind on `0.0.0.0:5000` for accessibility across the lab network.

---

## 3. Project Structure

```
saas-cloud-controller/
├── app.py                  # Core Flask Cloud Controller application
├── requirements.txt        # Python package dependencies (Flask, Werkzeug)
├── audit.log               # Local persistent audit event log (auto-created)
├── temp_uploads/           # Local transient spooling directory (auto-created)
├── templates/
│   ├── login.html          # Clean Bootstrap 5 authentication interface
│   └── dashboard.html      # Controller dashboard, HDFS explorer & audit viewer
└── README.md               # Technical manual and deployment documentation
```

---

## 4. Prerequisites

1. **Operating System**: Linux (Ubuntu 20.04/22.04 LTS or CentOS/RHEL 7/8).
2. **Python**: Python 3.8+ with `pip`.
3. **Apache Hadoop**: Hadoop 3.x cluster installed and active with the `hdfs` CLI binary available in the system `PATH`.
4. **Network**: Machines connected on the same local laboratory subnet.

---

## 5. Hadoop HDFS Setup Commands

Execute the following commands on the Hadoop master/NameNode host:

### Step 1: Format NameNode (Only for initial cluster setup)
```bash
hdfs namenode -format
```

### Step 2: Start HDFS Daemons
```bash
start-dfs.sh
# Alternatively using systemd:
# sudo systemctl start hadoop-hdfs-namenode hadoop-hdfs-datanode
```

### Step 3: Verify HDFS Cluster Health
```bash
hdfs dfsadmin -report
```

### Step 4: Create Target Storage Directory
Create the dedicated laboratory storage directory `/cloudlab/group1`:
```bash
hdfs dfs -mkdir -p /cloudlab/group1
```

### Step 5: Configure Permissions
Assign read/write permissions so lab users can manage blocks:
```bash
hdfs dfs -chmod -R 775 /cloudlab/group1
```

### Step 6: Verify HDFS Path Listing
```bash
hdfs dfs -ls /cloudlab
```

---

## 6. Installation & Configuration

### Step 1: Clone or Navigate to Project Directory
```bash
cd /home/student/.gemini/antigravity/scratch/saas-cloud-controller
```

### Step 2: Create and Activate Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install Required Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Environment Variables (Optional Configuration)
Configure environment settings or rely on the defaults:

| Variable | Default Value | Description |
|---|---|---|
| `LAB_ADMIN_USER` | `cloudadmin` | Default operator username displayed on the login page |
| `LAB_PASSWORD` | `cloudlab2026` | Laboratory access password (change in production) |
| `SECRET_KEY` | `cloudlab-secret-key-fallback...` | Flask session cryptographic secret |
| `HDFS_BASE_DIR` | `/cloudlab/group1` | Target directory inside Hadoop HDFS |
| `FLASK_HOST` | `0.0.0.0` | Network interface to bind (all LAN interfaces) |
| `FLASK_PORT` | `5000` | Network port for the web interface |

To set custom credentials in your shell:
```bash
export LAB_ADMIN_USER="labstudent1"
export LAB_PASSWORD="SecureLabPassword987!"
export SECRET_KEY="$(python3 -c 'import secrets; print(secrets.token_hex(32))')"
export HDFS_BASE_DIR="/cloudlab/group1"
```

---

## 7. Running the Server on Local LAN

### Step 1: Identify Host LAN IP Address
Run one of the following commands on the controller host:
```bash
hostname -I | awk '{print $1}'
# Or using iproute2:
ip -brief address show
```
*(Example output: `192.168.1.50`)*

### Step 2: Allow Port 5000 through Firewall (if UFW / firewalld active)
```bash
sudo ufw allow 5000/tcp
```

### Step 3: Launch Controller
```bash
python3 app.py
```
Output:
```
======================================================================
  SaaS Cloud Controller - Hadoop HDFS Gateway
======================================================================
  * LAN Binding IP    : 0.0.0.0
  * LAN Interface IP  : 192.168.1.50
  * Port              : 5000
  * HDFS Storage Path : /cloudlab/group1
  * Max File Size     : 16 MB
  * Access URL (LAN)  : http://192.168.1.50:5000
======================================================================
```

### Step 4: Access From Lab Workstations
From any laptop, desktop, or mobile device on the laboratory LAN, open a web browser and navigate to:
```
http://192.168.1.50:5000
```

---

## 8. Verification & Testing Checklist

Use this structured test matrix to verify all functional, security, and operational requirements:

### A. Pre-Flight & Network Readiness
- [ ] Controller server starts cleanly without errors on `0.0.0.0:5000`.
- [ ] Client workstation on same LAN can ping controller host IP.
- [ ] Browser loads `http://<CONTROLLER_IP>:5000` and displays the styled login screen.

### B. Authentication & Session Security
- [ ] Attempting to access `http://<CONTROLLER_IP>:5000/dashboard` directly without logging in redirects to `/login` with an alert.
- [ ] Submitting empty credentials displays a warning alert.
- [ ] Submitting an invalid password rejects authentication and logs an `ERROR`/`LOGIN` failure in `audit.log`.
- [ ] Submitting correct password (default: `cloudlab2026`) creates an authenticated session and redirects to `/dashboard`.
- [ ] Clicking "Sign Out" destroys session and redirects to `/login`.

### C. HDFS Directory Listing & Empty Handling
- [ ] Accessing `/dashboard` when `/cloudlab/group1` is empty shows the graceful empty state graphic ("No Files in HDFS Directory").
- [ ] Total metrics cards show `0` objects and `0 B` storage without crashing.
- [ ] HDFS cluster status displays "Online" when Hadoop NameNode is reachable.

### D. File Upload & Duplicate Handling
- [ ] Uploading a normal file (e.g., `sample.txt`, 2 KB):
  - Staged temporarily to `./temp_uploads/sample.txt`.
  - Pushed to HDFS via `hdfs dfs -put -f ./temp_uploads/sample.txt /cloudlab/group1/sample.txt`.
  - Temporary local file deleted automatically.
  - Success alert displayed on dashboard.
  - Entry added to HDFS file table with accurate file size, timestamp, and permissions.
  - `audit.log` records `ACTION=UPLOAD | FILE=sample.txt | STATUS=SUCCESS`.
- [ ] Uploading the same file (`sample.txt`) a second time:
  - System detects duplicate existence via `hdfs dfs -test -e`.
  - Overwrites existing copy using `-put -f`.
  - Displays a warning alert notifying user that duplicate file was overwritten.

### E. File Download & Streaming
- [ ] Clicking the download button next to `sample.txt`:
  - Retrived from HDFS using `hdfs dfs -get -f`.
  - Streamed to browser as an attachment with identical byte size and content.
  - Local copy in `./temp_uploads` is automatically deleted after stream completion.
  - `audit.log` records `ACTION=DOWNLOAD | FILE=sample.txt | STATUS=SUCCESS`.

### F. File Deletion & Client Confirmation
- [ ] Clicking the delete button triggers browser JavaScript prompt: `Are you sure you want to delete this file?`.
- [ ] Clicking "Cancel" prevents request execution.
- [ ] Clicking "OK" submits `POST /delete/sample.txt`:
  - Executes `hdfs dfs -rm /cloudlab/group1/sample.txt`.
  - Removes entry from UI table.
  - Displays green success flash message.
  - `audit.log` records `ACTION=DELETE | FILE=sample.txt | STATUS=SUCCESS`.

### G. Security & Limits Enforcement
- [ ] **16 MB Upload Limit**: Attempting to upload a file > 16 MB (e.g. 20 MB test file) triggers `MAX_CONTENT_LENGTH` rejection with HTTP 413, displaying an error alert and logging the error to `audit.log`.
- [ ] **Path Traversal Rejection**:
  - Submitting filename with `/` (e.g. `../../etc/passwd` or `foo/bar.txt`) is immediately rejected with flash warning.
  - Submitting filename with `\` (e.g. `..\..\windows\system32`) is immediately rejected.
  - Sanitization ensures filenames are cleansed via `secure_filename`.

### H. Error Recovery & Fault Tolerance
- [ ] Stop Hadoop NameNode (`stop-dfs.sh`) or point `HDFS_BASE_DIR` to a non-existent path.
- [ ] Refresh `/dashboard`: The app catches `subprocess.CalledProcessError` or `FileNotFoundError` and renders the dashboard with a clean alert ("HDFS error during directory listing") rather than raising an unhandled 500 error page.
- [ ] Restart NameNode: Refreshing restores the "Online" cluster state.

### I. Audit Trail Inspection
- [ ] Verify that `audit.log` contains detailed records for each action:
```
[2026-10-16 11:45:10] IP=192.168.1.101 | USER=cloudadmin | ACTION=LOGIN | FILE=- | STATUS=SUCCESS | DETAILS=User 'cloudadmin' logged in successfully
[2026-10-16 11:46:22] IP=192.168.1.101 | USER=cloudadmin | ACTION=UPLOAD | FILE=dataset.csv | STATUS=SUCCESS | DETAILS=File uploaded to HDFS (1.2 MB)
[2026-10-16 11:47:05] IP=192.168.1.102 | USER=cloudadmin | ACTION=DOWNLOAD | FILE=dataset.csv | STATUS=SUCCESS | DETAILS=File streamed to client
[2026-10-16 11:48:19] IP=192.168.1.101 | USER=cloudadmin | ACTION=DELETE | FILE=dataset.csv | STATUS=SUCCESS | DETAILS=Removed from HDFS storage
```
- [ ] Audit trail panel on `/dashboard` updates immediately to reflect recent actions in reverse chronological order.
