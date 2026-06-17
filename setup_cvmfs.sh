#!/usr/bin/env bash
# Install and configure the CernVM-FS (CVMFS) client on AlmaLinux 9.
#
# Repositories configured:
#   cms.cern.ch       — CMSSW releases and CMS software
#   cms-bril.cern.ch  — BRIL/luminosity tools
#   grid.cern.ch      — Grid utilities and CA certificates
#
# Run as root or a user with sudo privileges:
#   sudo bash setup_cvmfs.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo -e "\033[1;34m[INFO]\033[0m  $*"; }
ok()    { echo -e "\033[1;32m[ OK ]\033[0m  $*"; }
die()   { echo -e "\033[1;31m[ERR ]\033[0m  $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 0. Pre-flight
# ---------------------------------------------------------------------------
[[ $EUID -eq 0 ]] || die "Please run as root:  sudo bash $0"

info "Detected OS: $(. /etc/os-release && echo "$PRETTY_NAME")"

# ---------------------------------------------------------------------------
# 1. Install CVMFS package
# ---------------------------------------------------------------------------
if rpm -q cvmfs &>/dev/null; then
    ok "cvmfs already installed ($(rpm -q cvmfs --queryformat '%{VERSION}'))"
else
    info "Adding CVMFS yum repository..."
    dnf install -y \
        https://cvmrepo.s3.cern.ch/cvmrepo/yum/cvmfs-release-latest.noarch.rpm

    info "Installing cvmfs..."
    dnf install -y cvmfs
    ok "cvmfs installed: $(rpm -q cvmfs --queryformat '%{VERSION}')"
fi

# ---------------------------------------------------------------------------
# 2. Configure autofs
# ---------------------------------------------------------------------------
info "Configuring autofs for CVMFS..."
cvmfs_config setup
ok "autofs configured"

# ---------------------------------------------------------------------------
# 3. Write /etc/cvmfs/default.local
# ---------------------------------------------------------------------------
CVMFS_LOCAL=/etc/cvmfs/default.local

if [[ -f "$CVMFS_LOCAL" ]]; then
    info "Backing up existing $CVMFS_LOCAL -> ${CVMFS_LOCAL}.bak"
    cp "$CVMFS_LOCAL" "${CVMFS_LOCAL}.bak"
fi

info "Writing $CVMFS_LOCAL..."
cat > "$CVMFS_LOCAL" <<'EOF'
# CMS NanoAOD analysis repositories
CVMFS_REPOSITORIES=cms.cern.ch,cms-bril.cern.ch,grid.cern.ch

# Set to a squid proxy if one is available, otherwise use DIRECT.
# DIRECT is fine for a single workstation; for a cluster, provide a proxy.
CVMFS_HTTP_PROXY=DIRECT

# Local cache size in MB (default 4096 MB)
CVMFS_QUOTA_LIMIT=10240
EOF
ok "Written $CVMFS_LOCAL"

# ---------------------------------------------------------------------------
# 4. Enable and restart autofs
# ---------------------------------------------------------------------------
info "Enabling and restarting autofs..."
systemctl enable --now autofs
systemctl restart autofs
ok "autofs running"

# ---------------------------------------------------------------------------
# 5. Probe repositories
# ---------------------------------------------------------------------------
info "Probing CVMFS repositories (this may take a minute on first access)..."
if cvmfs_config probe; then
    ok "All repositories mounted successfully!"
else
    echo ""
    echo "Probe reported issues. Try:"
    echo "  cvmfs_config chksetup   # check configuration"
    echo "  cvmfs_config showconfig cms.cern.ch"
    echo "  sudo systemctl status autofs"
fi

# ---------------------------------------------------------------------------
# 6. Quick sanity check
# ---------------------------------------------------------------------------
echo ""
info "Sanity check — listing /cvmfs:"
ls /cvmfs/ 2>/dev/null || echo "(nothing listed yet — access a repo to trigger automount)"

echo ""
info "Test access with:"
echo "  ls /cvmfs/cms.cern.ch"
echo "  ls /cvmfs/grid.cern.ch"
echo "  ls /cvmfs/cms-bril.cern.ch"
echo ""
ok "CVMFS setup complete."
