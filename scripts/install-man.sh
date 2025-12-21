#!/bin/bash
# Install man pages for sql-splitter
# This script installs man pages to the appropriate system location

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MAN_SOURCE="$PROJECT_DIR/man"

# Check if man pages exist
if [[ ! -d "$MAN_SOURCE" ]] || [[ -z "$(ls -A "$MAN_SOURCE" 2>/dev/null)" ]]; then
    echo "Man pages not found in $MAN_SOURCE"
    echo "Run 'make man' first to generate them."
    exit 0
fi

# Determine installation directory
determine_man_dir() {
    # Try common locations in order of preference
    local dirs=(
        "/usr/local/share/man/man1"
        "/usr/share/man/man1"
        "$HOME/.local/share/man/man1"
    )
    
    for dir in "${dirs[@]}"; do
        if [[ -d "$dir" ]] && [[ -w "$dir" ]]; then
            echo "$dir"
            return 0
        fi
    done
    
    # Fall back to user-local directory
    local user_dir="$HOME/.local/share/man/man1"
    mkdir -p "$user_dir"
    echo "$user_dir"
}

MAN_DIR=$(determine_man_dir)

echo "Installing man pages to: $MAN_DIR"

# Copy man pages
cp "$MAN_SOURCE"/*.1 "$MAN_DIR/"

# Update man database if mandb is available
if command -v mandb &> /dev/null; then
    echo "Updating man database..."
    mandb -q 2>/dev/null || true
fi

echo "Man pages installed successfully."
echo ""
echo "View with: man sql-splitter"
echo "Or: man sql-splitter-diff, man sql-splitter-sample, etc."

# Check if user-local man dir is in MANPATH
if [[ "$MAN_DIR" == "$HOME/.local/share/man/man1" ]]; then
    if ! echo "$MANPATH" | grep -q "$HOME/.local/share/man"; then
        echo ""
        echo "Note: Add to your shell profile for user-local man pages:"
        echo "  export MANPATH=\"\$HOME/.local/share/man:\$MANPATH\""
    fi
fi
