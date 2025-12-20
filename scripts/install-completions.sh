#!/bin/bash
# Install shell completions for sql-splitter
# This script detects the user's shell and installs completions to the appropriate location

set -e

BINARY="${1:-sql-splitter}"

# Detect if binary is available
if ! command -v "$BINARY" &> /dev/null; then
    echo "Warning: $BINARY not found in PATH. Skipping completions installation."
    echo "Run this script again after the binary is in your PATH."
    exit 0
fi

install_bash_completions() {
    local completion_dir
    
    # Try system-wide location first (requires sudo)
    if [[ -d "/usr/local/share/bash-completion/completions" ]] && [[ -w "/usr/local/share/bash-completion/completions" ]]; then
        completion_dir="/usr/local/share/bash-completion/completions"
    elif [[ -d "/etc/bash_completion.d" ]] && [[ -w "/etc/bash_completion.d" ]]; then
        completion_dir="/etc/bash_completion.d"
    else
        # Use user-local directory
        completion_dir="${XDG_DATA_HOME:-$HOME/.local/share}/bash-completion/completions"
        mkdir -p "$completion_dir"
    fi
    
    "$BINARY" completions bash > "$completion_dir/sql-splitter"
    echo "Bash completions installed to: $completion_dir/sql-splitter"
}

install_zsh_completions() {
    local completion_dir
    
    # Try system-wide location first
    if [[ -d "/usr/local/share/zsh/site-functions" ]] && [[ -w "/usr/local/share/zsh/site-functions" ]]; then
        completion_dir="/usr/local/share/zsh/site-functions"
    else
        # Use user-local directory
        completion_dir="${ZDOTDIR:-$HOME}/.zsh/completions"
        mkdir -p "$completion_dir"
        
        # Ensure the directory is in fpath (add to .zshrc if not already)
        if ! grep -q 'fpath=.*\.zsh/completions' "${ZDOTDIR:-$HOME}/.zshrc" 2>/dev/null; then
            echo "" >> "${ZDOTDIR:-$HOME}/.zshrc"
            echo "# sql-splitter completions" >> "${ZDOTDIR:-$HOME}/.zshrc"
            echo 'fpath=(~/.zsh/completions $fpath)' >> "${ZDOTDIR:-$HOME}/.zshrc"
            echo "Added ~/.zsh/completions to fpath in .zshrc"
        fi
    fi
    
    "$BINARY" completions zsh > "$completion_dir/_sql-splitter"
    echo "Zsh completions installed to: $completion_dir/_sql-splitter"
}

install_fish_completions() {
    local completion_dir="${XDG_CONFIG_HOME:-$HOME/.config}/fish/completions"
    mkdir -p "$completion_dir"
    
    "$BINARY" completions fish > "$completion_dir/sql-splitter.fish"
    echo "Fish completions installed to: $completion_dir/sql-splitter.fish"
}

# Detect current shell
detect_shell() {
    # Check SHELL environment variable
    case "$SHELL" in
        */bash)  echo "bash" ;;
        */zsh)   echo "zsh" ;;
        */fish)  echo "fish" ;;
        *)       echo "unknown" ;;
    esac
}

main() {
    local shell="${2:-$(detect_shell)}"
    
    echo "Installing sql-splitter shell completions..."
    echo ""
    
    case "$shell" in
        bash)
            install_bash_completions
            ;;
        zsh)
            install_zsh_completions
            ;;
        fish)
            install_fish_completions
            ;;
        all)
            install_bash_completions
            install_zsh_completions
            install_fish_completions
            ;;
        *)
            echo "Unknown shell: $shell"
            echo "Supported shells: bash, zsh, fish, all"
            echo ""
            echo "Usage: $0 [binary] [shell]"
            echo "  binary: Path to sql-splitter binary (default: sql-splitter)"
            echo "  shell:  Shell to install completions for (default: auto-detect)"
            exit 1
            ;;
    esac
    
    echo ""
    echo "Done! Restart your shell or source your shell config to enable completions."
}

main "$@"
