#!/bin/bash

# Setup script for Claude review automation
# This script configures git hooks and GitHub Actions for automatic Claude reviews

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Setting up Claude Review Automation${NC}"
echo ""

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check dependencies
echo "Checking dependencies..."
missing_deps=()

if ! command_exists gh; then
    missing_deps+=("GitHub CLI (gh)")
fi

if ! command_exists claude; then
    missing_deps+=("Claude Code")
fi

if ! command_exists jq; then
    missing_deps+=("jq")
fi

if [ ${#missing_deps[@]} -ne 0 ]; then
    echo -e "${YELLOW}⚠️  Missing dependencies:${NC}"
    printf '  - %s\n' "${missing_deps[@]}"
    echo ""
    echo "Please install missing dependencies first."
    exit 1
fi

echo -e "${GREEN}✅ All dependencies found${NC}"
echo ""

# 1. Setup Git Hooks
echo "1. Setting up Git Hooks..."

# Make sure git hooks are executable
chmod +x .githooks/post-push
chmod +x claude-review.sh

# Configure git to use our hooks directory
git config core.hooksPath .githooks

echo -e "${GREEN}✅ Git hooks configured${NC}"
echo ""

# 2. Check GitHub Actions workflow
echo "2. Checking GitHub Actions workflow..."

if [ -f ".github/workflows/claude-review.yml" ]; then
    echo -e "${GREEN}✅ GitHub Actions workflow already exists${NC}"
else
    echo -e "${YELLOW}⚠️  GitHub Actions workflow not found${NC}"
    echo "Please add the claude-review.yml workflow to .github/workflows/"
fi
echo ""

# 3. Setup GitHub secrets
echo "3. Checking GitHub repository secrets..."

# Check if we can access repo secrets (this will fail if not admin)
if gh secret list >/dev/null 2>&1; then
    # Check for ANTHROPIC_API_KEY
    if gh secret list | grep -q "ANTHROPIC_API_KEY"; then
        echo -e "${GREEN}✅ ANTHROPIC_API_KEY secret found${NC}"
    else
        echo -e "${YELLOW}⚠️  ANTHROPIC_API_KEY secret not found${NC}"
        echo "Please add your Anthropic API key as a repository secret:"
        echo "  gh secret set ANTHROPIC_API_KEY"
    fi
else
    echo -e "${YELLOW}⚠️  Cannot check repository secrets (need admin access)${NC}"
    echo "Please ensure ANTHROPIC_API_KEY is set as a repository secret"
fi
echo ""

# 4. Test the setup
echo "4. Testing the setup..."

# Test claude-review.sh script
if [ -f "./claude-review.sh" ]; then
    echo "Testing claude-review.sh script..."
    if ./claude-review.sh --dry-run >/dev/null 2>&1; then
        echo -e "${GREEN}✅ claude-review.sh script working${NC}"
    else
        echo -e "${YELLOW}⚠️  claude-review.sh script may have issues${NC}"
        echo "Try running: ./claude-review.sh --help"
    fi
else
    echo -e "${YELLOW}⚠️  claude-review.sh script not found${NC}"
fi
echo ""

# 5. Display usage instructions
echo -e "${BLUE}📋 Usage Instructions:${NC}"
echo ""
echo "Local Development:"
echo "  make review                # Review current PR"
echo "  make review-security       # Security-focused review" 
echo "  make review-performance    # Performance-focused review"
echo "  make review-and-push       # Full CI + review + push"
echo ""
echo "Git Hooks (Automatic):"
echo "  - Post-push hook will automatically run security review"
echo "  - Only runs on feature/fix/hotfix branches with existing PRs"
echo ""
echo "GitHub Actions (Automatic):"
echo "  - Runs on every PR creation/update"
echo "  - Posts review as PR comment"
echo "  - Requires ANTHROPIC_API_KEY secret"
echo ""
echo "Manual Usage:"
echo "  ./claude-review.sh                    # Review current PR"
echo "  ./claude-review.sh 58                # Review specific PR"
echo "  ./claude-review.sh --focus security  # Security-focused review"
echo "  ./claude-review.sh --save-file       # Save to file instead of comment"
echo ""

echo -e "${GREEN}🎉 Claude Review Automation setup complete!${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Ensure ANTHROPIC_API_KEY is set as a GitHub repository secret"
echo "2. Test with: make review --dry-run"
echo "3. Create a PR and push to see automation in action"
echo ""
echo "For help: ./claude-review.sh --help"