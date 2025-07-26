#!/bin/bash

# Claude Code Local Review Script
# Saves PR reviews as markdown files

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Get current branch to return to later
ORIGINAL_BRANCH=$(git branch --show-current)

# Function to display usage
usage() {
    echo "Usage: $0 [PR_NUMBER]"
    echo "  PR_NUMBER: Optional PR number to review (defaults to current PR)"
    echo ""
    echo "Examples:"
    echo "  $0         # Review current PR"
    echo "  $0 54      # Review PR #54"
    exit 1
}

# Check if required tools are available
if ! command -v claude &> /dev/null; then
    echo -e "${RED}Error: Claude Code is not installed or not in PATH${NC}"
    echo "Please ensure Claude Code is installed: https://docs.anthropic.com/en/docs/claude-code"
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: jq is not installed${NC}"
    echo "Please install jq: https://stedolan.github.io/jq/"
    exit 1
fi

# Get PR number
if [ $# -eq 0 ]; then
    # Try to get current PR number
    PR_NUM=$(gh pr view --json number -q .number 2>/dev/null || echo "")
    if [ -z "$PR_NUM" ]; then
        echo -e "${RED}Error: Not currently on a PR branch${NC}"
        echo "Please specify a PR number or checkout a PR branch"
        usage
    fi
elif [ $# -eq 1 ]; then
    PR_NUM=$1
else
    usage
fi

# Validate PR exists
if ! gh pr view "$PR_NUM" > /dev/null 2>&1; then
    echo -e "${RED}Error: PR #$PR_NUM not found${NC}"
    exit 1
fi

# Get PR info
PR_INFO=$(gh pr view "$PR_NUM" --json title,author,baseRefName,headRefName)
PR_TITLE=$(echo "$PR_INFO" | jq -r .title)
PR_AUTHOR=$(echo "$PR_INFO" | jq -r .author.login)
PR_BRANCH=$(echo "$PR_INFO" | jq -r .headRefName)

echo -e "${GREEN}Reviewing PR #$PR_NUM: $PR_TITLE${NC}"
echo -e "Author: $PR_AUTHOR"
echo -e "Branch: $PR_BRANCH"
echo ""

# Checkout PR if not already on it
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "$PR_BRANCH" ]; then
    # Check if working tree is clean before switching branches
    if [ -n "$(git status --porcelain)" ]; then
        echo -e "${RED}Error: Working tree not clean${NC}"
        echo "Please commit or stash your changes before switching branches"
        exit 1
    fi
    echo -e "${YELLOW}Checking out PR branch...${NC}"
    gh pr checkout "$PR_NUM"
fi

# Create output filename
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
OUTPUT_DIR="reviews/manual"
OUTPUT_FILE="$OUTPUT_DIR/pr-${PR_NUM}-${TIMESTAMP}.md"

# Ensure output directory exists
mkdir -p "$OUTPUT_DIR"

# Create header for the review file
cat > "$OUTPUT_FILE" << EOF
# Code Review: PR #$PR_NUM

**Title:** $PR_TITLE  
**Author:** $PR_AUTHOR  
**Date:** $(date +"%Y-%m-%d %H:%M:%S")  
**Branch:** $PR_BRANCH  

---

## Claude Review Output

EOF

# Run Claude review and capture output
echo -e "${YELLOW}Running Claude review...${NC}"
echo ""

# Run claude review and append to file
if claude review >> "$OUTPUT_FILE" 2>&1; then
    echo -e "${GREEN}✓ Review completed successfully${NC}"
    echo -e "${GREEN}✓ Saved to: $OUTPUT_FILE${NC}"
    
    # Show summary
    echo ""
    echo "Review Summary:"
    echo "---------------"
    # Extract first few lines of review
    tail -n +10 "$OUTPUT_FILE" | head -n 20
    echo "..."
    echo ""
    echo -e "${YELLOW}Full review saved to: $OUTPUT_FILE${NC}"
else
    echo -e "${RED}✗ Review failed${NC}"
    echo "Check $OUTPUT_FILE for error details"
fi

# Return to original branch if we switched
if [ "$CURRENT_BRANCH" != "$PR_BRANCH" ] && [ -n "$ORIGINAL_BRANCH" ]; then
    echo ""
    echo -e "${YELLOW}Returning to branch: $ORIGINAL_BRANCH${NC}"
    git checkout "$ORIGINAL_BRANCH"
fi

# Add reminder about issue creation
echo ""
echo -e "${GREEN}Tip: To create issues from this review:${NC}"
echo "1. Open $OUTPUT_FILE"
echo "2. Extract concerns/issues"
echo "3. Create GitHub issues with: gh issue create --title \"...\" --body \"...\""