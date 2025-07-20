# Backlog Grooming Instructions for Claude

## Overview
Backlog grooming is the process of reviewing, prioritizing, and maintaining the GitHub issues to ensure the project backlog remains healthy and actionable. This should be performed regularly to keep the development process smooth and focused.

## When to Perform Grooming
- **Regular Schedule**: Weekly or bi-weekly review of all open issues
- **Before Sprint Planning**: Ensure issues are properly prioritized and estimated
- **After Major Releases**: Re-evaluate priorities and close completed items
- **When Requested**: User explicitly asks for backlog review

## Grooming Process

### 1. Issue Audit and Cleanup

#### A. Review All Open Issues
```bash
# Use GitHub CLI to list all open issues
gh issue list --state open --limit 100
```

#### B. Identify Issues to Close
**Close issues that are:**
- ✅ Already implemented (check against recent commits/PRs)
- 🚫 No longer relevant due to architecture changes
- 🔄 Duplicates of other issues
- 🗑️ Outdated requirements or obsolete features
- ❌ Invalid bug reports or feature requests

#### C. Update Issue Status
**For each issue, check:**
- Is the issue still valid?
- Has it been partially or fully implemented?
- Are there newer related issues that supersede this one?
- Does the issue description need updates?

### 2. Priority Assessment

#### A. Priority Levels
- **Critical**: Security vulnerabilities, system down, data loss
- **High**: Blocking functionality, performance issues affecting users
- **Medium**: Important features, moderate bugs with workarounds
- **Low**: Nice-to-have features, minor improvements, cosmetic issues

#### B. Factors to Consider
1. **Business Impact**: How many users are affected?
2. **Technical Debt**: Does this improve code quality or maintainability?
3. **Dependencies**: Does this block other important work?
4. **Effort vs Value**: Is the return on investment reasonable?
5. **User Feedback**: Are users actively requesting this?

### 3. Label Management

#### A. Required Labels for All Issues
- **Type**: `bug`, `feature`, `enhancement`, `documentation`, `technical-debt`
- **Priority**: `priority/critical`, `priority/high`, `priority/medium`, `priority/low`
- **Service**: `analytics-api`, `data-ingestion`, `ml-inference`, etc.
- **Status**: `needs-triage`, `ready-for-development`, `in-progress`, `blocked`

#### B. Additional Labels
- **Effort**: `effort/small`, `effort/medium`, `effort/large`, `effort/xl`
- **Skill Level**: `good-first-issue`, `help-wanted`, `senior-dev-needed`
- **Category**: `performance`, `security`, `ui/ux`, `api`, `testing`

### 4. Issue Description Updates

#### A. Ensure Each Issue Has:
- Clear, actionable title
- Detailed description of the problem/requirement
- Acceptance criteria or definition of done
- Technical context or implementation hints
- Links to related issues or documentation

#### B. Update Format for Consistency
```markdown
## Problem Statement
[Clear description of what needs to be solved]

## Proposed Solution
[How to address the problem]

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2
- [ ] Criterion 3

## Technical Notes
[Implementation details, dependencies, risks]
```

### 5. Backlog Organization

#### A. Milestone Assignment
- Assign issues to appropriate milestones (v1.0, v1.1, etc.)
- Move items between milestones based on priority changes
- Ensure milestones have realistic scope

#### B. Epic Management
- Group related issues under epics or projects
- Ensure epic scope is manageable
- Break down large epics into smaller issues

### 6. Stakeholder Alignment

#### A. Review with Team
- Discuss priority changes with stakeholders
- Validate technical estimates with developers
- Ensure business priorities are reflected

#### B. Document Decisions
- Comment on issues explaining priority changes
- Reference business justification for decisions
- Tag stakeholders for visibility

## Grooming Commands for Claude

### Issue Review Commands
```bash
# List issues by priority
gh issue list --label "priority/high" --state open
gh issue list --label "needs-triage" --state open

# List issues by service
gh issue list --label "analytics-api" --state open

# Search for specific types
gh issue list --search "is:open label:bug"
gh issue list --search "is:open label:feature"
```

### Issue Management Commands
```bash
# Update issue labels
gh issue edit [issue-number] --add-label "priority/high"
gh issue edit [issue-number] --remove-label "needs-triage"

# Close completed issues
gh issue close [issue-number] --comment "Completed in #[PR-number]"

# Add milestones
gh issue edit [issue-number] --milestone "v1.1"
```

## Grooming Checklist

### Weekly Review
- [ ] Review all issues labeled `needs-triage`
- [ ] Check for duplicate issues
- [ ] Verify recently closed PRs haven't resolved open issues
- [ ] Update priority labels based on current business needs
- [ ] Ensure all high-priority issues have clear acceptance criteria
- [ ] Check that `good-first-issue` items are still appropriate

### Monthly Deep Dive
- [ ] Review all open issues for relevance
- [ ] Close obsolete or completed issues
- [ ] Re-prioritize entire backlog
- [ ] Update issue descriptions for clarity
- [ ] Reorganize milestones and epics
- [ ] Generate backlog health report

### Quality Gates
- [ ] All issues have appropriate labels
- [ ] High-priority issues have detailed descriptions
- [ ] No duplicate issues exist
- [ ] Stale issues (>90 days) are reviewed
- [ ] Critical issues have owner assignments
- [ ] Feature requests have business justification

## Report Template

After grooming, provide a summary:

```markdown
## Backlog Grooming Summary - [Date]

### Actions Taken
- Closed X obsolete issues
- Updated priority for Y issues
- Added Z new labels for better organization

### Current Backlog Health
- Total Open Issues: X
- Critical: X | High: X | Medium: X | Low: X
- Needs Triage: X

### Key Recommendations
1. [Priority recommendation]
2. [Resource allocation suggestion]
3. [Process improvement]

### Next Review Date
[Suggested date for next grooming session]
```

## Best Practices

1. **Be Ruthless**: Close issues that don't add value
2. **Stay Current**: Regular grooming prevents backlog bloat
3. **Communicate**: Explain decisions in issue comments
4. **Prioritize Ruthlessly**: Not everything can be high priority
5. **Think Users First**: Prioritize based on user impact
6. **Consider Technical Debt**: Balance features with maintainability
7. **Document Decisions**: Leave audit trail for future reference

This process ensures the backlog remains a valuable tool for project planning and development prioritization.