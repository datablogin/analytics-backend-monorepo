# CI Timeout Configuration

This document explains the timeout configurations implemented to prevent runaway CI jobs from consuming excessive GitHub Actions minutes.

## Timeout Levels

### 1. **Job-Level Timeouts** (Primary Protection)
```yaml
jobs:
  test:
    timeout-minutes: 15  # Prevents entire job from running too long
  security:
    timeout-minutes: 10  # Security scans should be quick
  docker:
    timeout-minutes: 20  # Docker builds need more time
```

### 2. **Step-Level Timeouts** (Granular Control)
```yaml
- name: Run tests
  timeout-minutes: 10  # Most tests should complete quickly
  run: uv run pytest
```

### 3. **Test-Level Timeouts** (Individual Test Protection)
```toml
[tool.pytest.ini_options]
timeout = 300  # 5 minutes max per test
timeout_method = "thread"  # Use thread-based timeout
```

## Recommended Timeouts

| Component | Timeout | Rationale |
|-----------|---------|-----------|
| **Test Job** | 15 min | Complete test suite + setup |
| **Security Job** | 10 min | Security scans are typically fast |
| **Docker Job** | 20 min | Docker builds can be slower |
| **Test Step** | 10 min | Most test suites should finish quickly |
| **Individual Test** | 5 min | No single test should take this long |

## Cost Protection Benefits

- **Prevents Runaway Jobs**: Jobs automatically terminate instead of running for hours
- **Budget Protection**: Limits maximum CI minutes per pipeline run
- **Fast Feedback**: Developers get quick notification of hanging tests
- **Resource Management**: Prevents CI resource exhaustion

## Monitoring

Monitor your GitHub Actions usage at:
- **Settings** → **Billing** → **Actions & Packages**
- View usage by repository and workflow
- Set up billing alerts to prevent budget overruns

## Emergency Actions

If a job is running too long:
1. **Cancel via GitHub UI**: Go to Actions tab → Cancel running workflows
2. **Disable Workflow**: Temporarily disable the workflow file
3. **Check Logs**: Identify which step/test is hanging
4. **Adjust Timeouts**: Lower timeouts if needed for faster failure

## Best Practices

1. **Start with shorter timeouts** and increase only if needed
2. **Monitor actual CI run times** to optimize timeout values
3. **Use step-level timeouts** for expensive operations
4. **Add test-level timeouts** for individual test protection
5. **Review timeout logs** to identify slow tests for optimization