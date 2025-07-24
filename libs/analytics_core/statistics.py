"""Statistical analysis utilities for A/B testing and experimentation."""

import math
from dataclasses import dataclass
from enum import Enum
from typing import Any

import numpy as np
import structlog
from pydantic import BaseModel, Field
from scipy import stats

logger = structlog.get_logger(__name__)


class TestType(str, Enum):
    """Types of statistical tests."""

    TWO_SAMPLE_TTEST = "two_sample_ttest"
    WELCH_TTEST = "welch_ttest"
    MANN_WHITNEY = "mann_whitney"
    CHI_SQUARE = "chi_square"
    FISHERS_EXACT = "fishers_exact"
    PROPORTION_ZTEST = "proportion_ztest"
    BAYESIAN_TTEST = "bayesian_ttest"


class EffectSizeType(str, Enum):
    """Types of effect size measurements."""

    COHENS_D = "cohens_d"
    GLASS_DELTA = "glass_delta"
    HEDGES_G = "hedges_g"
    CRAMERS_V = "cramers_v"
    ODDS_RATIO = "odds_ratio"
    RELATIVE_RISK = "relative_risk"


class MultipleComparisonMethod(str, Enum):
    """Methods for multiple comparison correction."""

    BONFERRONI = "bonferroni"
    HOLM = "holm"
    BENJAMINI_HOCHBERG = "benjamini_hochberg"
    BENJAMINI_YEKUTIELI = "benjamini_yekutieli"
    SIDAK = "sidak"


@dataclass
class StatisticalTest:
    """Result of a statistical test."""

    test_type: TestType
    statistic: float
    p_value: float
    effect_size: float | None = None
    effect_size_type: EffectSizeType | None = None
    confidence_interval: tuple[float, float] | None = None
    degrees_of_freedom: float | None = None
    sample_size_a: int | None = None
    sample_size_b: int | None = None
    power: float | None = None
    is_significant: bool = False
    alpha: float = 0.05

    def __post_init__(self):
        """Calculate significance after initialization."""
        self.is_significant = self.p_value < self.alpha


class PowerAnalysisConfig(BaseModel):
    """Configuration for power analysis."""

    effect_size: float = Field(description="Expected effect size")
    alpha: float = Field(default=0.05, description="Type I error rate")
    power: float = Field(default=0.8, description="Desired statistical power")
    test_type: TestType = Field(default=TestType.TWO_SAMPLE_TTEST)
    two_sided: bool = Field(default=True, description="Two-sided test")
    ratio: float = Field(default=1.0, description="Ratio of sample sizes (n2/n1)")


class BayesianTestResult(BaseModel):
    """Result of a Bayesian A/B test."""

    probability_a_better: float = Field(description="Probability that A > B")
    probability_b_better: float = Field(description="Probability that B > A")
    expected_loss_a: float = Field(description="Expected loss if choosing A")
    expected_loss_b: float = Field(description="Expected loss if choosing B")
    credible_interval_diff: tuple[float, float] = Field(
        description="95% credible interval for difference"
    )
    rope_probability: float | None = Field(
        default=None, description="Probability of practical equivalence"
    )
    rope_bounds: tuple[float, float] | None = Field(
        default=None, description="Region of practical equivalence"
    )


class StatisticalAnalyzer:
    """Statistical analysis engine for A/B testing."""

    def __init__(self):
        """Initialize statistical analyzer."""
        self.logger = logger.bind(component="statistical_analyzer")

    def two_sample_ttest(
        self,
        group_a: list[float],
        group_b: list[float],
        equal_var: bool = True,
        alpha: float = 0.05,
    ) -> StatisticalTest:
        """Perform two-sample t-test."""
        try:
            # Convert to numpy arrays
            a = np.array(group_a)
            b = np.array(group_b)

            # Perform t-test
            if equal_var:
                statistic, p_value = stats.ttest_ind(a, b, equal_var=True)
                test_type = TestType.TWO_SAMPLE_TTEST
            else:
                statistic, p_value = stats.ttest_ind(a, b, equal_var=False)
                test_type = TestType.WELCH_TTEST

            # Calculate effect size (Cohen's d)
            effect_size = self.calculate_cohens_d(a, b)

            # Calculate confidence interval for difference in means
            diff_mean = np.mean(b) - np.mean(a)
            se_diff = np.sqrt(np.var(a, ddof=1) / len(a) + np.var(b, ddof=1) / len(b))
            df = len(a) + len(b) - 2 if equal_var else self._welch_df(a, b)
            t_critical = stats.t.ppf(1 - alpha / 2, df)
            ci_lower = diff_mean - t_critical * se_diff
            ci_upper = diff_mean + t_critical * se_diff

            return StatisticalTest(
                test_type=test_type,
                statistic=float(statistic),
                p_value=float(p_value),
                effect_size=effect_size,
                effect_size_type=EffectSizeType.COHENS_D,
                confidence_interval=(ci_lower, ci_upper),
                degrees_of_freedom=df,
                sample_size_a=len(a),
                sample_size_b=len(b),
                alpha=alpha,
            )

        except Exception as error:
            self.logger.error("Two-sample t-test failed", error=str(error))
            raise

    def mann_whitney_u(
        self, group_a: list[float], group_b: list[float], alpha: float = 0.05
    ) -> StatisticalTest:
        """Perform Mann-Whitney U test (non-parametric)."""
        try:
            a = np.array(group_a)
            b = np.array(group_b)

            statistic, p_value = stats.mannwhitneyu(a, b, alternative="two-sided")

            # Calculate effect size (rank-biserial correlation)
            n1, n2 = len(a), len(b)
            effect_size = 1 - (2 * statistic) / (n1 * n2)

            return StatisticalTest(
                test_type=TestType.MANN_WHITNEY,
                statistic=float(statistic),
                p_value=float(p_value),
                effect_size=effect_size,
                effect_size_type=EffectSizeType.GLASS_DELTA,  # Using as proxy
                sample_size_a=n1,
                sample_size_b=n2,
                alpha=alpha,
            )

        except Exception as error:
            self.logger.error("Mann-Whitney U test failed", error=str(error))
            raise

    def proportion_test(
        self,
        successes_a: int,
        trials_a: int,
        successes_b: int,
        trials_b: int,
        alpha: float = 0.05,
    ) -> StatisticalTest:
        """Perform two-proportion z-test."""
        try:
            from statsmodels.stats.proportion import proportions_ztest

            counts = np.array([successes_a, successes_b])
            nobs = np.array([trials_a, trials_b])

            statistic, p_value = proportions_ztest(counts, nobs)

            # Calculate effect size (difference in proportions)
            p1 = successes_a / trials_a
            p2 = successes_b / trials_b
            effect_size = p2 - p1

            # Calculate confidence interval for difference
            p_pooled = (successes_a + successes_b) / (trials_a + trials_b)
            se_diff = np.sqrt(p_pooled * (1 - p_pooled) * (1 / trials_a + 1 / trials_b))
            z_critical = stats.norm.ppf(1 - alpha / 2)
            ci_lower = effect_size - z_critical * se_diff
            ci_upper = effect_size + z_critical * se_diff

            return StatisticalTest(
                test_type=TestType.PROPORTION_ZTEST,
                statistic=float(statistic),
                p_value=float(p_value),
                effect_size=effect_size,
                confidence_interval=(ci_lower, ci_upper),
                sample_size_a=trials_a,
                sample_size_b=trials_b,
                alpha=alpha,
            )

        except Exception as error:
            self.logger.error("Proportion test failed", error=str(error))
            raise

    def chi_square_test(
        self, contingency_table: list[list[int]], alpha: float = 0.05
    ) -> StatisticalTest:
        """Perform chi-square test of independence."""
        try:
            table = np.array(contingency_table)
            statistic, p_value, dof, expected = stats.chi2_contingency(table)

            # Calculate Cramér's V
            n = np.sum(table)
            cramers_v = np.sqrt(statistic / (n * (min(table.shape) - 1)))

            return StatisticalTest(
                test_type=TestType.CHI_SQUARE,
                statistic=float(statistic),
                p_value=float(p_value),
                effect_size=cramers_v,
                effect_size_type=EffectSizeType.CRAMERS_V,
                degrees_of_freedom=float(dof),
                alpha=alpha,
            )

        except Exception as error:
            self.logger.error("Chi-square test failed", error=str(error))
            raise

    def calculate_cohens_d(self, group_a: np.ndarray, group_b: np.ndarray) -> float:
        """Calculate Cohen's d effect size."""
        n1, n2 = len(group_a), len(group_b)
        var1, var2 = np.var(group_a, ddof=1), np.var(group_b, ddof=1)

        # Pooled standard deviation
        pooled_std = np.sqrt(((n1 - 1) * var1 + (n2 - 1) * var2) / (n1 + n2 - 2))

        return (np.mean(group_b) - np.mean(group_a)) / pooled_std

    def calculate_hedges_g(self, group_a: np.ndarray, group_b: np.ndarray) -> float:
        """Calculate Hedges' g effect size (bias-corrected Cohen's d)."""
        cohens_d = self.calculate_cohens_d(group_a, group_b)
        n = len(group_a) + len(group_b)
        correction_factor = 1 - (3 / (4 * n - 9))
        return cohens_d * correction_factor

    def _welch_df(self, a: np.ndarray, b: np.ndarray) -> float:
        """Calculate Welch's degrees of freedom for unequal variances."""
        s1, s2 = np.var(a, ddof=1), np.var(b, ddof=1)
        n1, n2 = len(a), len(b)

        numerator = (s1 / n1 + s2 / n2) ** 2
        denominator = (s1 / n1) ** 2 / (n1 - 1) + (s2 / n2) ** 2 / (n2 - 1)

        return numerator / denominator

    def power_analysis(self, config: PowerAnalysisConfig) -> dict[str, float]:
        """Perform power analysis to determine sample size."""
        try:
            from statsmodels.stats.power import tt_solve_power, ttest_power

            if config.test_type in [TestType.TWO_SAMPLE_TTEST, TestType.WELCH_TTEST]:
                # Calculate required sample size
                sample_size = tt_solve_power(
                    effect_size=config.effect_size,
                    power=config.power,
                    alpha=config.alpha,
                    alternative="two-sided" if config.two_sided else "larger",
                )

                # Calculate power for given sample size
                actual_power = ttest_power(
                    effect_size=config.effect_size,
                    nobs=sample_size,
                    alpha=config.alpha,
                    alternative="two-sided" if config.two_sided else "larger",
                )

                return {
                    "required_sample_size_per_group": math.ceil(sample_size),
                    "total_sample_size": math.ceil(sample_size * 2),  # Two groups
                    "actual_power": actual_power,
                    "effect_size": config.effect_size,
                    "alpha": config.alpha,
                }
            else:
                self.logger.warning(
                    "Power analysis not implemented for test type",
                    test_type=config.test_type,
                )
                return {}

        except Exception as error:
            self.logger.error("Power analysis failed", error=str(error))
            raise

    def sequential_test(
        self,
        group_a: list[float],
        group_b: list[float],
        alpha: float = 0.05,
        beta: float = 0.2,
        effect_size: float = 0.5,
    ) -> dict[str, Any]:
        """Perform sequential probability ratio test for early stopping."""
        try:
            n = len(group_a)
            if n != len(group_b):
                raise ValueError(
                    "Groups must have equal sample sizes for sequential test"
                )

            # Calculate test statistic (standardized difference)
            mean_diff = np.mean(group_b) - np.mean(group_a)
            pooled_var = (np.var(group_a, ddof=1) + np.var(group_b, ddof=1)) / 2
            se = np.sqrt(2 * pooled_var / n)
            z_stat = mean_diff / se if se > 0 else 0

            # Sequential boundaries
            a = np.log((1 - beta) / alpha)  # Upper boundary (reject H0)
            b = np.log(beta / (1 - alpha))  # Lower boundary (accept H0)

            # Information function
            theta = effect_size / np.sqrt(2)  # Standardized effect size
            information = n * theta**2 / 2

            # Decision boundaries as function of information
            upper_boundary = a / theta
            lower_boundary = b / theta

            # Current test statistic standardized by information
            current_stat = z_stat * np.sqrt(information)

            decision = "continue"
            if current_stat >= upper_boundary:
                decision = "reject_h0"  # Significant difference detected
            elif current_stat <= lower_boundary:
                decision = "accept_h0"  # No significant difference

            return {
                "decision": decision,
                "test_statistic": current_stat,
                "upper_boundary": upper_boundary,
                "lower_boundary": lower_boundary,
                "information": information,
                "sample_size": n,
                "continue_sampling": decision == "continue",
            }

        except Exception as error:
            self.logger.error("Sequential test failed", error=str(error))
            raise

    def bayesian_ab_test(
        self,
        group_a: list[float],
        group_b: list[float],
        prior_mean: float = 0,
        prior_std: float = 1,
        rope_bounds: tuple[float, float] | None = None,
        n_samples: int = 10000,
    ) -> BayesianTestResult:
        """Perform Bayesian A/B test."""
        try:
            import pymc as pm

            a = np.array(group_a)
            b = np.array(group_b)

            with pm.Model():
                # Priors
                mu_a = pm.Normal("mu_a", mu=prior_mean, sigma=prior_std)
                mu_b = pm.Normal("mu_b", mu=prior_mean, sigma=prior_std)
                sigma_a = pm.HalfNormal("sigma_a", sigma=1)
                sigma_b = pm.HalfNormal("sigma_b", sigma=1)

                # Likelihoods
                pm.Normal("obs_a", mu=mu_a, sigma=sigma_a, observed=a)
                pm.Normal("obs_b", mu=mu_b, sigma=sigma_b, observed=b)

                # Difference
                pm.Deterministic("diff", mu_b - mu_a)

                # Sample
                trace = pm.sample(
                    n_samples, return_inferencedata=True, progressbar=False
                )

            # Extract posterior samples
            diff_samples = trace.posterior["diff"].values.flatten()

            # Calculate probabilities
            prob_b_better = np.mean(diff_samples > 0)
            prob_a_better = 1 - prob_b_better

            # Expected losses
            expected_loss_a = np.mean(np.maximum(diff_samples, 0))
            expected_loss_b = np.mean(np.maximum(-diff_samples, 0))

            # Credible interval
            ci_lower, ci_upper = np.percentile(diff_samples, [2.5, 97.5])

            # ROPE analysis if bounds provided
            rope_prob = None
            if rope_bounds:
                rope_prob = np.mean(
                    (diff_samples >= rope_bounds[0]) & (diff_samples <= rope_bounds[1])
                )

            return BayesianTestResult(
                probability_a_better=prob_a_better,
                probability_b_better=prob_b_better,
                expected_loss_a=expected_loss_a,
                expected_loss_b=expected_loss_b,
                credible_interval_diff=(ci_lower, ci_upper),
                rope_probability=rope_prob,
                rope_bounds=rope_bounds,
            )

        except ImportError:
            self.logger.error("PyMC not available for Bayesian analysis")
            raise ImportError("PyMC is required for Bayesian A/B testing")
        except Exception as error:
            self.logger.error("Bayesian A/B test failed", error=str(error))
            raise

    def multiple_comparison_correction(
        self,
        p_values: list[float],
        method: MultipleComparisonMethod = MultipleComparisonMethod.BENJAMINI_HOCHBERG,
        alpha: float = 0.05,
    ) -> dict[str, Any]:
        """Apply multiple comparison correction."""
        try:
            from statsmodels.stats.multitest import multipletests

            method_map = {
                MultipleComparisonMethod.BONFERRONI: "bonferroni",
                MultipleComparisonMethod.HOLM: "holm",
                MultipleComparisonMethod.BENJAMINI_HOCHBERG: "fdr_bh",
                MultipleComparisonMethod.BENJAMINI_YEKUTIELI: "fdr_by",
                MultipleComparisonMethod.SIDAK: "sidak",
            }

            reject, p_corrected, alpha_sidak, alpha_bonf = multipletests(
                p_values, alpha=alpha, method=method_map[method]
            )

            return {
                "original_p_values": p_values,
                "corrected_p_values": p_corrected.tolist(),
                "reject_null": reject.tolist(),
                "method": method.value,
                "original_alpha": alpha,
                "effective_alpha": alpha_bonf
                if method == MultipleComparisonMethod.BONFERRONI
                else alpha,
            }

        except Exception as error:
            self.logger.error("Multiple comparison correction failed", error=str(error))
            raise

    def calculate_minimum_detectable_effect(
        self,
        sample_size: int,
        alpha: float = 0.05,
        power: float = 0.8,
        baseline_mean: float | None = None,
        baseline_std: float | None = None,
    ) -> dict[str, float]:
        """Calculate minimum detectable effect for given sample size."""
        try:
            from statsmodels.stats.power import tt_solve_power

            effect_size = tt_solve_power(
                effect_size=None,
                nobs=sample_size,
                alpha=alpha,
                power=power,
                alternative="two-sided",
            )

            # Convert to practical units if baseline provided
            if baseline_mean is not None and baseline_std is not None:
                practical_effect = effect_size * baseline_std
                relative_effect = (
                    practical_effect / baseline_mean if baseline_mean != 0 else 0
                )

                return {
                    "effect_size_cohens_d": effect_size,
                    "practical_effect": practical_effect,
                    "relative_effect_percent": relative_effect * 100,
                }

            return {"effect_size_cohens_d": effect_size}

        except Exception as error:
            self.logger.error("MDE calculation failed", error=str(error))
            raise
