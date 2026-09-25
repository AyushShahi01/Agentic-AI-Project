"""Safety policy for automation actions. Pure and deterministic.

The policy is enforced for every action node, independent of how the workflow graph is built:
a workflow without an approval step still cannot change a PROD pipeline.
"""

from dataclasses import dataclass, field

APPROVAL_REQUIRED_ENVIRONMENTS = frozenset({"PROD"})


@dataclass(frozen=True)
class PolicyDecision:
    allowed: bool
    simulate: bool = False  # dry run: record what would happen, change nothing
    reasons: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, object]:
        return {"allowed": self.allowed, "simulate": self.simulate, "reasons": self.reasons}


def check_action(
    action: str,
    *,
    environment: str,
    dry_run: bool,
    approved: bool,
    actions_last_24h: int,
    max_per_day: int,
) -> PolicyDecision:
    """Decide whether `action` may run against a DAG in `environment`."""
    if dry_run:
        return PolicyDecision(
            allowed=True, simulate=True, reasons=["Dry run: the action is simulated only"]
        )
    reasons: list[str] = []
    if environment in APPROVAL_REQUIRED_ENVIRONMENTS and not approved:
        reasons.append(f"{environment} actions require an approved approval step")
    if actions_last_24h >= max_per_day:
        reasons.append(
            f"Rate limit: {actions_last_24h} automated action(s) on this DAG in the last 24h "
            f"(max {max_per_day})"
        )
    if reasons:
        return PolicyDecision(allowed=False, reasons=reasons)
    return PolicyDecision(allowed=True, reasons=[f"{action} allowed in {environment}"])
