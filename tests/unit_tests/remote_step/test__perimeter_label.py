"""The perimeter stamped on a submitted Job.

A team namespace holds every perimeter's pods, so without this label a prod
run and an ad-hoc one are indistinguishable to kubectl and to anything
reading labels for cost attribution.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step import keys
from remote_step.config import RemoteStepConfig
from remote_step.submit import StepResources, build_manifest

PERIMETER_LABEL = "remote-step.pattern.com/perimeter"


@pytest.fixture
def cfg():
    return RemoteStepConfig(
        cluster_name="pattern-ml-platform",
        cluster_endpoint="https://example.eks.amazonaws.com",
        region="us-west-2",
        payload_bucket="pattern-ml-platform",
        runner_image="example.dkr.ecr.us-west-2.amazonaws.com/runner:latest",
        service_account="remote-step-runner",
        submitter_role_arn="arn:aws:iam::1:role/submitter",
        artifact_read_role_arn="arn:aws:iam::1:role/reader",
        local_queue="default",
        log_group="/pattern-ml-platform/steps",
        teams=("forecasting",),
    )


def manifest(cfg, **kwargs):
    defaults = {
        "flow_name": "SampleRemoteFlow",
        "run_id": "238556",
        "step_name": "train",
        "task_id": "1925061",
        "attempt": 0,
        "user": "chinmay.dolli@pattern.com",
        "team": "forecasting",
    }
    return build_manifest(
        cfg,
        StepResources(cpu=2, memory_mb=4000, gpus=0, cpu_arch="arm64"),
        "s3://bucket/spec.json",
        **{**defaults, **kwargs},
    )


@pytest.mark.parametrize("perimeter", ["prod", "default", "staging"])
def test_perimeter_is_stamped_as_a_label(cfg, perimeter):
    labels = manifest(cfg, perimeter=perimeter)["metadata"]["labels"]
    assert labels[PERIMETER_LABEL] == perimeter


def test_defaults_to_the_default_perimeter(cfg):
    """Matches Outerbounds' own default, so the common case is unsurprising."""
    labels = manifest(cfg)["metadata"]["labels"]
    assert labels[PERIMETER_LABEL] == keys.DEFAULT_PERIMETER


def test_prod_and_default_are_distinguishable_in_one_namespace(cfg):
    """The whole point: same team, same run id, different perimeter.

    Run ids are only unique within a perimeter, so the run-id label alone can
    match pods from two of them.
    """
    prod = manifest(cfg, perimeter="prod")["metadata"]["labels"]
    adhoc = manifest(cfg, perimeter="default")["metadata"]["labels"]

    assert prod["remote-step.pattern.com/run-id"] == adhoc["remote-step.pattern.com/run-id"]
    assert prod[PERIMETER_LABEL] != adhoc[PERIMETER_LABEL]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Prod", "prod"),
        ("prod_west", "prod-west"),
        ("pattern.prod", "pattern-prod"),
        ("-prod-", "prod"),
    ],
)
def test_an_awkward_perimeter_name_is_coerced_to_a_valid_label(cfg, raw, expected):
    """An invalid label value would make the API reject the whole Job."""
    assert manifest(cfg, perimeter=raw)["metadata"]["labels"][PERIMETER_LABEL] == expected


def test_the_label_survives_onto_the_pod_template(cfg):
    """Kueue admits the Job; the pod is what actually gets scraped."""
    m = manifest(cfg, perimeter="prod")
    pod_labels = m["spec"]["template"]["metadata"]["labels"]
    assert pod_labels[PERIMETER_LABEL] == "prod"
