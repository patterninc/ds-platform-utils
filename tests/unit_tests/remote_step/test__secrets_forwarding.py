"""A sibling @secrets has to reach the runner pod.

Metaflow's @secrets fetches into the *driver's* os.environ during
task_pre_step. The runner is a separate pod in a separate cluster and
inherits nothing, and the forwarding list carries only GITHUB_TOKEN plus
METAFLOW_/OBP_/OUTERBOUNDS_ prefixes -- so a value exported under its own
name, which is the whole point of @secrets, never crossed. The step body then
died on os.environ["PATTERN_API_KEY"] with a KeyError naming a variable the
user could see was configured.
"""

import metaflow  # noqa: F401  -- resolves plugins before the direct imports below
import pytest

from remote_step.plugins.remote_step_decorator import (
    _find_secret_sources,
    _resolve_secret_env,
)


class Deco:
    def __init__(self, name, **attrs):
        self.name = name
        self.attributes = dict(attrs)


# ------------------------------------------------------------------- discovery


def test_a_user_source_is_found():
    decos = [Deco("secrets", sources=["outerbounds.pattern-api"])]
    assert _find_secret_sources(decos) == ["outerbounds.pattern-api"]


def test_our_own_injected_github_source_is_excluded():
    """We add that one ourselves for uv; it is not the user's to forward."""
    decos = [Deco("secrets", sources=["outerbounds.pattern-api", "ds-platform/github"])]
    assert _find_secret_sources(decos, injected="ds-platform/github") == ["outerbounds.pattern-api"]


def test_only_our_source_means_nothing_to_forward():
    decos = [Deco("secrets", sources=["ds-platform/github"])]
    assert _find_secret_sources(decos, injected="ds-platform/github") == []


def test_a_dict_source_is_preserved_verbatim():
    """@secrets accepts dict specs; they must reach SecretSpec unmangled."""
    src = {"source": "aws-secrets-manager", "id": "prod/key", "role": "arn:aws:iam::1:role/r"}
    assert _find_secret_sources([Deco("secrets", sources=[src])]) == [src]


def test_no_secrets_decorator_finds_nothing():
    assert _find_secret_sources([Deco("card"), Deco("retry", times=2)]) == []


def test_a_secrets_decorator_with_no_sources_finds_nothing():
    assert _find_secret_sources([Deco("secrets")]) == []


# ------------------------------------------------------------------ resolution


def test_no_sources_resolves_to_nothing():
    assert _resolve_secret_env([]) == {}


def test_sources_resolve_through_metaflows_own_provider(monkeypatch):
    """Keys come from the provider, not guessed at from os.environ."""
    seen = {}

    class FakeProvider:
        def get_secret_as_dict(self, secret_id, options=None, role=None):
            seen["secret_id"] = secret_id
            seen["role"] = role
            return {"PATTERN_API_KEY": "v1", "PATTERN_API_URL": "https://x"}

    import metaflow.plugins.secrets.secrets_decorator as sd

    monkeypatch.setattr(sd, "get_secrets_backend_provider", lambda _t: FakeProvider())
    env = _resolve_secret_env(["outerbounds.pattern-api"])
    assert env == {"PATTERN_API_KEY": "v1", "PATTERN_API_URL": "https://x"}
    assert seen["secret_id"]


def test_two_sources_are_merged(monkeypatch):
    class FakeProvider:
        calls = []

        def get_secret_as_dict(self, secret_id, options=None, role=None):
            FakeProvider.calls.append(secret_id)
            return {f"KEY_{len(FakeProvider.calls)}": "v"}

    import metaflow.plugins.secrets.secrets_decorator as sd

    monkeypatch.setattr(sd, "get_secrets_backend_provider", lambda _t: FakeProvider())
    env = _resolve_secret_env(["a", "b"])
    assert sorted(env) == ["KEY_1", "KEY_2"]


def test_values_are_stringified(monkeypatch):
    """The pod env only carries strings."""

    class FakeProvider:
        def get_secret_as_dict(self, secret_id, options=None, role=None):
            return {"PORT": 5432}

    import metaflow.plugins.secrets.secrets_decorator as sd

    monkeypatch.setattr(sd, "get_secrets_backend_provider", lambda _t: FakeProvider())
    assert _resolve_secret_env(["a"]) == {"PORT": "5432"}


def test_an_unresolvable_source_warns_and_does_not_raise(capsys):
    """Metaflow's own task_pre_step raises on the same source first."""
    assert _resolve_secret_env(["no-such-backend-type-here"]) == {}
    assert "could not resolve" in capsys.readouterr().err


def test_one_bad_source_does_not_lose_a_good_one(monkeypatch):
    class FakeProvider:
        def get_secret_as_dict(self, secret_id, options=None, role=None):
            if secret_id == "bad":
                raise RuntimeError("access denied")
            return {"GOOD": "yes"}

    import metaflow.plugins.secrets.secrets_decorator as sd
    from metaflow.plugins.secrets.secrets_spec import SecretSpec

    monkeypatch.setattr(sd, "get_secrets_backend_provider", lambda _t: FakeProvider())
    monkeypatch.setattr(
        SecretSpec,
        "secret_spec_from_str",
        classmethod(lambda cls, s, role=None: type("S", (), {"secrets_backend_type": "x", "secret_id": s, "options": {}, "role": role})()),
    )
    assert _resolve_secret_env(["bad", "good"]) == {"GOOD": "yes"}


def test_the_decorator_role_is_passed_through(monkeypatch):
    """@secrets(role=...) decides which IAM role fetches the secret."""
    captured = {}

    class FakeProvider:
        def get_secret_as_dict(self, secret_id, options=None, role=None):
            captured["role"] = role
            return {}

    import metaflow.plugins.secrets.secrets_decorator as sd
    from metaflow.plugins.secrets.secrets_spec import SecretSpec

    monkeypatch.setattr(sd, "get_secrets_backend_provider", lambda _t: FakeProvider())
    monkeypatch.setattr(
        SecretSpec,
        "secret_spec_from_str",
        classmethod(lambda cls, s, role=None: type("S", (), {"secrets_backend_type": "x", "secret_id": s, "options": {}, "role": role})()),
    )
    _resolve_secret_env(["a"], role="arn:aws:iam::1:role/secrets-reader")
    assert captured["role"] == "arn:aws:iam::1:role/secrets-reader"
