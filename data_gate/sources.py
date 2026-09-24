"""Evidence the gate reads from OUTSIDE its own store — and how it says it could not.

`alpha-engine-config-I10823`. Four base columns grade facts that do not live
under `s3://alpha-engine-research/data_collection/`:

* ``artifact_registry`` — the fleet's ARTIFACT_REGISTRY, read from the copy the
  freshness monitor actually enforces:
  ``s3://alpha-engine-research/_freshness_monitor/ARTIFACT_REGISTRY.yaml``,
  published by `alpha-engine-config`'s `sync-artifact-registry.yml` on every
  registry change and daily. The private repository is never checked out: that
  would need a long-lived cross-repo credential, and the published copy is the
  one the monitor pages from, so it is the better evidence anyway.
* ``consumers`` — each declared reader path, resolved against its repository's
  default branch through the GitHub REST contents API, with the workflow's own
  short-lived ``GITHUB_TOKEN``. No PAT.
* ``identity`` — the declared writer role, graded by ``iam:SimulatePrincipalPolicy``
  (see `data_gate.unit_readers.read_identity`).
* ``observability_row`` — this repository's own ``registry.d/`` tree, which the
  gate job has already checked out. No source object needed.

**The rule is the one `evidence.py` enforces: never MET without a read.** Every
source here raises :class:`SourceUnavailable` when it could not look — a denied
read, a missing token, a network failure, a repository the token cannot see —
and the reader turns that into UNMEASURABLE naming what is missing. "Could not
look" is never folded into "looked and it was absent", because the two have
opposite owners.
"""

from __future__ import annotations

import base64
import json
import pathlib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import yaml

__all__ = [
    "GATE_ROLE",
    "GITHUB_OWNER",
    "GITHUB_TOKEN_ENV",
    "PUBLISHED_REGISTRY_URI",
    "REGISTRY_GRANT",
    "ArtifactRegistrySource",
    "GitHubContents",
    "RegistryDocument",
    "SourceUnavailable",
]

#: The identity `data-gate.yml` runs under (nous-ergon-ops
#: `infrastructure/iam/github-actions-data-gate-read/`).
GATE_ROLE = "github-actions-data-gate-read"

PUBLISHED_REGISTRY_BUCKET = "alpha-engine-research"
PUBLISHED_REGISTRY_KEY = "_freshness_monitor/ARTIFACT_REGISTRY.yaml"
PUBLISHED_REGISTRY_URI = f"s3://{PUBLISHED_REGISTRY_BUCKET}/{PUBLISHED_REGISTRY_KEY}"

#: The one statement the gate role needs to read the published registry. Named
#: verbatim on every UNMEASURABLE reading it causes, so the row is a work item
#: with an address rather than a bare "AccessDenied".
REGISTRY_GRANT = (
    f"nous-ergon-ops infrastructure/iam/{GATE_ROLE}/data-gate-read.json needs "
    '{"Sid": "DataGateReadPublishedArtifactRegistry", "Effect": "Allow", '
    '"Action": "s3:GetObject", '
    f'"Resource": "arn:aws:s3:::{PUBLISHED_REGISTRY_BUCKET}/{PUBLISHED_REGISTRY_KEY}"}}'
)

#: The environment variable the CLI reads a GitHub token from. A dedicated name
#: rather than ``GITHUB_TOKEN``/``GH_TOKEN``, so a developer shell or a CI test
#: job that happens to export one never turns a hermetic test run into a
#: network read.
GITHUB_TOKEN_ENV = "DATA_GATE_GITHUB_TOKEN"

GITHUB_OWNER = "nousergon"


class SourceUnavailable(Exception):
    """We could not look. Always UNMEASURABLE, never absent."""


def _aws_error_code(exc: BaseException) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str((response.get("Error") or {}).get("Code") or "")
    return ""


@dataclass(frozen=True)
class RegistryDocument:
    """The parts of ARTIFACT_REGISTRY this gate grades against."""

    uri: str
    artifacts: tuple[dict[str, Any], ...]
    grandfathered: tuple[dict[str, Any], ...]

    def artifact(self, artifact_id: str) -> dict[str, Any] | None:
        return next((row for row in self.artifacts if row.get("artifact_id") == artifact_id), None)


class ArtifactRegistrySource:
    """One registry document, fetched at most once per gate run."""

    def __init__(self, uri: str, fetch: Callable[[], bytes]) -> None:
        self.uri = uri
        self._fetch = fetch
        self._document: RegistryDocument | None = None
        self._problem: str | None = None

    @classmethod
    def from_path(cls, path: pathlib.Path | str) -> ArtifactRegistrySource:
        location = pathlib.Path(path)

        def fetch() -> bytes:
            try:
                return location.read_bytes()
            except OSError as exc:
                raise SourceUnavailable(f"could not read {location}: {type(exc).__name__}: {exc}") from exc

        return cls(str(location), fetch)

    @classmethod
    def from_s3(cls, uri: str, client_factory: Callable[[], Any]) -> ArtifactRegistrySource:
        bucket, _, key = uri[len("s3://") :].partition("/")

        def fetch() -> bytes:
            try:
                return client_factory().get_object(Bucket=bucket, Key=key)["Body"].read()
            except Exception as exc:  # noqa: BLE001 - classified below, re-raised as SourceUnavailable
                # Deliberate: the failure mode is "this one object could not be
                # read"; every other clause survives; the recording surface is
                # the UNMEASURABLE row the reader builds from this message.
                code = _aws_error_code(exc)
                if code in {"AccessDenied", "403", "AllAccessDisabled"}:
                    raise SourceUnavailable(
                        f"AccessDenied reading {uri} as {GATE_ROLE}; {REGISTRY_GRANT}"
                    ) from exc
                if code in {"NoSuchKey", "404", "NotFound"}:
                    raise SourceUnavailable(
                        f"{uri} is ABSENT — alpha-engine-config sync-artifact-registry.yml has "
                        "not published it, so there is no enforced registry to grade against"
                    ) from exc
                raise SourceUnavailable(f"could not read {uri}: {type(exc).__name__}: {exc}") from exc

        return cls(uri, fetch)

    @classmethod
    def for_uri(cls, uri: str, client_factory: Callable[[], Any]) -> ArtifactRegistrySource:
        if uri.startswith("s3://"):
            return cls.from_s3(uri, client_factory)
        return cls.from_path(uri[len("file://") :] if uri.startswith("file://") else uri)

    def load(self) -> RegistryDocument:
        if self._document is not None:
            return self._document
        if self._problem is not None:
            raise SourceUnavailable(self._problem)
        try:
            raw = self._fetch()
            parsed = yaml.safe_load(raw)
            if not isinstance(parsed, dict) or not isinstance(parsed.get("artifacts"), list):
                raise SourceUnavailable(
                    f"{self.uri} parsed, but carries no `artifacts:` list — a registry this reader "
                    "cannot interpret is not a registry with no rows"
                )
        except SourceUnavailable as exc:
            self._problem = str(exc)
            raise
        except yaml.YAMLError as exc:
            self._problem = f"{self.uri} is not valid YAML: {exc}"
            raise SourceUnavailable(self._problem) from exc
        self._document = RegistryDocument(
            uri=self.uri,
            artifacts=tuple(r for r in parsed["artifacts"] if isinstance(r, dict)),
            grandfathered=tuple(r for r in (parsed.get("grandfathered_paths") or []) if isinstance(r, dict)),
        )
        return self._document


#: ``opener(url, headers) -> (status, body)``. Injected in tests; the default
#: reaches api.github.com.
Opener = Callable[[str, dict[str, str]], tuple[int, bytes]]


def _urllib_opener(url: str, headers: dict[str, str]) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers=headers)  # noqa: S310 - fixed https API host
    try:
        with urllib.request.urlopen(request, timeout=20) as response:  # noqa: S310
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read() if exc.fp else b""
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise SourceUnavailable(f"could not reach {url}: {type(exc).__name__}: {exc}") from exc


class GitHubContents:
    """Does a path exist on a repository's default branch? Answered once per path.

    A repository the token cannot see answers 404 exactly like a missing file,
    which is the trap: a private or renamed consumer repo would read as "the
    reader is gone". So the repository is resolved FIRST, and an invisible
    repository raises :class:`SourceUnavailable` — we could not look — rather
    than letting its paths read absent.
    """

    API = "https://api.github.com"

    def __init__(self, token: str, *, owner: str = GITHUB_OWNER, opener: Opener | None = None) -> None:
        if not token:
            raise ValueError("GitHubContents needs a token; construct it only when one is configured")
        self.owner = owner
        self._token = token
        self._opener = opener or _urllib_opener
        self._repos: dict[str, bool] = {}
        self._paths: dict[tuple[str, str], str | None] = {}
        self._contents: dict[tuple[str, str], tuple[str | None, bytes | None]] = {}

    def _get(self, url: str) -> tuple[int, Any]:
        status, body = self._opener(
            url,
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self._token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "nousergon-data-gate",
            },
        )
        try:
            payload = json.loads(body or b"null")
        except ValueError:
            payload = None
        return status, payload

    def _require_repo(self, repo: str) -> None:
        if repo not in self._repos:
            status, _ = self._get(f"{self.API}/repos/{self.owner}/{urllib.parse.quote(repo)}")
            if status == 200:
                self._repos[repo] = True
            elif status == 404:
                self._repos[repo] = False
            else:
                raise SourceUnavailable(f"GitHub answered HTTP {status} for repository {self.owner}/{repo}")
        if not self._repos[repo]:
            raise SourceUnavailable(
                f"repository {self.owner}/{repo} is not visible to the gate's token (private, renamed "
                "or deleted) — its paths cannot be told apart from absent, so they are not graded"
            )

    def path_kind(self, repo: str, path: str) -> str | None:
        """``"file"``, ``"dir"``, another GitHub content type, or ``None`` when absent."""
        self._require_repo(repo)
        cache_key = (repo, path)
        if cache_key not in self._paths:
            url = (
                f"{self.API}/repos/{self.owner}/{urllib.parse.quote(repo)}/contents/"
                f"{urllib.parse.quote(path)}"
            )
            status, payload = self._get(url)
            if status == 404:
                self._paths[cache_key] = None
            elif status == 200:
                self._paths[cache_key] = "dir" if isinstance(payload, list) else str((payload or {}).get("type") or "file")
            else:
                raise SourceUnavailable(f"GitHub answered HTTP {status} for {self.owner}/{repo}:{path}")
        return self._paths[cache_key]

    def read_file(self, repo: str, path: str) -> tuple[str | None, bytes | None]:
        """``(kind, content)`` of a path on the default branch, one request per path.

        `alpha-engine-config-I11282`: a consumer pin is graded on the pinned
        file's CONTENT, not its existence, so the contents API's ``content``
        field is decoded here. ``(None, None)`` is absent; a directory or other
        non-file answers its kind with no content. A file the API will not
        inline (over its 1 MB inline limit) raises :class:`SourceUnavailable`
        — we could not look — rather than reading as an empty file.
        """
        self._require_repo(repo)
        cache_key = (repo, path)
        if cache_key not in self._contents:
            url = (
                f"{self.API}/repos/{self.owner}/{urllib.parse.quote(repo)}/contents/"
                f"{urllib.parse.quote(path)}"
            )
            status, payload = self._get(url)
            if status == 404:
                self._contents[cache_key] = (None, None)
            elif status != 200:
                raise SourceUnavailable(f"GitHub answered HTTP {status} for {self.owner}/{repo}:{path}")
            elif isinstance(payload, list):
                self._contents[cache_key] = ("dir", None)
            else:
                kind = str((payload or {}).get("type") or "file")
                if kind != "file":
                    self._contents[cache_key] = (kind, None)
                elif (payload or {}).get("encoding") != "base64":
                    raise SourceUnavailable(
                        f"GitHub did not inline {self.owner}/{repo}:{path} (encoding "
                        f"{(payload or {}).get('encoding')!r}); its content cannot be compared"
                    )
                else:
                    self._contents[cache_key] = ("file", base64.b64decode(payload.get("content") or ""))
            self._paths.setdefault(cache_key, self._contents[cache_key][0])
        return self._contents[cache_key]
