"""Static dataflow analysis for Step Functions definitions: is every field a
state reads guaranteed to exist on every path that reaches it?

WHY THIS IS A STATIC OBLIGATION AND NOT A RUNTIME ONE
-----------------------------------------------------
A ``States.Runtime`` raised while evaluating a state's ``Parameters`` is NOT
catchable. It happens *before* the task runs, so the state's own
``Catch: [States.ALL]`` never sees it, a Map's ``ToleratedFailurePercentage``
never applies, and the whole execution dies — usually inside the very state
that was supposed to report some other failure. AWS's own
``validate-state-machine-definition`` does not resolve JSONPath scope, and the
definition is valid JSON and valid ASL, so nothing between the editor and
production rejects it. Correctness of parameter references can therefore only
be established here.

HISTORY THIS EXISTS FOR
-----------------------
The analysis originated in ``tests/test_sf_groom_field_reachability.py``
(2026-07-27, alpha-engine-config#4333) for three live-fatal defects in the
groom dispatch SF, and was pinned to ``step_function_groom.json`` alone. On
2026-08-28 the *same* class killed the weekly pipeline from a different
definition: ``ModelZooTrainMap``'s ``ItemSelector`` did not pass ``run_date``
into the item payload while ``TrainSpecDispatch`` inside it read
``$.run_date`` (alpha-engine-config#9077). That was the fifth recorded
recurrence of the ``States.Runtime``-masks-the-error class
(alpha-engine-config#5950) and the first outside a notifier state — a guard
capable of catching it existed and simply did not run over that file. This
module is that guard, generalised to every definition in the repo, so the
fleet's ``fix-not-propagated-to-analogous-sites`` failure mode cannot
reintroduce it from a file nobody remembered to add.

WHAT IS MODELLED, AND WHY EACH PIECE IS REQUIRED
------------------------------------------------
Each item below is present because omitting it produced a *false positive* on
one of the four live definitions. That matters: a guard that fires on a
correct definition gets muted, and a muted guard is worse than none.

* ``OutputPath`` re-roots the payload. All three non-groom pipelines use the
  ``Parameters: {"merged.$": States.JsonMerge(...)}`` + ``OutputPath:
  "$.merged"`` idiom to layer defaults under the execution input. Without
  modelling it, every state after ``InitializeInput`` looks empty.
* Literal-JSON intrinsics are evaluated. ``States.JsonMerge(A, B, false)``
  produces ``keys(A) | keys(B)``; ``States.StringToJson('<literal>')``
  produces its keys; ``States.StringToJson(States.Format('{"k":"{}"}', …))``
  produces the ``k``s. This is how every guaranteed-field floor in the fleet
  is declared, including the ``sns_topic_arn`` / ``run_date`` / degradation-
  flag floor in ``InitializeInput``.
* ``$$.Execution.Input`` contributes the *entry contract* — the intersection
  of what every declared caller passes — not "anything". See
  ``entry_contract`` below: it is DERIVED from the CloudFormation EventBridge
  target inputs and the trigger Lambda, never hand-kept.
* A Choice rule that tests ``IsPresent: true`` proves the field on the edge it
  takes. ``CheckSpotDispatchNeeded`` depends on exactly this: the cadence
  trigger deliberately omits ``ec2_instance_id`` so the spot-dispatch path
  always engages, and the alternative edge is guarded by ``IsPresent``.
* Boolean literal facts propagate along Choice edges. Taking an edge whose
  condition is ``$.skip_x BooleanEquals true`` establishes ``skip_x == true``
  downstream, which makes the later ``CheckSkipX`` Choice *forced* and its
  other edge infeasible. Without this, the weekly SF's "skip everything"
  entry path appears to reach stages it provably cannot.
* ``Parallel`` branches and ``Map`` item processors are separate scopes,
  seeded from the Parallel's input and the Map's ``ItemSelector``
  respectively — the Map seeding being the #9077 defect itself.
* ``ResultSelector`` resolves against the TASK RESULT, and ``ResultPath`` /
  ``OutputPath`` / ``InputPath`` are write targets or re-roots, so none of
  them is a read of the item payload.

The analysis is deliberately conservative in one direction only: it models the
first path segment of a reference (``$.foo.bar`` -> ``foo``). Deeper modelling
would require each Lambda's response schema, which is not statically knowable
here, and every defect in the recorded history was a missing top-level field.
"""

from __future__ import annotations

import heapq
import itertools
import json
import re
from pathlib import Path

INFRA = Path(__file__).resolve().parent

#: Safety bound on the path-sensitive walk. Exceeding it is a loud failure, never
#: a silently truncated analysis (fail-loud: a partial sweep reads as a clean one).
#:
#: Raised from 200,000 (alpha-engine-config-I12481). Measured 2026-10-21: the weekly
#: graph on main already walked 199,046 contexts, 99.5% of the old bound, with
#: ~1,936 contexts on every state of the cadence path between spot bootstrap and
#: CheckShellRun. Any one new state on that path costs about that many, so the
#: weekly SF could no longer gain a state without failing this check, even one
#: that adds no boolean fact (I12481 adds two and lands at 202,918). The
#: walk is linear in this number, so the bound is a runaway guard for a
#: combinatorial blow-up, which is still loud. It does not budget legitimate
#: growth. Doubling it leaves room for that growth and still catches the 10s
#: blow-up the liveness pass was written to stop.
_MAX_CONTEXTS = 400_000

#: ``$.foo`` / ``$.foo.bar``, never ``$$.Map.Item.Value`` and never a bare ``$``.
_REF = re.compile(r"(?<!\$)\$\.([A-Za-z_][A-Za-z0-9_]*)")

#: A single-quoted span inside an ASL intrinsic is a LITERAL argument — a JSON
#: blob or a format template — and ASL never dereferences inside one. A JSONPath
#: argument is never quoted. Stripping these before matching is what stops a
#: floor whose own default text explains itself ("no $.error was set …") from
#: being read as a dereference of the field it is defaulting.
_QUOTED = re.compile(r"'[^']*'")

#: Keys whose contents are not references evaluated against the input payload.
_NOT_A_READ = (
    "ItemProcessor",
    "Iterator",
    "ItemSelector",
    "Catch",
    "Retry",
    "ResultPath",
    "ResultSelector",
    "OutputPath",
    "InputPath",
    "Branches",
    "Comment",
    # A Pass's ``Result`` is a LITERAL value — ASL never evaluates it — so a
    # ``$.foo`` appearing inside one is prose, not a dereference. Reading it as a
    # reference is the same mistake as a scanner treating a comment as an
    # execution path: ``StampMarketHoursVerdictMissing`` explains itself by NAMING
    # ``$.market_hours_gate_error`` in its own Cause text, and was reported as a
    # defect for describing the very condition it exists to report.
    "Result",
)


# --------------------------------------------------------------------------- #
# Reference extraction
# --------------------------------------------------------------------------- #
def refs(node) -> set[str]:
    """Every top-level field name referenced anywhere inside ``node``."""
    found: set[str] = set()
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "Comment":
                continue
            found |= refs(value)
    elif isinstance(node, list):
        for value in node:
            found |= refs(value)
    elif isinstance(node, str):
        found |= set(_REF.findall(_QUOTED.sub("''", node)))
    return found


def _guarded_vars(rule: dict) -> set[str]:
    """Variables an ``IsPresent`` in this rule makes safe to *reference*.

    Testing a field's presence is the standard ASL idiom for an optional field
    and never raises, so such a reference is not a defect.
    """
    safe: set[str] = set()
    if "IsPresent" in rule and "Variable" in rule:
        safe |= set(_REF.findall(rule["Variable"]))
    for key in ("And", "Or"):
        for sub in rule.get(key, []):
            safe |= _guarded_vars(sub)
    if "Not" in rule:
        safe |= _guarded_vars(rule["Not"])
    return safe


def state_refs(state: dict) -> set[str]:
    """Fields this state reads, excluding ``IsPresent``-guarded Choice variables."""
    kind = state.get("Type")
    if kind == "Choice":
        needed: set[str] = set()
        for rule in state.get("Choices", []):
            needed |= refs(rule) - _guarded_vars(rule)
        return needed
    if kind == "Fail":
        # ``Error`` / ``Cause`` are literal strings; only the *Path variants read.
        return refs({k: state[k] for k in ("ErrorPath", "CausePath") if k in state})
    return refs({k: v for k, v in state.items() if k not in _NOT_A_READ})


# --------------------------------------------------------------------------- #
# Intrinsic-function evaluation (guaranteed produced keys)
# --------------------------------------------------------------------------- #
def _split_args(text: str) -> list[str]:
    """Split an intrinsic argument list on its TOP-LEVEL commas."""
    out: list[str] = []
    depth, in_quote, current = 0, False, ""
    for char in text:
        if char == "'":
            in_quote = not in_quote
        if not in_quote:
            if char in "([":
                depth += 1
            elif char in ")]":
                depth -= 1
            elif char == "," and depth == 0:
                out.append(current.strip())
                current = ""
                continue
        current += char
    if current.strip():
        out.append(current.strip())
    return out


def _inner(expr: str, func: str) -> str:
    """The argument text of ``func(...)`` inside ``expr``, brace-balanced."""
    start = expr.index("(", expr.index(func))
    depth = 0
    for index in range(start, len(expr)):
        if expr[index] == "(":
            depth += 1
        elif expr[index] == ")":
            depth -= 1
            if depth == 0:
                return expr[start + 1 : index]
    return ""


def produced_keys(expr: str, current: frozenset[str], entry: frozenset[str]) -> set[str]:
    """Top-level field names an intrinsic expression is GUARANTEED to produce."""
    text = expr.strip()
    if text == "$":
        return set(current)
    if text.startswith("$$.Execution.Input"):
        return set(entry)
    if text.startswith("$$"):
        return set()  # context object: not a payload document
    if text.startswith("States.JsonMerge"):
        args = _split_args(_inner(text, "States.JsonMerge"))
        keys: set[str] = set()
        for arg in args[:2]:  # third arg is the deep-merge boolean
            keys |= produced_keys(arg, current, entry)
        return keys
    if text.startswith("States.StringToJson"):
        arg = _inner(text, "States.StringToJson").strip()
        if arg.startswith("'") and arg.endswith("'"):
            try:
                return set(json.loads(arg[1:-1]).keys())
            except (ValueError, AttributeError):
                return set()
        if arg.startswith("States.Format"):
            template = _split_args(_inner(arg, "States.Format"))[0].strip()
            body = template[1:-1].replace("\\{", "{").replace("\\}", "}")
            return {m.group(1) for m in re.finditer(r'"([A-Za-z_][A-Za-z0-9_]*)"\s*:', body)}
        return set()
    return set()  # a JSONPath into a sub-document: opaque, guarantees nothing


def _pass_output(state: dict, current: frozenset[str], entry: frozenset[str]) -> set[str]:
    """Fields guaranteed on the payload leaving a Pass that rewrites its input."""
    params = state.get("Parameters") or {}
    out_path = state.get("OutputPath")
    if isinstance(out_path, str) and out_path.startswith("$."):
        root = out_path[2:].split(".")[0]
        source = params.get(root + ".$")
        if isinstance(source, str):
            return produced_keys(source, current, entry)
        if isinstance(params.get(root), dict):
            return {k[:-2] if k.endswith(".$") else k for k in params[root]}
        return set()
    return {k[:-2] if k.endswith(".$") else k for k in params}


# --------------------------------------------------------------------------- #
# Choice path sensitivity
# --------------------------------------------------------------------------- #
def _rule_facts(rule: dict) -> tuple[set[str], dict[str, bool]]:
    """(fields proven PRESENT, boolean literal facts) established by taking a rule."""
    present: set[str] = set()
    bools: dict[str, bool] = {}
    if "Variable" in rule:
        names = _REF.findall(rule["Variable"])
        if names:
            if rule.get("IsPresent") is True:
                present.add(names[0])
            if isinstance(rule.get("BooleanEquals"), bool):
                present.add(names[0])
                bools[names[0]] = rule["BooleanEquals"]
    for sub in rule.get("And", []):
        sub_present, sub_bools = _rule_facts(sub)
        present |= sub_present
        bools.update(sub_bools)
    # ``Or`` / ``Not`` establish nothing that holds on every satisfying assignment.
    return present, bools


def _rule_is_contradicted(rule: dict, bools: dict[str, bool]) -> bool:
    """True when known boolean facts make this rule unsatisfiable."""
    if "Variable" in rule and isinstance(rule.get("BooleanEquals"), bool):
        names = _REF.findall(rule["Variable"])
        if names and names[0] in bools and bools[names[0]] != rule["BooleanEquals"]:
            return True
    return any(_rule_is_contradicted(sub, bools) for sub in rule.get("And", []))


def _rule_is_entailed(rule: dict, bools: dict[str, bool], present: frozenset[str]) -> bool:
    """True when known facts already satisfy every conjunct of this rule.

    An entailed rule makes the Choice's remaining rules and its ``Default``
    unreachable *on this path*. Modelling that is what stops the weekly SF's
    "skip everything" entry path from appearing to reach the very stages its
    skip flags turn off — the single largest source of false positives before
    it was added.
    """
    if "And" in rule:
        return all(_rule_is_entailed(sub, bools, present) for sub in rule["And"])
    if "Variable" not in rule:
        return False
    names = _REF.findall(rule["Variable"])
    if not names:
        return False
    name = names[0]
    if rule.get("IsPresent") is True:
        return name in present
    if isinstance(rule.get("BooleanEquals"), bool):
        return bools.get(name) == rule["BooleanEquals"]
    return False


# --------------------------------------------------------------------------- #
# The walk
# --------------------------------------------------------------------------- #
def _choice_bool_vars(state: dict) -> set[str]:
    """Boolean variables this state's Choice rules test."""
    def collect(rule: dict) -> set[str]:
        found: set[str] = set()
        if "Variable" in rule and isinstance(rule.get("BooleanEquals"), bool):
            names = _REF.findall(rule["Variable"])
            if names:
                found.add(names[0])
        for key in ("And", "Or"):
            for sub in rule.get(key, []):
                found |= collect(sub)
        if "Not" in rule:
            found |= collect(rule["Not"])
        return found

    found: set[str] = set()
    for rule in state.get("Choices", []):
        found |= collect(rule)
    # A Parallel/Map state consumes facts INSIDE its branches. Without this, the
    # liveness pass drops a skip flag at the Parallel's boundary and the branch is
    # then explored on a path its own gates rule out.
    nested_states: list[dict] = []
    for key in ("ItemProcessor", "Iterator"):
        nested = state.get(key)
        if isinstance(nested, dict) and "States" in nested:
            nested_states.append(nested["States"])
    for branch in state.get("Branches", []) or []:
        nested_states.append(branch["States"])
    for group in nested_states:
        for child in group.values():
            if isinstance(child, dict):
                found |= _choice_bool_vars(child)
    return found


def _edges(state: dict) -> list[str]:
    """Every state name this state can transition to, feasibility ignored."""
    out: list[str] = []
    if "Next" in state:
        out.append(state["Next"])
    out += [r["Next"] for r in state.get("Choices", []) if "Next" in r]
    if "Default" in state:
        out.append(state["Default"])
    out += [c["Next"] for c in state.get("Catch", []) if "Next" in c]
    return out


def live_bool_vars(states: dict) -> dict[str, frozenset[str]]:
    """Boolean variables still readable by some Choice reachable FROM each state.

    A fact is worth carrying only until the last gate that can consult it. Without
    this, the weekly SF's ~22 independent skip flags make the path-sensitive walk
    combinatorial (measured: it blew the 200k context bound in 10s); with it, a
    fact is dropped the moment no reachable Choice can read it, so the paths that
    differed only in a dead flag merge straight back together. Dropping a fact can
    only ever LOSE precision, never soundness — a forgotten fact means an edge is
    explored that ASL might not take, which errs toward reporting, not silence.
    """
    live: dict[str, set[str]] = {name: _choice_bool_vars(state) for name, state in states.items()}
    changed = True
    while changed:
        changed = False
        for name, state in states.items():
            grown = set(live[name])
            for nxt in _edges(state):
                if nxt in live:
                    grown |= live[nxt]
            if grown != live[name]:
                live[name] = grown
                changed = True
    return {name: frozenset(vars_) for name, vars_ in live.items()}


def _result_root(path) -> str | None:
    if isinstance(path, str) and path.startswith("$."):
        return path[2:].split(".")[0]
    return None


def _successors(name: str, state: dict, bools: dict[str, bool], present: frozenset[str]):
    """Yield (next_state, extra_present_fields, extra_bool_facts) per feasible edge."""
    written = _result_root(state.get("ResultPath"))
    extra = {written} if written else set()

    if "Next" in state:
        yield state["Next"], extra, {}

    # ASL evaluates Choices in order and takes the FIRST match, so an entailed
    # rule ends the chain: nothing after it, Default included, is reachable here.
    settled = False
    for rule in state.get("Choices", []):
        if "Next" not in rule:
            continue
        if _rule_is_contradicted(rule, bools):
            continue  # provably unsatisfiable on this path
        rule_present, rule_bools = _rule_facts(rule)
        yield rule["Next"], rule_present, rule_bools
        if _rule_is_entailed(rule, bools, present):
            settled = True
            break
    if "Default" in state and not settled:
        yield state["Default"], set(), {}

    for catch in state.get("Catch", []):
        caught = _result_root(catch.get("ResultPath"))
        yield catch["Next"], ({caught} if caught else set()), {}


def _topological_rank(states: dict, start: str) -> dict[str, int]:
    """Reverse-postorder index of every state reachable from ``start``.

    The walk's worklist pops in this order (alpha-engine-config-I11309). It used
    to pop last-in-first-out, so a state was first processed on whichever path
    arrived first and then re-processed, with its whole downstream re-propagated,
    every time another predecessor narrowed its fields. Popping in reverse
    postorder processes a state after its forward predecessors, so on the
    acyclic bulk of a definition each (state, context) settles in one pass. The
    result is the same fixpoint either way: availability only ever narrows, and
    narrowing only ever adds feasible edges, so the order changes the work done,
    and the arbitrary ``<N>`` index a Parallel branch variant is labelled with,
    but never which fields are available where or which defects are reported.
    """
    order: list[str] = []
    seen = {start}
    stack = [(start, iter(_edges(states[start])))]
    while stack:
        name, edges = stack[-1]
        for nxt in edges:
            if nxt not in seen and nxt in states:
                seen.add(nxt)
                stack.append((nxt, iter(_edges(states[nxt]))))
                break
        else:
            stack.pop()
            order.append(name)
    return {name: index for index, name in enumerate(reversed(order))}


def walk(
    states: dict,
    start: str,
    seed: set[str],
    entry: set[str],
    context: tuple[tuple[str, bool], ...] = (),
) -> dict:
    """Fields guaranteed available at each reachable (state, boolean-context) pair.

    PATH SENSITIVITY IS LOAD-BEARING, not an optimisation. Keying availability on
    the state alone intersects paths that ASL never runs together, and the loss
    then propagates forward: the weekly SF's "skip everything" entry path reaches
    ``CheckShellRun`` without ``ec2_instance_id``, and a state-keyed analysis
    would strip that field from the normal cadence path too, reporting 44
    reachability defects that cannot occur. Keying on the boolean facts known at
    that point keeps the two paths apart, and Choice entailment then prunes the
    skip path before it ever reaches a stage its own flags turned off.
    """
    entry_fs = frozenset(entry)
    live = live_bool_vars(states)
    available: dict[tuple[str, tuple], set[str]] = {}
    # Worklist in reverse postorder, with every not-yet-processed arrival at a
    # (state, context) key folded into ONE pending entry (alpha-engine-config-I11309).
    # Popping last-in-first-out, one entry per arrival, processed a key once per
    # narrowing predecessor and re-propagated its whole downstream each time. Once
    # the rehearsal marker (#2176) doubled the weekly SF's contexts, one weekly
    # analysis took ~160 s locally and the test file runs six, which is what
    # doubled the CI ``Run tests`` step. See _topological_rank for why the order
    # cannot change the result.
    rank = _topological_rank(states, start)
    unranked = len(rank)
    ticket = itertools.count()
    pending: dict[tuple[str, tuple], frozenset[str]] = {}
    queue: list[tuple[int, int, str, tuple[tuple[str, bool], ...]]] = []

    def arrive(name: str, fields: frozenset[str], bool_items: tuple) -> None:
        key = (name, bool_items)
        if key in pending:
            pending[key] = pending[key] & fields
            return
        pending[key] = fields
        heapq.heappush(queue, (rank.get(name, unranked), next(ticket), name, bool_items))

    arrive(
        start,
        frozenset(seed),
        tuple(f for f in context if f[0] in live.get(start, frozenset())),
    )

    while queue:
        if len(available) > _MAX_CONTEXTS:
            raise RuntimeError(
                f"reachability walk exceeded {_MAX_CONTEXTS} (state, context) pairs "
                f"from {start!r} — a definition grew a boolean fan-out this analysis "
                f"cannot enumerate; bound it or narrow the tracked facts"
            )
        _, _, name, bool_items = heapq.heappop(queue)
        key = (name, bool_items)
        incoming = pending.pop(key)
        state = states[name]
        bools = dict(bool_items)

        if key in available:
            merged = available[key] & incoming
            if merged == available[key]:
                continue  # no new constraint on this path; stop
            available[key] = merged
        else:
            available[key] = set(incoming)

        current = frozenset(available[key])
        params = state.get("Parameters")
        out_path = state.get("OutputPath")
        rewrites = state.get("Type") == "Pass" and params is not None and (
            not state.get("ResultPath")
            or (isinstance(out_path, str) and out_path.startswith("$."))
        )
        base = _pass_output(state, current, entry_fs) if rewrites else set(current)

        for nxt, extra, edge_bools in _successors(name, state, bools, current):
            forward = dict(bools)
            forward.update(edge_bools)
            still_live = live.get(nxt, frozenset())
            forward = {k: v for k, v in forward.items() if k in still_live}
            arrive(nxt, frozenset(base | extra), tuple(sorted(forward.items())))

    return available


def collapse(available: dict) -> dict[str, set[str]]:
    """Per-state intersection over its feasible contexts, for reporting only."""
    out: dict[str, set[str]] = {}
    for (name, _context), fields in available.items():
        out[name] = fields if name not in out else out[name] & fields
    return out


def _scopes(states: dict, available: dict, scope: str, entry: set[str]):
    """Yield (scope_label, states, start, seed) for every nested Map/Parallel scope.

    A Parallel branch is seeded ONCE PER DISTINCT payload the Parallel is reached
    with, not with the intersection across all of them. Collapsing first would
    re-import exactly the imprecision the path-sensitive walk exists to avoid —
    the weekly SF reaches ``ResearchPredictorParallel`` with ``ec2_instance_id``
    on every path that actually runs it, and only an intersection with a path
    that skips the whole branch makes it look absent inside.
    """
    # An insertion-ordered dict, not a list (alpha-engine-config-I11309): an
    # ``if pair not in list`` check made this quadratic in a state's context
    # count, which the weekly SF puts near two thousand on many states. (name,
    # context) is already unique in ``available``, so this keeps every pair in
    # first-seen order, exactly as the list did.
    reached: dict[str, dict[tuple[frozenset[str], tuple], None]] = {}
    for (name, context), fields in available.items():
        reached.setdefault(name, {})[(frozenset(fields), context)] = None

    for name, state in states.items():
        if not isinstance(state, dict) or name not in reached:
            continue
        for key in ("ItemProcessor", "Iterator"):
            nested = state.get(key)
            if isinstance(nested, dict) and "States" in nested:
                selector = state.get("ItemSelector") or state.get("Parameters") or {}
                seed = {k[:-2] if k.endswith(".$") else k for k in selector}
                yield f"{scope}/{name}", nested["States"], nested["StartAt"], seed, ()
        variants = list(reached[name])
        for index, branch in enumerate(state.get("Branches", []) or []):
            for variant, (fields, context) in enumerate(variants):
                label = f"{scope}/{name}[{index}]"
                if len(variants) > 1:
                    label = f"{label}<{variant}>"
                yield label, branch["States"], branch["StartAt"], set(fields), context


#: The walk is path-sensitive and the four live definitions take ~30s together.
#: Tests call it repeatedly on the same document, so memoise on the document's
#: exact content — never on identity, which would go stale under an in-memory
#: mutation, and mutating a copy is precisely what the detector self-checks do.
#:
#: The scope is NOT part of the key (alpha-engine-config-I11309): every problem
#: string starts with its label and every label starts with ``scope``, so one
#: unscoped analysis serves every scope by prefixing. Keying on it made
#: ``item_selector_problems`` (scope ``""``) and the per-file test (scope = file
#: name) run the whole weekly analysis twice.
_ANALYSIS_CACHE: dict[tuple[str, frozenset], list[str]] = {}


def analyse(definition: dict, entry: set[str], scope: str) -> list[str]:
    """Every field-reachability problem in a definition, including nested scopes."""
    cache_key = (json.dumps(definition, sort_keys=True), frozenset(entry))
    unscoped = _ANALYSIS_CACHE.get(cache_key)
    if unscoped is None:
        unscoped = _analyse(definition, entry, "")
        _ANALYSIS_CACHE[cache_key] = unscoped
    return [f"{scope}{problem}" for problem in unscoped]


def _analyse(definition: dict, entry: set[str], scope: str) -> list[str]:
    problems: list[str] = []
    pending = [(scope, definition["States"], definition["StartAt"], set(entry), ())]

    while pending:
        label, states, start, seed, context = pending.pop()
        available = walk(states, start, seed, entry, context)
        seen: set[tuple[str, tuple[str, ...]]] = set()
        for (name, _context), fields in sorted(available.items()):
            missing = state_refs(states[name]) - fields
            if not missing:
                continue
            signature = (name, tuple(sorted(missing)))
            if signature in seen:
                continue  # same defect, another context reaching it
            seen.add(signature)
            problems.append(
                f"{label}/{name} references {sorted(missing)} which no predecessor "
                f"produces on every reaching path (available: {sorted(fields)})"
            )
        problems.extend(_item_selector_problems_in(states, available, label))
        pending.extend(_scopes(states, available, label, entry))

    return problems


def _item_selector_problems_in(states: dict, available: dict, scope: str) -> list[str]:
    """``ItemSelector`` is evaluated at the Map's INPUT, not inside an iteration.

    Checked inside ``analyse``'s scope walk rather than only at the top level:
    ``ModelZooTrainMap`` — the Map that caused #9077 — lives inside a Parallel
    branch, so a top-level-only sweep is blind to the one defect this guard was
    written for. That blindness was itself caught by the detector
    self-verification below, which is what those tests are for.
    """
    reached = collapse(available)
    problems: list[str] = []
    for name, state in states.items():
        if not isinstance(state, dict) or state.get("Type") != "Map":
            continue
        at_map = reached.get(name)
        if at_map is None:
            continue
        missing = refs(state.get("ItemSelector") or {}) - at_map
        if missing:
            problems.append(
                f"{scope}/{name}.ItemSelector references {sorted(missing)} at the "
                f"Map's input level, where only {sorted(at_map)} exist"
            )
    return problems


def item_selector_problems(definition: dict, entry: set[str]) -> list[str]:
    """Every Map ``ItemSelector`` problem, at any nesting depth.

    Kept as a named entry point because it answers a different question from
    ``analyse``: not "can this state read what it reads" but "is this Map's fan-out
    payload complete". ``analyse`` reports both, so callers wanting only the
    reachability half can ignore this one.
    """
    return [p for p in analyse(definition, entry, "") if ".ItemSelector references" in p]
