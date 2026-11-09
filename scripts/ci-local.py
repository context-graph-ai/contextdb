#!/usr/bin/env python3
"""Run the pull-request workflow on this machine, each job in a box shaped like a hosted runner.

    python3 scripts/ci-local.py [--tree DIR] [--work DIR] [--tmp DIR] [--out DIR] [--boxes N]
                                [--only REGEX] [--base SHA] [--path-prepend DIR ...]

The job graph, matrices, `if:` conditions, `env:` blocks, outputs and `run:` steps are read from
.github/workflows/ci.yml at run time and executed the way a GitHub-hosted runner executes them for a
pull_request event: jobs start when their `needs` finish, matrix legs run concurrently, step
outputs, GITHUB_ENV and GITHUB_PATH flow between steps, `continue-on-error` and step outcomes behave
as on GitHub. A workflow construct this script does not model (an unknown action, expression
function or context) stops the run with exit 3 instead of silently running less than GitHub does.

Each job runs in its own box:
  * a fresh clone of the tree at HEAD (tracked uncommitted changes are applied on top), with the
    Cargo target inside it, as on a hosted runner (CARGO_TARGET_DIR is not set);
  * test binaries compiled by a shared-build job name that job's checkout (CARGO_MANIFEST_DIR,
    CARGO_TARGET_TMPDIR); on GitHub every job's checkout is at that same path. Here boxes cannot
    share one path without root (a user namespace shows root-owned paths as uid 65534, which the
    engine refuses), so a box that packed a shared build keeps its checkout until the run ends and
    those paths keep resolving to the same sources. Residual difference: a shared-build test that
    writes under CARGO_TARGET_TMPDIR writes into that one build checkout for every job using it;
  * every step is one transient `systemd-run --user` service on 4 whole CPUs of the set the
    build-host admission chose, held by no other box step; a step waits until four are free
    (--cpu-model cores; `quota` instead
    spreads CPUQuota=400% over that set, which stalls bursts for most of each 100 ms period),
    MemoryMax=16G, MemorySwapMax=0 and RuntimeMaxSec set to what is left of the job's
    timeout-minutes, so a step that would be OOM-killed or time out on GitHub is killed here
    (OOMPolicy=continue: the kernel kills the offending process and the step carries on, as on
    the hosted VM; the kill is reported as the job's cause);
  * the disk budget is the free space a hosted runner reports after the workflow's cleanup step
    (minus the toolchain and crate sources it then installs), enforced by measuring the box's
    usage and killing the step at the limit (an exact quota would need root);
  * RUNNER_TEMP and TMPDIR are on disk under --tmp, CI=true, RUNNER_OS=Linux and the GitHub
    variables are set, and the environment is otherwise clean.
Boxes share one parent slice capped below the host's available memory. A step killed while that
parent slice hit its own cap is reported INCONCLUSIVE, never as the job's verdict.

Artifacts are kept by name under --work, so the shared-build jobs pack their target once and the
test jobs restore it, as on GitHub. Hosted chores that need root (`sudo`, apt) are skipped and
listed; the tools they install must already be on PATH. The build cache action restores nothing
here, so a job GitHub serves from a warm cache compiles cold here.

When `build-any` is on PATH every box step is admitted through it (the shared build-host
contract), and the box keeps the CPU set that admission chose.

Output in --out: jobs.tsv (job, name, result, cause, minutes in the box, peak memory, peak disk),
steps.tsv, logs/, not-run.tsv and result.txt. Exit status 0 when the final required job passed,
1 when it did not, 3 on workflow drift, 130 when stopped by SIGINT/SIGTERM (the report is still
written; unfinished jobs are marked interrupted).
"""
import argparse
import fnmatch
import hashlib
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import yaml

WORKFLOW = '.github/workflows/ci.yml'
BOX_CPUS = 4
BOX_CPU_QUOTA = '400%'
BOX_MEMORY = 16 * 1024 ** 3
# Free bytes the workflow's cleanup step reported on hosted ubuntu-latest runners (November 2026,
# cleanup skipped because at least NORMAL_CLEANUP_MIN_FREE_BYTES were free).
HOSTED_FREE_AFTER_CLEANUP = 92_027_510_784
DISK_POLL_SECONDS = 20
SLICE = 'cilocal.slice'
KNOWN_ACTIONS = ('actions/checkout', 'actions-rust-lang/setup-rust-toolchain', 'Swatinem/rust-cache',
                 'actions/upload-artifact', 'actions/download-artifact')
# Hosted-runner chores that need root; the tools they install must already be present here.
ROOT_CHORE = re.compile(r'\bsudo\b|\bapt-get\b')
# Commands that never compile; every other step is admitted through the build-host wrapper.
NO_COMPILE = re.compile(r'^\s*(python3 (-B )?scripts/ci-(inventory|impact|required|integration\.py (plan|normal-required))'
                        r'|bash scripts/ci-(free-disk|shared-build|cache-post-cleanup)\.sh|cat > )', re.M)


class Drift(Exception):
    """A workflow construct this script does not model."""


# ------------------------------------------------------------------------------------------------
# Expressions (${{ }}): the GitHub Actions expression language subset the workflow uses.
# ------------------------------------------------------------------------------------------------
TOKEN = re.compile(r"\s*(?:(?P<str>'(?:[^']|'')*')|(?P<num>\d+(?:\.\d+)?)|(?P<op>&&|\|\||==|!=|<=|>=|<|>|!|\(|\)|,)"
                   r"|(?P<id>[A-Za-z_][A-Za-z0-9_\-]*(?:\.[A-Za-z0-9_\-*]+|\[[^\]]+\])*))")
CONTEXTS = {'github', 'env', 'runner', 'matrix', 'needs', 'steps', 'job', 'strategy', 'inputs', 'secrets', 'vars'}


def truthy(v):
    if isinstance(v, float) and math.isnan(v):
        return False
    return not (v is None or v is False or v == '' or v == 0)


def to_str(v):
    if v is None:
        return ''
    if v is True:
        return 'true'
    if v is False:
        return 'false'
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, (dict, list)):
        return json.dumps(v, indent=2)
    return str(v)


def to_num(v):
    if v is None or v is False:
        return 0
    if v is True:
        return 1
    if isinstance(v, (int, float)):
        return v
    if isinstance(v, str):
        s = v.strip()
        if s == '':
            return 0
        try:
            return float(s)
        except ValueError:
            return float('nan')
    return float('nan')


def loose_eq(a, b):
    if isinstance(a, str) and isinstance(b, str):
        return a.lower() == b.lower()
    if type(a) is type(b) and not isinstance(a, (dict, list)):
        return a == b
    if isinstance(a, (dict, list)) or isinstance(b, (dict, list)):
        return a is b
    x, y = to_num(a), to_num(b)
    return not (math.isnan(x) or math.isnan(y)) and x == y


class Expr:
    def __init__(self, text, ctx):
        self.text, self.ctx, self.toks, self.i = text, ctx, [], 0
        text, pos = text.strip(), 0
        while pos < len(text):
            m = TOKEN.match(text, pos)
            if not m or m.end() == pos:
                raise Drift(f'cannot read expression {text!r} at {pos}')
            pos = m.end()
            self.toks.append((m.lastgroup, m.group(m.lastgroup)))

    def peek(self):
        return self.toks[self.i] if self.i < len(self.toks) else (None, None)

    def eat(self, val=None):
        t = self.peek()
        if val is not None and t[1] != val:
            raise Drift(f'expected {val!r} in {self.text!r}')
        self.i += 1
        return t

    def parse(self):
        v = self.or_()
        if self.i != len(self.toks):
            raise Drift(f'trailing tokens in {self.text!r}')
        return v

    def or_(self):
        v = self.and_()
        while self.peek() == ('op', '||'):
            self.eat()
            r = self.and_()
            v = v if truthy(v) else r
        return v

    def and_(self):
        v = self.cmp()
        while self.peek() == ('op', '&&'):
            self.eat()
            r = self.cmp()
            v = r if truthy(v) else v
        return v

    def cmp(self):
        v = self.unary()
        while self.peek()[1] in ('==', '!=', '<', '>', '<=', '>='):
            op = self.eat()[1]
            r = self.unary()
            if op in ('==', '!='):
                v = loose_eq(v, r) == (op == '==')
            else:
                x, y = to_num(v), to_num(r)
                v = {'<': x < y, '>': x > y, '<=': x <= y, '>=': x >= y}[op]
        return v

    def unary(self):
        if self.peek() == ('op', '!'):
            self.eat()
            return not truthy(self.unary())
        return self.atom()

    def atom(self):
        kind, val = self.peek()
        if (kind, val) == ('op', '('):
            self.eat()
            v = self.or_()
            self.eat(')')
            return v
        if kind == 'str':
            self.eat()
            return val[1:-1].replace("''", "'")
        if kind == 'num':
            self.eat()
            return float(val) if '.' in val else int(val)
        if kind == 'id':
            self.eat()
            if self.peek() == ('op', '('):
                self.eat()
                args = []
                if self.peek() != ('op', ')'):
                    args.append(self.or_())
                    while self.peek() == ('op', ','):
                        self.eat()
                        args.append(self.or_())
                self.eat(')')
                return self.call(val, args)
            return self.lookup(val)
        raise Drift(f'unsupported expression syntax {self.text!r}')

    def call(self, name, a):
        n, status = name.lower(), self.ctx.get('_status', {})
        if n == 'always':
            return True
        if n == 'cancelled':
            return status.get('cancelled', False)
        if n == 'success':
            return status.get('success', True)
        if n == 'failure':
            return status.get('failure', False)
        if n == 'contains':
            if isinstance(a[0], list):
                return any(loose_eq(x, a[1]) for x in a[0])
            return to_str(a[1]).lower() in to_str(a[0]).lower()
        if n == 'startswith':
            return to_str(a[0]).lower().startswith(to_str(a[1]).lower())
        if n == 'endswith':
            return to_str(a[0]).lower().endswith(to_str(a[1]).lower())
        if n == 'tojson':
            return json.dumps(a[0], indent=2)
        if n == 'fromjson':
            return json.loads(a[0]) if isinstance(a[0], str) else a[0]
        if n == 'format':
            out = to_str(a[0])
            for k, v in enumerate(a[1:]):
                out = out.replace('{%d}' % k, to_str(v))
            return out
        if n == 'hashfiles':
            return hash_files(self.ctx['_workspace'], [to_str(x) for x in a])
        raise Drift(f'unsupported expression function {name}() in {self.text!r}')

    def lookup(self, dotted):
        if dotted in ('true', 'false', 'null'):
            return {'true': True, 'false': False, 'null': None}[dotted]
        parts = re.findall(r"[A-Za-z0-9_\-*]+|\[[^\]]+\]", dotted)
        if parts[0] not in CONTEXTS:
            raise Drift(f'unknown expression context {dotted!r} in {self.text!r}')
        cur = self.ctx.get(parts[0], {})
        for p in parts[1:]:
            if p.startswith('['):
                p = p[1:-1].strip("'\"")
            cur = cur.get(p) if isinstance(cur, dict) else None
            if cur is None:
                return None
        return cur


INTERP = re.compile(r'\$\{\{((?:(?!\}\}).)*)\}\}', re.S)


def interp(value, ctx):
    """Evaluate ${{ }} in a value; a string that is exactly one expression keeps the result's type."""
    if isinstance(value, dict):
        return {k: interp(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [interp(v, ctx) for v in value]
    if not isinstance(value, str):
        return value
    m = INTERP.fullmatch(value.strip())
    if m:
        return Expr(m.group(1), ctx).parse()
    return INTERP.sub(lambda m: to_str(Expr(m.group(1), ctx).parse()), value)


def condition(value, ctx):
    if value is None:
        return truthy(Expr('success()', ctx).parse())
    if isinstance(value, bool):
        return value
    text = str(value).strip()
    m = INTERP.fullmatch(text)
    expr = m.group(1) if m else text
    if not re.search(r'\b(always|failure|cancelled|success)\s*\(', expr):
        expr = f'success() && ({expr})'
    return truthy(Expr(expr, ctx).parse())


def hash_files(root, patterns):
    files = sorted({p for pat in patterns for p in Path(root).glob(pat) if p.is_file()})
    if not files:
        return ''
    outer = hashlib.sha256()
    for f in files:
        outer.update(hashlib.sha256(f.read_bytes()).digest())
    return outer.hexdigest()


def parse_kv(text):
    out, lines, i = {}, text.split('\n'), 0
    while i < len(lines):
        line = lines[i]
        m = re.match(r'^([^=<]+)<<(\S+)$', line)
        if m:
            key, delim, body = m.group(1), m.group(2), []
            i += 1
            while i < len(lines) and lines[i] != delim:
                body.append(lines[i])
                i += 1
            out[key] = '\n'.join(body)
        elif '=' in line:
            k, v = line.split('=', 1)
            out[k] = v
        i += 1
    return out


def link_or_copy(src, dst):
    try:
        os.link(src, dst)
    except OSError:
        shutil.copy2(src, dst)


def du_bytes(*paths):
    have = [str(p) for p in paths if Path(p).exists()]
    if not have:
        return 0
    r = subprocess.run(['du', '-sxc', '--block-size=1', *have], capture_output=True, text=True)
    last = r.stdout.strip().splitlines()[-1:] or ['0']
    return int(last[0].split()[0])


def human(n):
    for unit in ('B', 'K', 'M', 'G', 'T'):
        if abs(n) < 1024 or unit == 'T':
            return f'{n:.1f}{unit}' if unit != 'B' else f'{n}B'
        n /= 1024


def parse_size(text):
    m = re.match(r'([\d.]+)([BKMGT]?)', text)
    if not m:
        return 0
    return int(float(m.group(1)) * 1024 ** 'BKMGT'.index(m.group(2) or 'B'))


ERROR_LINE = re.compile(r'^(error(\[[A-Za-z0-9]+\])?:|Error:|FAIL|FAILED|Diff in |thread .* panicked|ERROR:|fatal:|.*: command not found)', re.I)


def first_error(log):
    try:
        with open(log, errors='replace') as f:
            for line in f:
                s = re.sub(r'\x1b\[[0-9;]*m', '', line.rstrip('\n'))
                if ERROR_LINE.match(s):
                    return s[:200]
    except OSError:
        pass
    return ''


# ------------------------------------------------------------------------------------------------
class Box:
    """One job instance: its clone, temp dirs, state and step loop."""

    def __init__(self, run, jobid, job, matrix, index):
        self.run, self.jobid, self.job, self.matrix = run, jobid, job, matrix
        label = jobid + (f'-{index + 1:02d}' if run.matrix_size(job) > 1 else '')
        self.slug = re.sub(r'[^A-Za-z0-9_.-]+', '-', label)
        self.root = run.work / self.slug
        self.ws = self.root / 'src'
        self.tmp = run.tmp / self.slug
        self.runner_temp = self.tmp / 'runner-temp'
        self.env, self.path, self.steps = {}, [], {}
        self.failed = self.cancelled = False
        self.cause, self.used, self.peak_mem, self.peak_disk, self.base_disk = '', 0.0, 0, 0, 0
        self.unit, self.disk_killed, self.inconclusive, self.keep = None, False, False, False
        self.done = threading.Event()
        self.ctx = run.context(self)
        self.name = self.display_name()
        self.timeout = float(interp(job.get('timeout-minutes', 360), self.ctx)) * 60

    def display_name(self):
        if 'name' in self.job:
            return to_str(interp(self.job['name'], self.ctx))
        if self.matrix:
            vals = [to_str(v) for v in self.matrix.values() if not isinstance(v, (dict, list))]
            return f'{self.jobid} ({", ".join(vals)})'
        return self.jobid

    def status(self):
        if self.cancelled:
            return 'cancelled'
        return 'failure' if self.failed else 'success'

    def merged_env(self):
        return dict(self.run.wf_env(self.ctx), **self.job_env, **self.env)

    def execute(self):
        try:
            self.job_env = {k: to_str(interp(v, self.ctx)) for k, v in (self.job.get('env') or {}).items()}
            for n, step in enumerate(self.job.get('steps') or []):
                if self.cancelled:
                    self.run.not_run(self, step, 'job cancelled at its timeout')
                    continue
                self.step(n, step)
        except Drift as e:
            self.run.drift = str(e)
            self.failed, self.cause = True, 'drift'
        except Exception as e:  # report and fail this job; never hang the scheduler
            self.failed, self.cause = True, f'runner error: {e!r}'
        finally:
            self.done.set()

    def step(self, n, step):
        ctx = self.ctx
        ctx['env'] = self.merged_env()
        ctx['job'] = {'status': self.status()}
        ctx['_status'] = {'success': not self.failed, 'failure': self.failed, 'cancelled': self.cancelled}
        name = to_str(interp(step.get('name') or step.get('uses') or (step.get('run') or '').strip().splitlines()[0][:60], ctx))
        sid = step.get('id')
        if not condition(step.get('if'), ctx):
            self.run.not_run(self, step, f'if: {step.get("if", "success()")} is false', name)
            if sid:
                self.steps[sid] = {'outputs': {}, 'outcome': 'skipped', 'conclusion': 'skipped'}
            return
        cont = truthy(interp(step.get('continue-on-error', False), ctx))
        if step.get('uses'):
            ok = self.action(step, name)
        else:
            run = interp(step['run'], ctx)
            if ROOT_CHORE.search(run):
                self.run.not_run(self, step, 'needs root on a hosted runner; its tools must already be on PATH', name)
                return
            ok = self.shell(n, name, run, step)
        if sid:
            outcome = 'success' if ok else 'failure'
            self.steps.setdefault(sid, {'outputs': {}})
            self.steps[sid].update(outcome=outcome, conclusion='success' if cont else outcome)
        if not ok and not cont:
            self.failed = True

    # -- actions
    def action(self, step, name):
        uses = step['uses']
        if not uses.startswith(KNOWN_ACTIONS):
            raise Drift(f'unsupported action {uses!r} in {self.jobid}')
        w = {k: interp(v, self.ctx) for k, v in (step.get('with') or {}).items()}
        action = uses.split('@')[0]
        if action == 'actions/checkout':
            for key in w:
                if key not in ('fetch-depth',):
                    raise Drift(f'checkout input {key!r} is not modelled')
            t0 = time.time()
            ok = self.run.checkout(self)
            self.used += time.time() - t0
            self.base_disk = du_bytes(self.root, self.tmp)
            return ok
        if action == 'actions-rust-lang/setup-rust-toolchain':
            self.env.update(CARGO_INCREMENTAL='0', RUST_BACKTRACE='short', CARGO_TERM_COLOR='always',
                            RUSTFLAGS=to_str(w['rustflags']) if 'rustflags' in w else '-D warnings')
            target = to_str(w.get('target') or '')
            if target:
                have = subprocess.run(['rustup', 'target', 'list', '--installed'], cwd=self.ws,
                                      capture_output=True, text=True).stdout.split()
                if target not in have:
                    self.run.note(self, name, f'Rust target {target} is not installed (rustup target add {target})')
                    return False
            return True
        if action == 'Swatinem/rust-cache':
            self.run.not_run(self, step, 'build cache: nothing is restored here (a warm hosted cache is not reproduced)', name)
            return True
        if action == 'actions/upload-artifact':
            return self.run.upload(self, to_str(w.get('name')), to_str(w.get('path')),
                                   to_str(w.get('if-no-files-found') or 'warn'), name)
        if action == 'actions/download-artifact':
            return self.run.download(self, to_str(w.get('name') or ''), to_str(w.get('pattern') or ''),
                                     to_str(w.get('path') or '.'), name)
        raise Drift(f'unsupported action {uses!r}')

    # -- run: steps
    def shell(self, n, name, run, step):
        r = self.run
        shell = step.get('shell') or (self.job.get('defaults') or {}).get('run', {}).get('shell') or r.default_shell
        if shell != 'bash':
            raise Drift(f'shell {shell!r} is not modelled')
        wd = step.get('working-directory') or (self.job.get('defaults') or {}).get('run', {}).get('working-directory')
        cwd = self.ws / interp(wd, self.ctx) if wd else self.ws
        seq = r.next_seq()
        tag = f'{seq:04d}-{self.slug}-{re.sub(r"[^A-Za-z0-9_.-]+", "-", name)[:40].strip("-")}'
        files = {k: r.out / 'scripts' / f'{tag}.{k}' for k in ('sh', 'env', 'output', 'genv', 'path', 'summary', 'start')}
        for k in ('output', 'genv', 'path', 'summary'):
            files[k].write_text('')
        files['sh'].write_text(run if run.endswith('\n') else run + '\n')
        env = r.base_env(self)
        env.update({k: to_str(v) for k, v in self.merged_env().items()})
        for k, v in (step.get('env') or {}).items():
            env[k] = to_str(interp(v, self.ctx))
        env.update(GITHUB_OUTPUT=str(files['output']), GITHUB_ENV=str(files['genv']), GITHUB_PATH=str(files['path']),
                   GITHUB_STEP_SUMMARY=str(files['summary']))
        files['env'].write_bytes(b''.join(f'{k}={v}'.encode() + b'\0' for k, v in env.items()))
        remaining = int(self.timeout - self.used)
        log = r.out / 'logs' / f'{tag}.log'
        if remaining <= 0:
            self.cancelled, self.cause = True, 'timeout'
            log.write_text('job timeout reached before this step\n')
            return False
        self.unit = f'cilocal-{r.run_id}-{seq:04d}.service'
        argv = [sys.executable, str(Path(__file__).resolve()), '_box', '--unit', self.unit, '--runtime', str(remaining),
                '--cwd', str(cwd), '--cpu-model', r.a.cpu_model, '--cpu-pool', str(r.work / 'cpu-pool.json'),
                '--env-file', str(files['env']), '--stamp', str(files['start']), str(files['sh'])]
        compile_step = not NO_COMPILE.search(run)
        if compile_step and r.launcher:
            argv = [r.launcher] + argv
        oom_before = r.slice_oom()
        r.say(f'[run ] {tag}')
        t0 = time.time()
        with open(log, 'w') as lf:
            lf.write(f'$ cwd={cwd}\n--- script ---\n{run}\n--- output ---\n')
            lf.flush()
            rc = subprocess.run(argv, cwd=cwd, stdout=lf, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL).returncode
        end = time.time()
        stamp = files['start'].read_text().strip() if files['start'].exists() else ''
        secs = end - float(stamp) if stamp else 0.0
        self.used += secs
        unit, self.unit = self.unit, None
        text = log.read_text(errors='replace')
        result = (re.findall(r'Finished with result: (\S+)', text) or ['success' if rc == 0 else 'exit-code'])[-1]
        peak = re.findall(r'Memory peak: (\S+)', text)
        cpu = re.findall(r'CPU time consumed: (.*)', text)
        cpu_secs = sum(float(v) * {'h': 3600, 'min': 60, 's': 1, 'ms': 0.001}[u]
                       for v, u in re.findall(r'([\d.]+)(min|ms|s|h)', cpu[-1])) if cpu else 0.0
        mem = parse_size(peak[-1]) if peak else 0
        self.peak_mem = max(self.peak_mem, mem)
        oom_after = r.slice_oom()
        cause = ''
        if self.disk_killed:
            cause = 'disk budget exceeded'
            self.disk_killed = False
        elif result == 'oom-kill' or r.unit_oom_killed(unit):
            if oom_after > oom_before:
                cause, self.inconclusive = 'oom-kill while the shared parent slice was at its cap (INCONCLUSIVE)', True
            else:
                cause = 'oom-kill at the 16G box limit'
        elif result == 'timeout':
            cause = 'job timeout'
            self.cancelled = True
        if cause and not self.cause:
            self.cause = cause
        out = parse_kv(files['output'].read_text())
        sid = step.get('id')
        if sid:
            self.steps[sid] = {'outputs': out}
        self.env.update(parse_kv(files['genv'].read_text()))
        self.path += [p for p in files['path'].read_text().split('\n') if p]
        ok = rc == 0 and result == 'success'
        r.record_step(self, tag, name, 'PASS' if ok else 'FAIL', rc, result, secs, end - t0 - secs, cpu_secs, mem, log,
                      '' if ok else (cause or first_error(log)))
        return ok


# ------------------------------------------------------------------------------------------------
class LocalRun:
    def __init__(self, a):
        self.a = a
        self.tree = Path(a.tree).resolve()
        self.sha = self.git('rev-parse', 'HEAD')
        self.run_id = time.strftime('%Y%m%d%H%M%S')
        self.work = Path(a.work).resolve() / self.run_id
        self.tmp = Path(a.tmp).resolve() / self.run_id
        self.out = Path(a.out).resolve() if a.out else self.work / 'report'
        self.base = a.base or self.git('merge-base', 'HEAD', 'origin/dev') or self.git('rev-parse', 'HEAD^')
        self.launcher = shutil.which(a.launcher) if a.launcher else None
        self.only = re.compile(a.only) if a.only else None
        self.lock = threading.Lock()
        self.seq = 0
        self.steps_rows, self.not_run_rows = [], []
        self.needs = {}
        self.boxes = {}
        self.drift = None
        self.interrupted = False
        self.t0 = time.time()
        self.artifacts = self.work / 'artifacts'
        self.doc = yaml.safe_load((self.tree / WORKFLOW).read_text())
        self.default_shell = ((self.doc.get('defaults') or {}).get('run') or {}).get('shell', 'bash')

    def git(self, *args, cwd=None):
        r = subprocess.run(['git', *args], cwd=cwd or self.tree, capture_output=True, text=True)
        return r.stdout.strip() if r.returncode == 0 else ''

    def say(self, msg):
        with self.lock:
            print(msg, flush=True)

    def next_seq(self):
        with self.lock:
            self.seq += 1
            return self.seq

    def matrix_size(self, job):
        return len(job.get('_combos') or [None])

    # -- contexts and environment
    def context(self, box):
        event = {'pull_request': {'number': 0, 'base': {'sha': self.base, 'ref': 'dev'},
                                  'head': {'sha': self.sha, 'ref': 'local'}}, 'before': self.base, 'after': self.sha}
        declared = box.job.get('needs') or []
        declared = [declared] if isinstance(declared, str) else declared
        return {'github': {'sha': self.sha, 'event_name': 'pull_request', 'event': event, 'ref': 'refs/pull/0/merge',
                           'base_ref': 'dev', 'head_ref': 'local', 'repository': 'context-graph-ai/contextdb',
                           'run_id': self.run_id, 'run_attempt': '1', 'workspace': str(box.ws), 'job': box.jobid},
                'runner': {'os': 'Linux', 'arch': 'X64', 'temp': str(box.runner_temp), 'environment': 'self-hosted'},
                'matrix': box.matrix, 'needs': {n: self.needs[n] for n in declared if n in self.needs},
                'steps': box.steps, 'strategy': {'fail-fast': False}, 'inputs': {}, 'secrets': {}, 'vars': {},
                'env': {}, 'job': {'status': 'success'}, '_workspace': str(box.ws)}

    def wf_env(self, ctx):
        return {k: to_str(interp(v, ctx)) for k, v in (self.doc.get('env') or {}).items()}

    def base_env(self, box):
        keep = ('HOME', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'SHELL', 'CARGO_HOME', 'RUSTUP_HOME')
        env = {k: os.environ[k] for k in keep if k in os.environ}
        path = [*self.a.path_prepend, *box.path, str(Path.home() / '.cargo/bin'), os.environ.get('PATH', '/usr/bin:/bin')]
        env['PATH'] = os.pathsep.join(path)
        event_file = box.runner_temp.parent / 'event.json'
        if not event_file.exists():
            event_file.write_text(json.dumps(box.ctx['github']['event']))
        env.update(CI='true', GITHUB_ACTIONS='true', GITHUB_SHA=self.sha, GITHUB_REF='refs/pull/0/merge',
                   GITHUB_EVENT_NAME='pull_request', GITHUB_EVENT_PATH=str(event_file), GITHUB_WORKSPACE=str(box.ws),
                   GITHUB_REPOSITORY='context-graph-ai/contextdb', GITHUB_RUN_ID=self.run_id, GITHUB_RUN_ATTEMPT='1',
                   GITHUB_JOB=box.jobid, GITHUB_BASE_REF='dev', GITHUB_HEAD_REF='local', RUNNER_OS='Linux',
                   RUNNER_ARCH='X64', RUNNER_ENVIRONMENT='self-hosted', RUNNER_TEMP=str(box.runner_temp),
                   TMPDIR=str(box.tmp / 'tmp'))
        return env

    # -- checkout and artifacts
    def checkout(self, box):
        box.root.mkdir(parents=True, exist_ok=True)
        for d in (box.runner_temp, box.tmp / 'tmp'):
            d.mkdir(parents=True, exist_ok=True)
        log = self.out / 'logs' / f'{self.next_seq():04d}-{box.slug}-checkout.log'
        with open(log, 'w') as lf:
            steps = [['git', 'clone', '--quiet', '--shared', '--no-checkout', str(self.tree), str(box.ws)],
                     ['git', '-C', str(box.ws), 'checkout', '--quiet', '--detach', self.sha]]
            for argv in steps:
                if subprocess.run(argv, stdout=lf, stderr=subprocess.STDOUT).returncode:
                    return False
            diff = subprocess.run(['git', '-C', str(self.tree), 'diff', 'HEAD', '--binary'], capture_output=True).stdout
            if diff:
                lf.write('applying tracked uncommitted changes of the tree\n')
                lf.flush()
                if subprocess.run(['git', '-C', str(box.ws), 'apply', '--whitespace=nowarn'], input=diff,
                                  stdout=lf, stderr=subprocess.STDOUT).returncode:
                    return False
        return True

    def upload(self, box, name, path, if_missing, step_name):
        src = Path(path) if Path(path).is_absolute() else box.ws / path
        dst = self.artifacts / name
        if not src.exists() or (src.is_dir() and not any(src.iterdir())):
            self.note(box, step_name, f'artifact {name}: nothing at {src}')
            return if_missing != 'error'
        with self.lock:
            if dst.exists():
                shutil.rmtree(dst)
            dst.mkdir(parents=True)
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True, copy_function=link_or_copy)
        else:
            link_or_copy(src, dst / src.name)
            box.keep = True   # a packed build: its compiled paths name this checkout
        self.note(box, step_name, f'artifact {name} <- {src}')
        return True

    def download(self, box, name, pattern, path, step_name):
        dest = Path(path) if Path(path).is_absolute() else box.ws / path
        names = [name] if name else sorted(p.name for p in self.artifacts.glob('*') if fnmatch.fnmatch(p.name, pattern or '*'))
        if name and not (self.artifacts / name).is_dir():
            self.note(box, step_name, f'artifact {name} not found')
            return False
        for nm in names:
            target = dest if name else dest / nm
            target.mkdir(parents=True, exist_ok=True)
            shutil.copytree(self.artifacts / nm, target, dirs_exist_ok=True, copy_function=link_or_copy)
        self.note(box, step_name, f'artifacts {names} -> {dest}')
        return True

    # -- records
    def note(self, box, step, reason):
        with self.lock:
            self.not_run_rows.append((box.name, step, reason))

    def not_run(self, box, step, reason, name=None):
        nm = name or to_str(step.get('name') or step.get('uses') or (step.get('run') or '').strip()[:60])
        self.note(box, nm, reason)

    def record_step(self, box, tag, name, status, rc, result, secs, wait, cpu, mem, log, err):
        with self.lock:
            self.steps_rows.append((tag, box.name, name, status, rc, result, f'{secs:.1f}', f'{wait:.1f}', f'{cpu:.1f}',
                                    mem, str(log), err))
        self.say(f'[{status.lower()}] {tag} {result} {secs:.0f}s' + (f' | {err}' if err else ''))

    # -- host
    def slice_cgroup(self):
        cg = subprocess.run(['systemctl', '--user', 'show', SLICE, '-p', 'ControlGroup', '--value'],
                            capture_output=True, text=True).stdout.strip()
        return Path('/sys/fs/cgroup' + cg) if cg else None

    def slice_oom(self):
        cg = self.slice_cgroup()
        try:
            text = (cg / 'memory.events.local').read_text() if cg else ''
        except OSError:
            return 0
        m = re.search(r'^oom (\d+)', text, re.M)
        return int(m.group(1)) if m else 0

    @staticmethod
    def unit_oom_killed(unit):
        # OOMPolicy=continue kills only the offending process, as the kernel does on a hosted VM; the
        # user journal records that the unit lost a process to the OOM killer.
        text = subprocess.run(['journalctl', '--user', '-u', unit, '-o', 'cat', '--no-pager'],
                              capture_output=True, text=True).stdout
        return 'OOM killer killed' in text

    def disk_budget(self):
        if self.a.disk_budget:
            return self.a.disk_budget, 'given'
        sysroot = subprocess.run(['rustc', '--print', 'sysroot'], cwd=self.tree, capture_output=True, text=True).stdout.strip()
        toolchain = du_bytes(sysroot) if sysroot else 0
        registry = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo')) / 'registry'
        lock = (self.tree / 'Cargo.lock').read_text()
        pkgs = re.findall(r'name = "([^"]+)"\nversion = "([^"]+)"\nsource = "registry', lock)
        dirs = [d for n, v in pkgs for d in registry.glob(f'src/*/{n}-{v}')]
        dirs += [d for n, v in pkgs for d in registry.glob(f'cache/*/{n}-{v}.crate')]
        crates = du_bytes(*dirs) if dirs else 0
        return HOSTED_FREE_AFTER_CLEANUP - toolchain - crates, (
            f'{HOSTED_FREE_AFTER_CLEANUP} hosted free after cleanup - {toolchain} toolchain - {crates} crate sources')

    def watch_disk(self, stop):
        while not stop.wait(DISK_POLL_SECONDS):
            for box in list(self.boxes_running()):
                used = du_bytes(box.root, box.tmp) - box.base_disk
                box.peak_disk = max(box.peak_disk, used)
                unit = box.unit
                if used > self.budget and unit and not box.disk_killed:
                    box.disk_killed = True
                    subprocess.run(['systemctl', '--user', 'kill', '--signal=SIGKILL', unit], capture_output=True)
                    self.say(f'[disk] {box.name}: {human(used)} over the {human(self.budget)} budget; step killed')

    def boxes_running(self):
        for boxes in self.boxes.values():
            for b in boxes:
                if b.started and not b.done.is_set():
                    yield b

    def stop_all(self, *_):
        self.interrupted = True
        subprocess.run(['systemctl', '--user', 'kill', '--signal=SIGKILL', f'cilocal-{self.run_id}-*'], capture_output=True)
        subprocess.run(['systemctl', '--user', 'stop', f'cilocal-{self.run_id}-*'], capture_output=True)
        try:   # what finished stays reported; the rest is marked interrupted
            self.report(time.time() - self.t0)
        finally:
            os._exit(130)

    # -- scheduling
    def expand(self, jobid, job):
        ctx = {'github': {'event_name': 'pull_request'}, 'needs': self.needs, 'env': {}, 'matrix': {}, 'steps': {},
               'runner': {}, 'inputs': {}, 'secrets': {}, 'vars': {}, '_workspace': str(self.tree)}
        m = (job.get('strategy') or {}).get('matrix')
        if m is None:
            return [{}]
        m = interp(m, ctx)
        if not isinstance(m, dict):
            raise Drift(f'{jobid}: unsupported matrix {type(m)}')
        keys = {k: v for k, v in m.items() if k not in ('include', 'exclude')}
        if m.get('exclude'):
            raise Drift(f'{jobid}: matrix exclude is not modelled')
        combos = [{}]
        for k, vals in keys.items():
            combos = [dict(c, **{k: v}) for c in combos for v in vals]
        if not keys:
            combos = []
        for inc in m.get('include') or []:
            hit = False
            for c in combos if keys else []:
                if all(loose_eq(c[k], inc[k]) for k in inc if k in keys):
                    c.update({k: v for k, v in inc.items() if k not in keys})
                    hit = True
            if not hit:
                combos.append(dict(inc))
        return combos

    def job_if(self, job, declared):
        results = [self.needs[n]['result'] for n in declared]
        ctx = {'github': {'event_name': 'pull_request', 'ref': 'refs/pull/0/merge', 'repository': 'context-graph-ai/contextdb'},
               'needs': {n: self.needs[n] for n in declared}, 'env': {}, 'matrix': {}, 'steps': {}, 'runner': {},
               'inputs': {}, 'secrets': {}, 'vars': {},
               '_status': {'success': all(r == 'success' for r in results), 'failure': any(r == 'failure' for r in results),
                           'cancelled': False}}
        return condition(job.get('if'), ctx)

    def selected(self, box):
        return not self.only or self.only.search(box.name) or self.only.search(box.jobid)

    def finish_job(self, jobid, job, boxes):
        statuses = [b.status() for b in boxes]
        result = ('failure' if 'failure' in statuses else 'cancelled' if 'cancelled' in statuses else 'success') if boxes else 'skipped'
        outs = {}
        if boxes:
            ctx = dict(boxes[0].ctx, steps=boxes[0].steps)
            for k, v in (job.get('outputs') or {}).items():
                outs[k] = to_str(interp(v, ctx))
        self.needs[jobid] = {'result': result, 'outputs': outs}

    def run(self):
        for d in (self.out / 'logs', self.out / 'scripts', self.artifacts, self.tmp):
            d.mkdir(parents=True, exist_ok=True)
        jobs = self.doc['jobs']
        self.budget, budget_why = self.disk_budget()
        avail = int(re.search(r'MemAvailable:\s+(\d+)', Path('/proc/meminfo').read_text()).group(1)) * 1024
        cap = self.a.host_memory or min(self.a.boxes * BOX_MEMORY, avail - 8 * 1024 ** 3)
        subprocess.run(['systemctl', '--user', 'set-property', '--runtime', SLICE, f'MemoryMax={cap}', 'MemorySwapMax=0'], check=True)
        meta = dict(tree=str(self.tree), sha=self.sha, base=self.base, run_id=self.run_id, work=str(self.work),
                    tmp=str(self.tmp), boxes=self.a.boxes, box=dict(cpu=BOX_CPU_QUOTA, memory=BOX_MEMORY, swap=0),
                    disk_budget=self.budget, disk_budget_basis=budget_why, parent_slice_memory=cap,
                    launcher=self.launcher, only=self.a.only, started=time.strftime('%FT%T%z'))
        (self.out / 'meta.json').write_text(json.dumps(meta, indent=1))
        self.say(f'== {self.sha[:12]} base {self.base[:12]}; {self.a.boxes} boxes; disk budget {human(self.budget)}; '
                 f'parent slice {human(cap)}; out {self.out}')
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self.stop_all)
        stop = threading.Event()
        threading.Thread(target=self.watch_disk, args=(stop,), daemon=True).start()
        pool = ThreadPoolExecutor(max_workers=self.a.boxes)
        t0 = self.t0 = time.time()
        pending = list(jobs)
        while pending and not self.drift:
            for jobid in list(pending):
                job = jobs[jobid]
                declared = job.get('needs') or []
                declared = [declared] if isinstance(declared, str) else declared
                if jobid in self.boxes:
                    boxes = self.boxes[jobid]
                    if all(b.done.is_set() for b in boxes):
                        self.finish_job(jobid, job, [b for b in boxes if not b.filtered])
                        pending.remove(jobid)
                    continue
                if any(n not in self.needs for n in declared):
                    continue
                runs_on = to_str(job.get('runs-on', ''))
                if not self.job_if(job, declared):
                    self.needs[jobid] = {'result': 'skipped', 'outputs': {}}
                    self.note_job(jobid, f'job if: {job.get("if")} is false for this pull_request')
                    pending.remove(jobid)
                    continue
                if 'uses' in job:
                    raise Drift(f'{jobid}: a called workflow that runs for pull_request is not modelled')
                job['_combos'] = self.expand(jobid, job)
                boxes = []
                for i, mx in enumerate(job['_combos']):
                    b = Box(self, jobid, job, mx, i)
                    on = to_str(interp(job.get('runs-on', ''), b.ctx)) if '${{' in runs_on else runs_on
                    b.started, b.filtered = False, False
                    if not re.fullmatch(r'ubuntu-(latest|\d\d\.\d\d)', on):
                        self.note_job(b.name, f'runs-on {on}: not a Linux hosted runner; not run here')
                        b.filtered = True
                        b.done.set()
                    elif not self.selected(b):
                        self.note_job(b.name, '--only does not select it')
                        b.filtered = True
                        b.done.set()
                    boxes.append(b)
                self.boxes[jobid] = boxes
                for b in boxes:
                    if not b.filtered:
                        pool.submit(self.start, b)
            if pending:
                time.sleep(1)
        pool.shutdown(wait=True)
        stop.set()
        if not self.a.keep_boxes:
            for boxes in self.boxes.values():
                for b in boxes:
                    if b.keep:
                        for d in (b.root, b.tmp):
                            shutil.rmtree(d, ignore_errors=True)
        return self.report(time.time() - t0)

    def start(self, box):
        box.started = True
        self.say(f'[box ] {box.name}')
        box.execute()
        box.peak_disk = max(box.peak_disk, du_bytes(box.root, box.tmp) - box.base_disk)
        if not self.a.keep_boxes and not box.keep:   # a hosted runner is discarded after its job; so is the box
            for d in (box.root, box.tmp):
                shutil.rmtree(d, ignore_errors=True)
        self.say(f'[job ] {box.name}: {box.status()} {box.used / 60:.1f} min' + (f' ({box.cause})' if box.cause else ''))

    def note_job(self, name, reason):
        with self.lock:
            self.not_run_rows.append((name, '(job)', reason))

    def report(self, total):
        rows = []
        for jobid, boxes in self.boxes.items():
            for b in boxes:
                if b.filtered:
                    continue
                result = ('interrupted' if not b.done.is_set() else 'inconclusive' if b.inconclusive else b.status())
                rows.append((jobid, b.name, result, b.cause, f'{b.used / 60:.1f}', b.peak_mem, b.peak_disk))
        with open(self.out / 'jobs.tsv', 'w') as f:
            f.write('job\tname\tresult\tcause\tminutes\tpeak_memory_bytes\tpeak_disk_bytes\n')
            for r in rows:
                f.write('\t'.join(map(str, r)) + '\n')
        with open(self.out / 'steps.tsv', 'w') as f:
            f.write('step\tjob\tname\tstatus\trc\tresult\tbox_seconds\tadmission_wait_seconds\tcpu_seconds\tpeak_memory_bytes\tlog\tfirst_error\n')
            for r in sorted(self.steps_rows):
                f.write('\t'.join(map(str, r)) + '\n')
        (self.out / 'needs.json').write_text(json.dumps(self.needs, indent=1))
        with open(self.out / 'not-run.tsv', 'w') as f:
            f.write('job\tstep\treason\n')
            for r in self.not_run_rows:
                f.write('\t'.join(map(str, r)) + '\n')
        if self.interrupted:
            line = f'INTERRUPTED jobs={len(rows)} finished={sum(r[2] != "interrupted" for r in rows)}'
            code = 130
        elif self.drift:
            line = f'DRIFT {self.drift}'
            code = 3
        else:
            final = self.needs.get('required', {}).get('result', 'missing')
            failed = sorted(r[1] for r in rows if r[2] != 'success')
            line = f'{"PASS" if final == "success" else "FAIL"} required={final} jobs={len(rows)} not-success={len(failed)}'
            code = 0 if final == 'success' else 1
        (self.out / 'result.txt').write_text(line + f'\nwall {total / 60:.1f} min\n')
        self.say(line)
        self.say(f'wall {total / 60:.1f} min; report {self.out}')
        return code


# ------------------------------------------------------------------------------------------------
def take_cores(pool, unit, offered, count):
    """Give this step `count` CPUs of the admitted set that no other running box step holds; wait while
    fewer are free, since a 4-vCPU runner never shares its CPUs (the wait is admission, not box time)."""
    import fcntl
    while True:
        with open(str(pool) + '.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            held = json.loads(Path(pool).read_text()) if Path(pool).exists() else {}
            now = time.time()
            # an entry stays held while its service runs, or briefly before it has started
            live = {u: e for u, e in held.items() if now - e['since'] < 60 or
                    subprocess.run(['systemctl', '--user', 'is-active', '--quiet', u]).returncode == 0}
            busy = {c for e in live.values() for c in e['cpus']}
            free = [c for c in offered if c not in busy]
            if len(free) >= count or len(offered) < count:
                mine = (free or offered)[:count]
                live[unit] = {'cpus': mine, 'since': now}
                Path(pool).write_text(json.dumps(live))
                return mine
            Path(pool).write_text(json.dumps(live))
        time.sleep(2)


def release_cores(pool, unit):
    import fcntl
    with open(str(pool) + '.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        held = json.loads(Path(pool).read_text()) if Path(pool).exists() else {}
        held.pop(unit, None)
        Path(pool).write_text(json.dumps(held))


def box_main(argv):
    """Inside the admission wrapper: start one step as a capped transient service and wait for it."""
    p = argparse.ArgumentParser(prog='ci-local.py _box')
    for flag in ('--unit', '--runtime', '--cwd', '--cpu-model',
                 '--cpu-pool', '--env-file', '--stamp'):
        p.add_argument(flag, required=True)
    p.add_argument('script')
    a = p.parse_args(argv)
    offered = sorted(os.sched_getaffinity(0))
    if a.cpu_model == 'cores':   # four whole CPUs, never throttled, as on a 4-vCPU VM
        cpus = take_cores(a.cpu_pool, a.unit, offered, BOX_CPUS)
        cpu_props = ['-p', f'AllowedCPUs={",".join(map(str, cpus))}']
    else:                        # four CPUs' worth of time spread over the admitted set
        cpu_props = ['-p', f'CPUQuota={BOX_CPU_QUOTA}', '-p', f'AllowedCPUs={",".join(map(str, offered))}']
    inner = ('while IFS= read -r -d "" kv; do export "$kv"; done < "$1"; date +%s.%N > "$2"; '
             'exec bash --noprofile --norc -eo pipefail "$3"')
    cmd = ['systemd-run', '--user', '--wait', '--collect', '--pipe', f'--unit={a.unit}', f'--slice={SLICE}',
           f'--working-directory={a.cwd}', *cpu_props, '-p', f'MemoryMax={BOX_MEMORY}',
           '-p', 'MemorySwapMax=0', '-p', 'OOMPolicy=continue', '-p', f'RuntimeMaxSec={a.runtime}', '-p', 'TimeoutStopSec=5',
           '--',
           'env', '-i', 'bash', '--noprofile', '--norc', '-c', inner, 'box', a.env_file, a.stamp, a.script]
    try:
        rc = subprocess.run(cmd).returncode
    finally:
        if a.cpu_model == 'cores':
            release_cores(a.cpu_pool, a.unit)
    sys.exit(rc)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == '_box':
        box_main(sys.argv[2:])
    here = Path(__file__).resolve().parents[1]
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    p.add_argument('--tree', default=str(here), help='checkout to run (default: this repository)')
    p.add_argument('--work', default=os.environ.get('CI_LOCAL_WORK', str(here / 'target/ci-local/work')),
                   help='per-run box checkouts, targets and artifacts')
    p.add_argument('--tmp', default=os.environ.get('CI_LOCAL_TMP', str(here / 'target/ci-local/tmp')),
                   help='per-box RUNNER_TEMP and TMPDIR')
    p.add_argument('--out', help='report directory (default: <work>/<run>/report)')
    p.add_argument('--boxes', type=int, default=8, help='job boxes running at once (default 8)')
    p.add_argument('--only', help='run only job instances whose name or job id matches this regex')
    p.add_argument('--base', help='pull-request base commit (default: merge-base with origin/dev)')
    p.add_argument('--disk-budget', type=int, help='bytes a box may add after checkout (default: hosted figure)')
    p.add_argument('--host-memory', type=int, help='memory cap of the parent slice of all boxes, bytes')
    p.add_argument('--launcher', default='build-any', help='admission wrapper for compiling steps, if on PATH')
    p.add_argument('--cpu-model', choices=['cores', 'quota'], default='cores',
                   help='cores: 4 whole CPUs per box step (default, like a 4-vCPU VM); quota: CPUQuota=400%% over the admitted CPUs')
    p.add_argument('--keep-boxes', action='store_true', help='keep each box checkout and temp dir after its job')
    p.add_argument('--path-prepend', action='append', default=[], help='directory put first on PATH in every box')
    a = p.parse_args()
    try:
        sys.exit(LocalRun(a).run())
    except Drift as e:
        print(f'DRIFT {e}', file=sys.stderr)
        sys.exit(3)


if __name__ == '__main__':
    main()
