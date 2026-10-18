#!/usr/bin/env python3
"""Fixed public source images; diagnostic capture never grants PR qualification.
No user-selected refs, paths, code URLs, cases, execution modes or fallback source.
"""
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import xml.etree.ElementTree as ET

REPO = 'context-graph-ai/contextdb'
BRANCH = 'ci-macos-diagnostic-295'
BASE = 'ef16fe66365bb3d1b39c96d22403187610ab8958'
FROZEN_SHA = 'f5066c842195aeba87fb0943fb4eebbb00093fb2d0fde692f641482339b44f53'
CARGO = ['rustup', 'run', '1.94.1', 'cargo']
NAMES = {'contextdb-'+n for n in ('core','engine','graph','hnsw','parser','planner','redb','relational','server','tx','vector')}
PHASES = {'subscription_trace': ('engine_trace','CG_trace')}


# Scoped to the selected product case, never build/list/seed/model prerequisites.
CASE_ENV = {'SUBSCRIPTION_EXTERNAL_TRACE':'external-472-v1',
            'NEXTEST_SUCCESS_OUTPUT':'immediate', 'NEXTEST_FAILURE_OUTPUT':'immediate'}


def case_environment(base):
    return dict(base, **CASE_ENV)


def trace_presence(stdout, stderr):
    # Only recognize a complete emitted line prefix after nextest whitespace indentation.
    # This is an output-transport check, not an accepted trace parser or causal oracle.
    pattern = rb'^[ \t]*EXTSUB1 stream=(?:engine|cg|test) pid=[0-9]+ seq=[0-9]+ epoch_ns=[0-9]+ sink=[a-fA-F0-9]{32} event=[^ \t\r\n]+[^\r\n]*\r?$'
    counts = {name:len(re.findall(pattern, raw, re.MULTILINE))
              for name,raw in (('stdout',stdout),('stderr',stderr))}
    return {'status':'PRESENT' if sum(counts.values()) else 'MISSING_TRACE',
            'line_counts':counts, 'interpretation':'raw-capture-only/unverified'}


def finish_case(out, receipt, row, code, junit_raw):
    receipt['trace_capture'] = trace_presence((out/'case-1.stdout').read_bytes(),
                                             (out/'case-1.stderr').read_bytes())
    receipt['capture_complete'] = False
    receipt['verdict'] = 'TEST_FAIL' if code > 0 else 'FAIL'
    result = junit_result(junit_raw, row, code)
    receipt['runs'].append(result); receipt['unrun'] = 0
    receipt['capture_complete'] = receipt['trace_capture']['status'] == 'PRESENT'
    if receipt['capture_complete']:
        receipt['verdict'] = 'PASS' if result['passed'] == 1 else 'TEST_FAIL'
    else:
        receipt['error'] = 'MISSING_TRACE: selected case retained no emitted trace lines'


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def digest(path):
    return sha(Path(path).read_bytes())


def write_json(path, data):
    path = Path(path)
    tmp = path.with_suffix(path.suffix+'.tmp')
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True)+'\n')
    tmp.replace(path)


def load_frozen(root):
    p = root/'scripts/diagnostic-assets/frozen.json'
    require(digest(p) == FROZEN_SHA, 'frozen inventory hash mismatch')
    return json.loads(p.read_text())


def selection(config, frozen):
    keys = {'schema','phase','tooling_base','frozen_sha256','activation_receipt_sha256',
            'reader_adoption_receipt_sha256','actual_tooling_sha','actual_tooling_tree',
            'actual_generated_lock_sha256','actual_metadata_sha256','actual_source_post'}
    require(set(config) == keys and config['schema'] == 'mac-diagnostic-selection-v1', 'selection schema mismatch')
    require(config['phase'] in PHASES, 'unknown phase')
    require(config['tooling_base'] == BASE and config['frozen_sha256'] == FROZEN_SHA, 'selection source binding mismatch')
    for x in ('activation_receipt_sha256',):
        require(isinstance(config[x], str) and re.fullmatch('[a-f0-9]{64}', config[x]), 'null activation: not admitted')
    require(config['reader_adoption_receipt_sha256'] is None, 'reader authority forbidden in trace image')
    require(all(config[k] is None for k in keys if k.startswith('actual_')), 'actual receipts cannot be prefilled')
    return [frozen['patches'][x] for x in PHASES[config['phase']]]


APPROVAL_VERDICT = 'CLEAR_FOR_BOUNDED_DIAGNOSTIC_PUBLICATION_AND_ONE_CAPTURE'
SELECTION_PATH = 'scripts/macos-diagnostic-selection.json'
ACTIVATION_PATH = 'scripts/diagnostic-assets/activation.json'
REVIEWED_PATHS = ('.github/workflows/macos-diagnostics.yml',
    'scripts/ci-macos-diagnostics.py', 'scripts/observe-macos-diagnostics.py',
    'scripts/dispatch-macos-diagnostics.py', SELECTION_PATH,
    'scripts/macos-diagnostic-selection.schema.json', 'scripts/diagnostic-assets/frozen.json',
    'scripts/diagnostic-assets/nomic.rs', 'scripts/diagnostic-assets/CG_trace.patch',
    'scripts/diagnostic-assets/engine_trace.patch')


def canonical(data):
    return json.dumps(data, sort_keys=True, separators=(',', ':')).encode()


def content_entry(path, raw, mode):
    require(mode == '100644', 'reviewed public files must be ordinary nonexecutable files')
    if path == SELECTION_PATH:
        config = json.loads(raw)
        require('activation_receipt_sha256' in config, 'selection missing activation field')
        pattern = rb'("activation_receipt_sha256"\s*:\s*)(null|"[a-f0-9]{64}")'
        require(len(re.findall(pattern, raw)) == 1, 'selection activation value must occur once')
        raw = re.sub(pattern, lambda match: match[1]+b'null', raw)
    return {'sha256': sha(raw), 'mode': mode}


def reviewed_manifest(root):
    files = {}
    for path in REVIEWED_PATHS:
        p = root/path
        require(p.is_file() and not p.is_symlink(), 'missing/linked reviewed source: '+path)
        mode = '100755' if p.stat().st_mode & 0o111 else '100644'
        files[path] = content_entry(path, p.read_bytes(), mode)
    return {'schema':'mac-diagnostic-reviewed-content-v1', 'files':files}


def source_scope(frozen):
    return {'tooling_base':BASE, 'phase':'subscription_trace', 'frozen_sha256':FROZEN_SHA,
            'sources':{r:s['commit'] for r,s in frozen['sources'].items()}}


def activation_binding(authority, manifest, frozen):
    keys = {'schema', 'tooling_base', 'phase', 'frozen_sha256', 'sources',
            'reviewed_content_manifest_sha256', 'diagnostic_publication_approval_receipt_sha256',
            'publication_approved', 'native_execution_approved'}
    require(set(authority) == keys and authority['schema'] == 'mac-diagnostic-activation-v2',
            'activation schema mismatch')
    require(all(authority[k] == v for k,v in source_scope(frozen).items()), 'activation source scope mismatch')
    require(authority['reviewed_content_manifest_sha256'] == sha(canonical(manifest)),
            'activation reviewed content mismatch')
    require(isinstance(authority['diagnostic_publication_approval_receipt_sha256'], str) and
            re.fullmatch('[a-f0-9]{64}', authority['diagnostic_publication_approval_receipt_sha256']),
            'diagnostic source approval hash required')
    require(authority['publication_approved'] is True and authority['native_execution_approved'] is True,
            'concrete diagnostic activation required')
    return authority


def safe_extract(raw, root):
    root.mkdir(exist_ok=False)
    with tarfile.open(fileobj=io.BytesIO(raw), mode='r:') as archive:
        seen = set()
        for m in archive:
            p = PurePosixPath(m.name)
            require(not p.is_absolute() and '..' not in p.parts and p.parts and m.name not in seen,
                    'unsafe/duplicate archive path')
            seen.add(m.name)
            require(m.isfile() or m.isdir(), 'archive links/devices forbidden')
            target = root.joinpath(*p.parts)
            if m.isdir():
                target.mkdir(parents=True, exist_ok=True)
            else:
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(archive.extractfile(m).read())
                target.chmod(m.mode & 0o777)


def file_map(root):
    result = {}
    for p in root.rglob('*'):
        require(not p.is_symlink(), 'source symlink forbidden')
        if p.is_file():
            result[p.relative_to(root).as_posix()] = digest(p)
    return result


def expected_maps(frozen, patches):
    maps = {r: dict(s['files']) for r,s in frozen['sources'].items()}
    for patch in patches:
        for c in patch['changes']:
            require(maps[c['repo']][c['path']] == c['preimage_sha256'], 'patch preimage does not match fixed source')
            maps[c['repo']][c['path']] = c['postimage_sha256']
    return maps


def verify_map(root, expected):
    actual = file_map(root)
    require(actual == expected, 'full source closure mismatch')
    return actual


def overlay_text(engine):
    text = '[patch."https://github.com/context-graph-ai/contextdb"]\n'
    for name in sorted(NAMES):
        text += name+' = { path = '+json.dumps(str(engine/'crates'/name))+' }\n'
    return text


def overlay(cg, engine):
    directory = cg/'.cargo'
    require(not (directory/'config').exists() and not (directory/'config.toml').exists(), 'unexpected Cargo config')
    directory.mkdir(exist_ok=True)
    path = directory/'config.toml'
    path.write_text(overlay_text(engine))
    return path


def normalize_dependency(value):
    # Cargo lock dependency entries can qualify a package by source identity.
    return re.sub(r' \(git\+https://github.com/context-graph-ai/contextdb[^)]*\)$', '', value)


def approve_lock(before, after):
    before, after = tomllib.loads(before), tomllib.loads(after)
    require(set(before) == set(after) and before.get('version') == after.get('version'), 'lock header changed')
    old, new = before.pop('package'), after.pop('package')
    require(before == after, 'lock nonpackage data changed')
    def indexed(rows):
        d = {}
        for p in rows:
            k = (p['name'], p['version'])
            require(k not in d, 'ambiguous/mixed package identities')
            d[k] = p
        return d
    a,b = indexed(old),indexed(new)
    require(set(a) == set(b), 'package added/removed/version changed')
    changed = set()
    for k,p in a.items():
        q = b[k]
        p,q = dict(p),dict(q)
        if k[0] in NAMES:
            require(p.get('source','').startswith('git+https://github.com/context-graph-ai/contextdb?rev=db9d067f291c0add0922713936a63f51f24e9f1c#db9d067f291c0add0922713936a63f51f24e9f1c'), 'wrong original engine source')
            require('source' not in q and 'checksum' not in q and 'checksum' not in p, 'engine not coherent path source')
            del p['source']; changed.add(k[0])
        p['dependencies'] = sorted(normalize_dependency(x) for x in p.get('dependencies', []))
        q['dependencies'] = sorted(normalize_dependency(x) for x in q.get('dependencies', []))
        require(p == q, 'unexpected lock change: '+k[0])
    require(changed == NAMES, 'all eleven source replacements required')
    return sorted(changed)


def metadata_closure(metadata, engine, frozen):
    packages = metadata['packages']
    require(packages and len({p['id'] for p in packages}) == len(packages), 'empty/duplicate package identities')
    rows = [p for p in packages if p['name'] in NAMES]
    require(len(rows) == 11 and {p['name'] for p in rows} == NAMES, 'mixed or missing engine packages')
    ids = {}
    for p in rows:
        manifest = engine/'crates'/p['name']/'Cargo.toml'
        require(p['source'] is None and Path(p['manifest_path']).resolve() == manifest.resolve(), 'engine package outside selected image')
        require(p['version'] == ('4.1.0' if p['name']=='contextdb-redb' else '1.0.0'), 'engine version mismatch')
        ids[p['name']] = p['id']
        expected = next(x for x in frozen['closure'] if x['package']==p['name'])
        for edge in expected['path_dependencies']:
            matches = [d for d in p['dependencies'] if d['name']==edge['package'] and
                       (d.get('rename') or d['name'])==edge['alias'] and
                       (d.get('kind') or 'normal')==('dev' if edge['kind']=='dev-dependencies' else 'normal')]
            require(len(matches)==1 and matches[0]['source'] is None and
                    Path(matches[0]['path']).resolve()==(manifest.parent/edge['path']).resolve(), 'alias/path edge mismatch')
    canonical_graph(metadata, {})
    return {'package_ids':ids,'nodes':metadata['resolve']['nodes'],'workspace_members':metadata['workspace_members']}


def canonical_graph(metadata, replacements):
    # IDs are opaque. Only the explicit package mapping may replace them.
    packages={p['id']:p for p in metadata['packages']}
    rows=metadata['resolve']['nodes']
    require(rows and len({n['id'] for n in rows})==len(rows) and
            {n['id'] for n in rows}==set(packages), 'empty/missing/duplicate resolved nodes')
    def ident(value):
        require(value in packages, 'unknown resolved identity')
        return replacements.get(value,value)
    result={}; edge_count=0
    for n in rows:
        deps=[]
        require(isinstance(n['features'],list) and len(set(n['features']))==len(n['features']), 'invalid resolved features')
        require(set(n['dependencies'])=={d['pkg'] for d in n['deps']}, 'resolved dependency representations differ')
        for d in n['deps']:
            require(d['name'] and d['dep_kinds'], 'missing dependency alias/kind')
            kinds=[]
            for k in d['dep_kinds']:
                require(set(k)=={'kind','target'} and k['kind'] in (None,'dev','build') and
                        (k['target'] is None or isinstance(k['target'],str)), 'invalid dependency kind/target')
                kinds.append(k)
            deps.append({'name':d['name'],'pkg':ident(d['pkg']),
                         'dep_kinds':sorted(kinds,key=lambda x:json.dumps(x,sort_keys=True))})
        edge_count+=len(deps)
        require(len({json.dumps(d,sort_keys=True) for d in deps})==len(deps), 'duplicate resolved edge')
        result[ident(n['id'])]={'features':sorted(n['features']),
            'dependencies':sorted(ident(x) for x in n['dependencies']),
            'deps':sorted(deps,key=lambda x:json.dumps(x,sort_keys=True))}
    require(edge_count>0, 'empty resolved dependency graph')
    return {'nodes':result,'root':ident(metadata['resolve']['root']) if metadata['resolve']['root'] else None,
            'workspace_members':sorted(ident(x) for x in metadata['workspace_members']),
            'workspace_default_members':sorted(ident(x) for x in metadata['workspace_default_members'])}


def compare_metadata(baseline, derived, engine, frozen, before_lock, after_lock, evidence_dir=None):
    approve_lock(before_lock,after_lock)
    graph=metadata_closure(derived,engine,frozen)
    old={p['id']:p for p in baseline['packages']}; new={p['id']:p for p in derived['packages']}
    require(len(old)==len(baseline['packages']) and len(new)==len(derived['packages']), 'duplicate metadata package')
    old_engine=[p for p in old.values() if p['name'] in NAMES]
    require(len(old_engine)==11 and {p['name'] for p in old_engine}==NAMES, 'baseline engine closure mismatch')
    source='git+https://github.com/context-graph-ai/contextdb?rev='+frozen['sources']['engine']['commit']+'#'+frozen['sources']['engine']['commit']
    mapping={}
    for p in old_engine:
        q=new[graph['package_ids'][p['name']]]
        require(p['source']==source and p['version']==q['version'], 'baseline engine identity mismatch')
        mapping[p['id']]=q['id']
    require(len(set(mapping.values()))==11 and {mapping.get(i,i) for i in old}==set(new), 'unapproved package identity change')
    for metadata,lock in ((baseline,before_lock),(derived,after_lock)):
        lockrows=tomllib.loads(lock)['package']
        for p in metadata['packages']:
            matches=[r for r in lockrows if (r['name'],r['version'],r.get('source'))==(p['name'],p['version'],p['source'])]
            require(len(matches)==1, 'metadata identity absent/ambiguous in accepted lock: '+p['name'])
    for i,p in old.items():
        q=new[mapping.get(i,i)]
        require((p['name'],p['version'])==(q['name'],q['version']), 'package name/version changed')
        if i not in mapping:
            require(p['source']==q['source'] and p['manifest_path']==q['manifest_path'], 'unapproved source/path replacement')
    expected=canonical_graph(baseline,mapping); actual=canonical_graph(derived,{})
    if evidence_dir is not None:
        import difflib
        write_json(evidence_dir/'graph-baseline-normalized.json',expected)
        write_json(evidence_dir/'graph-overlay-normalized.json',actual)
        (evidence_dir/'graph.diff').write_text(''.join(difflib.unified_diff(json.dumps(expected,sort_keys=True,indent=2).splitlines(True),json.dumps(actual,sort_keys=True,indent=2).splitlines(True),fromfile='baseline-normalized',tofile='overlay-normalized')))
    require(expected==actual, 'resolved baseline/overlay graph differs: edges/aliases/kinds/targets/features')
    return dict(graph,comparison={'equal':True,'approved_id_replacements':mapping,
                                 'normalized_sha256':sha(json.dumps(actual,sort_keys=True).encode())})


def cargo_artifacts(raw, package_id, target_name, target_dir, engine_id):
    rows = [json.loads(l) for l in raw.splitlines() if l.strip()]
    require([r.get('success') for r in rows if r.get('reason')=='build-finished']==[True], 'Cargo build incomplete')
    engine = [r for r in rows if r.get('reason')=='compiler-artifact' and r.get('package_id')==engine_id]
    require(engine and all(not r.get('fresh', True) for r in engine), 'changed engine must compile in empty owned target')
    matches = [r for r in rows if r.get('reason')=='compiler-artifact' and r.get('package_id')==package_id and
               r.get('target',{}).get('name')==target_name and r.get('target',{}).get('kind')==['test'] and
               r.get('profile',{}).get('test') is True and r.get('executable')]
    require(len(matches)==1, 'unique test artifact required')
    binary = Path(matches[0]['executable']).resolve()
    require(binary.is_relative_to(target_dir.resolve()) and binary.is_file() and os.access(binary,os.X_OK), 'artifact outside target')
    return {'path':str(binary),'sha256':digest(binary),'cargo_artifact':matches[0],'engine_artifacts':engine}


def inventory(data, row, binary, selected=True):
    suites=data['rust-suites']
    require(set(suites)=={row['binary']}, 'unexpected binary inventory')
    s=suites[row['binary']]
    require(s['binary-id']==row['binary'] and s['status']=='listed', 'suite identity/status mismatch')
    require(Path(s['binary-path']).resolve()==Path(binary).resolve(), 'listed binary differs from compiled artifact')
    tests=s['testcases'];matches=[]
    require(type(data['test-count']) is int and data['test-count']==sum(len(x['testcases']) for x in suites.values()), 'discovered count/map mismatch')
    for name,test in tests.items():
        require(type(test['ignored']) is bool, 'unknown ignored state')
        f=test['filter-match'];status=f['status']
        require(status in ('matches','mismatch'), 'unknown filter status')
        if status=='mismatch':
            require(f.get('reason') in ('not-benchmark','ignored','string','expression','partition','rerun-already-passed','default-filter'), 'unknown filter mismatch reason')
        else:
            require(test['ignored'] is False, 'ignored matching case')
            matches.append(name)
    require(row['case'] in tests and tests[row['case']]['ignored'] is False, 'missing/ignored requested case')
    if selected: require(matches==[row['case']], 'exact one matching selected case required')
    return s


def inventory_pair(full, selected, row, binary):
    a=inventory(full,row,binary,selected=False);b=inventory(selected,row,binary)
    require(full['test-count']==selected['test-count'] and set(a['testcases'])==set(b['testcases']), 'selected inventory discarded discovered cases')
    require({k:v for k,v in a.items() if k!='testcases'}=={k:v for k,v in b.items() if k!='testcases'}, 'inventory suite metadata changed')
    for name,t in a['testcases'].items():
        require({k:v for k,v in t.items() if k!='filter-match'}==
                {k:v for k,v in b['testcases'][name].items() if k!='filter-match'}, 'inventory testcase identity changed')
    return b


def junit_result(raw, row, code):
    root=ET.fromstring(raw)
    cases=list(root.iter('testcase'))
    require(len(cases)==1 and cases[0].get('name')==row['case'] and
            cases[0].get('classname')==row['binary'] and cases[0].find('skipped') is None, 'JUnit exact case required')
    failed=cases[0].find('failure') is not None or cases[0].find('error') is not None
    require((code==0 and not failed) or (code>0 and failed), 'exit/JUnit mismatch or signal')
    return {'executed':1,'passed':int(not failed),'failed':int(failed),'skipped':0,'exit':code,
            'case':row['case'],'binary':row['binary']}


def redact(raw, secrets):
    for value in secrets:
        if value:
            raw=raw.replace(value.encode(), b'[REDACTED]')
    raw=re.sub(rb'(https://)[^/\s:@]+:[^/\s@]+@', rb'\1[REDACTED]@', raw)
    return raw


class Commands:
    def __init__(self, out, receipt, secrets=()):
        self.out,self.receipt,self.secrets=out,receipt,secrets
        self.active=None

    def save(self):
        write_json(self.out/'receipt.json',self.receipt)

    def stop(self):
        p=self.active
        if p is None:
            return
        # Kill the whole session even if its leader has already exited.
        try: os.killpg(p.pid, signal.SIGTERM)
        except ProcessLookupError: pass
        try: p.wait(timeout=3)
        except subprocess.TimeoutExpired: pass
        try: os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        p.wait()
        self.active=None

    def run(self, args, name, cwd, env=None, timeout=6600, required=True, binary=False):
        require(re.fullmatch('[a-z0-9-]+',name), 'unsafe evidence name')
        start=time.time()
        child_env=dict(os.environ if env is None else env)
        if name != 'case-1':
            for key in CASE_ENV: child_env.pop(key,None)
        else:
            require(all(child_env.get(k)==v for k,v in CASE_ENV.items()), 'selected case environment missing')
        record={'environment':{k:child_env[k] for k in CASE_ENV if k in child_env},'argv':list(map(str,args)),'cwd':str(cwd),'name':name,'started':start,'exit':None,'complete':False}
        self.receipt['commands'].append(record); self.save()
        timed_out=False
        try:
            self.active=subprocess.Popen(args,cwd=cwd,env=child_env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
            try: stdout,stderr=self.active.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                timed_out=True
                p=self.active; self.stop(); stdout,stderr=p.communicate()
            code=self.active.returncode if self.active is not None else p.returncode
            self.stop()
            if not binary: (self.out/(name+'.stdout')).write_bytes(redact(stdout,self.secrets))
            (self.out/(name+'.stderr')).write_bytes(redact(stderr,self.secrets))
            record.update(exit=code,complete=True,timeout=timed_out,duration=time.time()-start,
                          stdout_sha256=sha(stdout) if binary else digest(self.out/(name+'.stdout')),stderr_sha256=digest(self.out/(name+'.stderr')),stdout_excluded=binary)
            self.save()
            require(not timed_out, 'command timed out: '+name)
            require(not required or code==0,'command failed: '+name)
            return subprocess.CompletedProcess(args,code,stdout,stderr)
        except BaseException:
            p=self.active
            self.stop()
            if p is not None:
                stdout,stderr=p.communicate()
                if not binary: (self.out/(name+'.stdout')).write_bytes(redact(stdout,self.secrets))
                (self.out/(name+'.stderr')).write_bytes(redact(stderr,self.secrets))
                record.update(exit=p.returncode,complete=False,duration=time.time()-start)
                for stream in (p.stdout,p.stderr):
                    if stream is not None: stream.close()
            raise
        finally:
            p=self.active
            self.stop()
            if 'p' in locals() and p is not None:
                for stream in (p.stdout,p.stderr):
                    if stream is not None: stream.close()
            self.save()


def interrupted(signum, frame):
    raise InterruptedError('signal '+str(signum))


def capture(root):
    out=root/'target/macos-diagnostics'
    out.mkdir(parents=True,exist_ok=False)
    receipt={'schema':'mac-diagnostic-receipt-v1','verdict':'FAIL','product_qualification':'UNQUALIFIED',
             'commands':[],'runs':[],'actual_tooling_sha':None,'generated_lock_sha256':None,
             'metadata_sha256':None,'source_post':None,'cleanup':{'success':False},'unrun':1,
             'capture_complete':False,'case_environment':dict(CASE_ENV)}
    secrets=[v for k,v in os.environ.items() if any(x in k.upper() for x in ('TOKEN','PASSWORD','SECRET','API_KEY')) and len(v)>5]
    cmd=Commands(out,receipt,secrets)
    temp=None; sources={}; expected={}
    for s in (signal.SIGTERM,signal.SIGINT): signal.signal(s,interrupted)
    cmd.save()
    try:
        frozen=load_frozen(root)
        config=json.loads((root/'scripts/macos-diagnostic-selection.json').read_text())
        patches=selection(config,frozen)
        require(platform.system()=='Darwin', 'Darwin required')
        T=os.environ.get('GITHUB_SHA','')
        require(re.fullmatch('[a-f0-9]{40}',T) and os.environ.get('GITHUB_WORKFLOW_SHA')==T and
                os.environ.get('GITHUB_REPOSITORY')==REPO and os.environ.get('GITHUB_REF')=='refs/heads/'+BRANCH and
                os.environ.get('GITHUB_EVENT_NAME')=='push' and os.environ.get('RUNNER_ENVIRONMENT')=='github-hosted', 'host/event identity mismatch')
        require(cmd.run(['git','rev-parse','HEAD'],'tooling-head',root).stdout.decode().strip()==T,'tooling checkout mismatch')
        require(not cmd.run(['git','status','--porcelain','--untracked-files=no'],'tooling-status',root).stdout.strip(),'dirty tooling')
        require(cmd.run(['git','rev-parse','HEAD^'],'tooling-parent',root).stdout.decode().strip()==BASE,'tooling must be single ordinary commit over fixed base')
        # Activation is a content-bound manager receipt; exact resulting T is separately bound by local observer.
        activation=root/'scripts/diagnostic-assets/activation.json'
        require(digest(activation)==config['activation_receipt_sha256'],'activation receipt mismatch')
        authority=json.loads(activation.read_text())
        activation_binding(authority, reviewed_manifest(root), frozen)
        receipt.update(actual_tooling_sha=T,actual_tooling_tree=cmd.run(['git','rev-parse','HEAD^{tree}'],'tooling-tree',root).stdout.decode().strip(),
                       phase='subscription_trace',selection=config,selection_sha256=digest(root/'scripts/macos-diagnostic-selection.json'),activation=authority,platform=platform.platform(),
                       arch=platform.machine(),run_id=os.environ['GITHUB_RUN_ID'],run_attempt=os.environ['GITHUB_RUN_ATTEMPT'],event='push',ref='refs/heads/'+BRANCH)
        for name,args in [('sw-vers',['sw_vers']),('uname',['uname','-a']),('cpu',['sysctl','-n','hw.ncpu']),
                          ('memory',['sysctl','-n','hw.memsize']),('disk',['df','-k','.']),('python',['python3','--version']),
                          ('rustc', ['rustup','run','1.94.1','rustc','-Vv']),('cargo',CARGO+['-V']),
                          ('nextest',CARGO+['nextest','--version']),('cfg',['rustup','run','1.94.1','rustc','--print','cfg'])]:
            result=cmd.run(args,name,root)
            if name=='rustc': require(result.stdout.decode().startswith('rustc 1.94.1 ') and '-apple-darwin' in result.stdout.decode(),'native Rust pin mismatch')
            if name=='nextest': require('0.9.140' in result.stdout.decode(),'nextest pin mismatch')
        receipt['tool_paths']={}
        for tool in ('rustc','cargo'):
            p=Path(cmd.run(['rustup','which','--toolchain','1.94.1',tool],'which-'+tool,root).stdout.decode().strip())
            receipt['tool_paths'][tool]={'path':str(p),'sha256':digest(p)}
        p=Path(shutil.which('cargo-nextest')); receipt['tool_paths']['nextest']={'path':str(p),'sha256':digest(p)}
        temp=Path(tempfile.mkdtemp(prefix='mac-diagnostic-',dir=os.environ['RUNNER_TEMP'])).resolve()
        receipt['owned_root']=str(temp); cmd.save()
        for repo,s in frozen['sources'].items():
            objects=temp/(repo+'.git'); tree=temp/repo
            cmd.run(['git','init','--bare',str(objects)],repo+'-init',root)
            env=dict(os.environ,GIT_TERMINAL_PROMPT='0')
            cmd.run(['git','-c','credential.helper=','fetch','--no-tags','--depth=1',s['url'],s['commit']],repo+'-fetch',objects,env)
            require(cmd.run(['git','rev-parse','FETCH_HEAD'],repo+'-head',objects).stdout.decode().strip()==s['commit'],'fetched commit mismatch')
            require(cmd.run(['git','rev-parse',s['commit']+'^{tree}'],repo+'-tree',objects).stdout.decode().strip()==s['tree'],'tree mismatch')
            closure=cmd.run(['git','rev-list','--objects','--missing=print',s['commit']],repo+'-objects',objects).stdout.decode()
            require(not any(l.startswith('?') for l in closure.splitlines()),'missing source object')
            raw=cmd.run(['git','archive','--format=tar',s['commit']],repo+'-archive',objects,binary=True).stdout
            # Raw source archives remain in memory only, never in the upload allowlist.
            safe_extract(raw,tree)
            sources[repo]=tree
            verify_map(tree,s['files'])
            (out/(repo+'-original-Cargo.lock')).write_bytes((tree/'Cargo.lock').read_bytes())
        cg,engine=sources['cg'],sources['engine']
        target=temp/'target'; target.mkdir()
        env=dict(os.environ,CARGO_TARGET_DIR=str(target),CG_DATA_ROOT=str(temp/'data'),GATE_CI_MODEL_MODE='catalog-narrow',CI='true')
        metadata_flags=['metadata','--format-version','1']
        receipt['metadata_flags']=metadata_flags
        receipt['baseline_source_maps']={r:file_map(p) for r,p in sources.items()}
        baseline=json.loads(cmd.run(CARGO+metadata_flags+['--locked'],'metadata-baseline',cg,env).stdout)
        for repo,tree in sources.items(): verify_map(tree,frozen['sources'][repo]['files'])
        receipt['baseline_metadata_sha256']=sha(json.dumps(baseline,sort_keys=True).encode())
        expected=expected_maps(frozen,patches)
        for i,p in enumerate(patches):
            path=root/p['file']; require(digest(path)==p['sha256'],'patch hash mismatch')
            repo=p['changes'][0]['repo']; require(all(c['repo']==repo for c in p['changes']),'cross source patch')
            for c in p['changes']: require(digest(sources[repo]/c['path'])==c['preimage_sha256'],'patch preimage mismatch')
            cmd.run(['git','apply','--check','--whitespace=error',str(path)],'patch-check-'+str(i),sources[repo])
            cmd.run(['git','apply','--whitespace=error',str(path)],'patch-apply-'+str(i),sources[repo])
        for repo,tree in sources.items(): verify_map(tree,expected[repo])
        receipt['sources']={r:{k:v for k,v in s.items() if k!='files'} for r,s in frozen['sources'].items()}
        receipt['patches']=patches
        cg,engine=sources['cg'],sources['engine']
        for b in frozen['model_bindings']: require(digest(cg/b['path'])==b['sha256'],'model/gate seam changed')
        overlay_path=overlay(cg,engine); expected['cg']['.cargo/config.toml']=digest(overlay_path)
        shutil.copyfile(overlay_path,out/'cargo-overlay.toml')
        # The existing helper defaults and real public model checks remain untouched.
        cache=Path(os.environ['CG_MODEL_CACHE_ROOT']).resolve()
        require(cache==Path(os.environ['RUNNER_TEMP']).resolve()/'cg-model-cache','model cache root mismatch')
        original=(cg/'Cargo.lock').read_text()
        metadata=json.loads(cmd.run(CARGO+metadata_flags,'metadata-generate',cg,env).stdout)
        generated=(cg/'Cargo.lock').read_text()
        import difflib
        (out/'lock.diff').write_text(''.join(difflib.unified_diff(original.splitlines(True),generated.splitlines(True))))
        shutil.copyfile(cg/'Cargo.lock',out/'generated-Cargo.lock')
        receipt['lock_replacements']=approve_lock(original,generated)
        receipt['generated_lock_sha256']=digest(cg/'Cargo.lock'); expected['cg']['Cargo.lock']=digest(cg/'Cargo.lock')
        receipt['metadata_sha256']=sha(json.dumps(metadata,sort_keys=True).encode())
        receipt['default_graph']=compare_metadata(baseline,metadata,engine,frozen,original,generated,out)
        write_json(out/'graph-comparison.json',receipt['default_graph']['comparison'])
        cmd.run(CARGO+['tree','--locked','--edges','features'],'feature-graph-default',cg,env)
        require(digest(engine/'Cargo.lock')==digest(out/'engine-original-Cargo.lock'),'engine lock changed')
        # Compile before seeding so the changed engine must produce a fresh compiler artifact.
        row=frozen['cases']['A'][0]
        packages=[p for p in metadata['packages'] if p['name']=='context-graph' and Path(p['manifest_path']).resolve()==(cg/'crates/context-graph/Cargo.toml').resolve()]
        require(len(packages)==1,'CG package identity mismatch')
        build=cmd.run(CARGO+['test','--locked','-p','context-graph','--test','acceptance','--no-run','--message-format=json-render-diagnostics'],'compile',cg,env)
        receipt['binary']=cargo_artifacts(build.stdout.decode(),packages[0]['id'],'acceptance',target,receipt['default_graph']['package_ids']['contextdb-engine'])
        receipt['compiled_feature_vectors']=[{'package_id':e['package_id'],'target':e['target'],'features':e.get('features',[])} for e in map(json.loads,build.stdout.decode().splitlines()) if e.get('reason')=='compiler-artifact']
        # Collect compilation fingerprints without uploading model/cache/source trees.
        for p in target.rglob('*.json'):
            if '.fingerprint' in p.parts:
                dest=out/'fingerprints'/(p.parent.name+'-'+p.name); dest.parent.mkdir(parents=True,exist_ok=True); dest.write_bytes(redact(p.read_bytes(),secrets))
        if os.environ.get('MODEL_CACHE_HIT')!='true':
            cmd.run(['python3','scripts/ci-model-seed.py'],'model-seed',cg,env)
        cmd.run(CARGO+['test','--locked','-p','context-graph',
            '--test','nomic_document_preparer_adds_exactly_one_system_task_prefix',
            '--test','nomic_query_preparer_adds_exactly_one_system_task_prefix',
            '--test','event_search_uses_role_preparation_while_digest_record_and_evidence_paths_remain_byte_for_byte_unchanged'],
            'real-model-proofs',cg,env)
        unfiltered=row['list_argv'][:]; index=unfiltered.index('-E'); del unfiltered[index:index+2]
        all_inventory=json.loads(cmd.run(unfiltered,'inventory-all',cg,env).stdout)
        listed=json.loads(cmd.run(row['list_argv'],'inventory-selected',cg,env).stdout)
        receipt['inventory']=inventory_pair(all_inventory,listed,row,receipt['binary']['path'])
        require(digest(cg/'Cargo.lock')==receipt['generated_lock_sha256'],'lock changed after freeze')
        require(digest(receipt['binary']['path'])==receipt['binary']['sha256'],'binary changed after inventory')
        run=cmd.run(row['run_argv'],'case-1',cg,case_environment(env),timeout=660,required=False)
        junit=target/'nextest/gate/junit.xml'; (out/'junit.xml').write_bytes(redact(junit.read_bytes(),secrets))
        finish_case(out,receipt,row,run.returncode,junit.read_bytes())
    except BaseException as e:
        receipt['error']=type(e).__name__+': '+str(e)
    finally:
        cmd.stop()
        if (out/'case-1.stdout').is_file() and (out/'case-1.stderr').is_file():
            receipt['trace_capture']=trace_presence((out/'case-1.stdout').read_bytes(), (out/'case-1.stderr').read_bytes())
            if receipt['trace_capture']['status']=='MISSING_TRACE':
                receipt['capture_complete']=False
                receipt['error']=receipt.get('error','')+'; MISSING_TRACE: no emitted trace lines retained'
                if receipt['verdict']=='PASS': receipt['verdict']='FAIL'
        try:
            receipt['source_post']={r:file_map(p) for r,p in sources.items()}
            require(set(sources)=={'cg','engine'} and set(sources)==set(expected) and all(receipt['source_post'][r]==expected[r] for r in expected), 'source-post drift/incomplete source')
            receipt['source_post_verified']=True
            receipt['process_cleanup']={'success':cmd.active is None}
        except Exception as e:
            receipt['source_post_verified']=False; receipt['source_post_error']=str(e); receipt['verdict']='FAIL'
        if temp is not None:
            # Retain sources until the always-upload step. Cleanup is a distinct, fixed, owned path operation.
            write_json(out/'cleanup-plan.json',{'owned_root':str(temp),'runner_temp':os.environ['RUNNER_TEMP'],'phase':'subscription_trace'})
        try:
            cmd.run(['df','-k','.'],'disk-after',root,timeout=10,required=False)
        except Exception as e: receipt['resource_post_error']=str(e)
        cmd.save()
    return 0 if receipt['verdict']=='PASS' else 1


def cleanup(root):
    out=root/'target/macos-diagnostics'
    receipt=json.loads((out/'receipt.json').read_text())
    plan=out/'cleanup-plan.json'
    try:
        if plan.exists():
            d=json.loads(plan.read_text()); p=Path(d['owned_root']); parent=Path(os.environ['RUNNER_TEMP']).resolve()
            require(str(p)==receipt.get('owned_root') and p.parent==parent and p.name.startswith('mac-diagnostic-') and not p.is_symlink() and d['phase']=='subscription_trace','unsafe cleanup ownership')
            shutil.rmtree(p); require(not p.exists(),'owned root remains')
        receipt['cleanup']={'success':True,'source_captured_before_cleanup':receipt.get('source_post_verified',False)}
    except Exception as e:
        receipt['cleanup']={'success':False,'error':str(e)}
    write_json(out/'receipt.json',receipt)
    return 0 if receipt['cleanup']['success'] else 1


if __name__=='__main__':
    root=Path(__file__).resolve().parents[1]
    require(len(sys.argv)==2 and sys.argv[1] in ('capture','cleanup'),'fixed capture or cleanup action required')
    sys.exit(capture(root) if sys.argv[1]=='capture' else cleanup(root))
