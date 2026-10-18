#!/usr/bin/env python3
"""Model-free durable read-only collector. No push, dispatch, rerun or cancellation.
Run only after a manager fills and admits dispatch.json; --prepare starts no daemon.
"""
import argparse
import hashlib
import io
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import time
import zipfile
import xml.etree.ElementTree as ET

REPO='context-graph-ai/contextdb'
BRANCH='ci-macos-diagnostic-295'
BASE='2358c71db6ceedea174d7e9ceb5a1eec0bbe836f'
FROZEN='f5066c842195aeba87fb0943fb4eebbb00093fb2d0fde692f641482339b44f53'
CASE='subscriptions::subscription_filter_context_ids_routes_msp_subscriber_across_authorized_contexts_with_single_sink_count'
BINARY='context-graph::acceptance'


def require(ok,message):
    if not ok: raise ValueError(message)


def sha(raw): return hashlib.sha256(raw).hexdigest()


def save(path,data):
    t=path.with_suffix('.tmp'); t.write_text(json.dumps(data,indent=2,sort_keys=True)+'\n'); t.replace(path)


def dispatch_binding(d):
    require(d['schema']=='mac-diagnostic-dispatch-v1' and d['repository']==REPO and d['branch']==BRANCH and
            d['event']=='push' and d['workflow_path']=='.github/workflows/macos-diagnostics.yml' and
            d['tooling_parent']==BASE and d['phase']=='subscription_trace' and d['frozen_sha256']==FROZEN and
            d['binary']==BINARY and d['case']==CASE,'dispatch scope mismatch')
    require(d['sources']=={'engine':'db9d067f291c0add0922713936a63f51f24e9f1c','cg':'db6d20cfc32a8bdf6a0753f3b629bbf94ab2f8af'},'dispatch source mismatch')
    for k,n in [('tooling_sha',40),('tooling_tree',40),('activation_receipt_sha256',64),('selection_sha256',64),('normal_hook_commit_receipt_sha256',64),
                ('diagnostic_publication_approval_receipt_sha256',64),('current_inherited_trigger_recheck_receipt_sha256',64),('reviewed_content_manifest_sha256',64)]:
        require(isinstance(d[k],str) and re.fullmatch('[a-f0-9]{'+str(n)+'}',d[k]),'null/nonexact dispatch binding: '+k)
    require(d['activation_approved'] is True and d['run_attempt']==1,'activation/attempt not admitted')
    callback_config(d.get('callback'))
    require(d['run_id'] is None or isinstance(d['run_id'],int),'run id must be exact integer or unbound until discovery')
    return d


def match_run(run,d):
    return (run.get('head_sha')==d['tooling_sha'] and run.get('head_branch')==BRANCH and
            run.get('event')=='push' and run.get('path')==d['workflow_path'] and run.get('run_attempt')==1 and
            run.get('repository',{}).get('full_name')==REPO and
            (d['run_id'] is None or run.get('id')==d['run_id']))


def check_digest(raw,api_digest):
    require(api_digest=='sha256:'+sha(raw),'API artifact digest mismatch/missing')


def unzip(raw,destination):
    destination.mkdir(exist_ok=False)
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        names=z.namelist(); require(len(names)==len(set(names)),'duplicate artifact member')
        for info in z.infolist():
            p=PurePosixPath(info.filename)
            require(not p.is_absolute() and '..' not in p.parts and (info.external_attr>>16)&0o170000 != 0o120000,'unsafe artifact member')
            require(info.file_size<100_000_000 and sum(x.file_size for x in z.infolist())<500_000_000,'artifact expansion bound exceeded')
            target=destination.joinpath(*p.parts)
            if info.is_dir(): target.mkdir(parents=True,exist_ok=True)
            else:
                target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(z.read(info))


def receipt_identity(capture,final,d):
    require(capture['actual_tooling_sha']==d['tooling_sha'] and final['actual_tooling_sha']==d['tooling_sha'] and
            final['actual_tooling_tree']==d['tooling_tree'] and final['phase']==d['phase'] and
            str(final['run_id'])==str(d['run_id']) and int(final['run_attempt'])==1 and final['event']=='push' and
            final['ref']=='refs/heads/'+BRANCH,'receipt run/source identity mismatch')
    require(final['selection']['activation_receipt_sha256']==d['activation_receipt_sha256'] and
            final['selection']['frozen_sha256']==FROZEN and final['selection_sha256']==d['selection_sha256'],'receipt activation mismatch')
    require(final['source_post_verified'] is True and final['cleanup']['success'] is True and
            final['cleanup']['source_captured_before_cleanup'] is True and final['process_cleanup']['success'] is True,'cleanup/source evidence incomplete')
    require({r:s['commit'] for r,s in final['sources'].items()}==d['sources'],'fetched source mismatch')
    require(final['product_qualification']=='UNQUALIFIED','diagnostics cannot grant PR qualification')


def receipt_check(capture,final,d):
    receipt_identity(capture,final,d)
    require(final['generated_lock_sha256'] and final['metadata_sha256'] and final['default_graph']['package_ids'] and
            final['binary']['sha256'] and final['inventory'],'compile/lock/inventory proof missing')
    rows=final['runs'];require(len(rows)==1 and final['unrun']==0,'partial execution receipt')
    r=rows[0];require(r['case']==CASE and r['binary']==BINARY and r['executed']==1 and r['skipped']==0 and
                     r['passed']+r['failed']==1,'case accounting mismatch')
    require((r['exit']==0 and r['passed']==1 and final['verdict']=='PASS') or
            (r['exit']>0 and r['failed']==1 and final['verdict']=='TEST_FAIL'),'case failure incorrectly promoted')
    require(final.get('capture_complete') is True and final.get('trace_capture',{}).get('status')=='PRESENT',
            'MISSING_TRACE: incomplete capture evidence')
    require(final['product_qualification']=='UNQUALIFIED','diagnostics cannot grant PR qualification')
    return r


def verify_graph_evidence(directory, receipt, dispatch):
    runner=Path(__file__).with_name('ci-macos-diagnostics.py')
    spec=importlib.util.spec_from_file_location('diagnostic_validator',runner)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    frozen=m.load_frozen(runner.parents[1])
    expected=m.expected_maps(frozen,[frozen['patches'][x] for x in m.PHASES['subscription_trace']])
    lock=(directory/'generated-Cargo.lock').read_text()
    m.approve_lock((directory/'cg-original-Cargo.lock').read_text(),lock)
    require(sha(lock.encode())==receipt['generated_lock_sha256'],'derived lock digest differs')
    expected['cg']['Cargo.lock']=sha(lock.encode())
    require((directory/'cargo-overlay.toml').read_text()==m.overlay_text(Path(receipt['owned_root'])/'engine'),'overlay must bind all eleven exact phase paths')
    expected['cg']['.cargo/config.toml']=sha((directory/'cargo-overlay.toml').read_bytes())
    require(receipt['source_post']==expected,'full source-post map differs from frozen image/lock/overlay')
    metadata=json.loads((directory/'metadata-generate.stdout').read_bytes())
    require(sha(json.dumps(metadata,sort_keys=True).encode())==receipt['metadata_sha256'],'raw metadata hash differs')
    baseline=json.loads((directory/'metadata-baseline.stdout').read_bytes())
    require(sha(json.dumps(baseline,sort_keys=True).encode())==receipt['baseline_metadata_sha256'],'baseline metadata hash differs')
    require(receipt['baseline_source_maps']=={r:v['files'] for r,v in frozen['sources'].items()},'baseline not pristine source maps')
    require(receipt['metadata_flags']==['metadata','--format-version','1'],'metadata flags differ')
    metadata_commands=[]
    for name,extra in (('metadata-baseline',['--locked']),('metadata-generate',[])):
        commands=[(i,c) for i,c in enumerate(receipt['commands']) if c['name']==name]
        require(len(commands)==1,'missing/duplicate metadata command')
        i,c=commands[0];metadata_commands.append(i)
        require(c['argv']==m.CARGO+receipt['metadata_flags']+extra and c['cwd']==str(Path(receipt['owned_root'])/'cg') and c['complete'] is True and c['exit']==0,'metadata command flags/source mismatch')
    require(metadata_commands[0]<metadata_commands[1],'baseline must precede overlay metadata')
    graph=m.compare_metadata(baseline,metadata,Path(receipt['owned_root'])/'engine',frozen,(directory/'cg-original-Cargo.lock').read_text(),lock)
    require(sha((directory/'engine-original-Cargo.lock').read_bytes())==frozen['sources']['engine']['files']['Cargo.lock'],'original engine lock mismatch')
    require(sha((directory/'cg-original-Cargo.lock').read_bytes())==frozen['sources']['cg']['files']['Cargo.lock'],'original CG lock mismatch')
    if receipt.get('default_graph') is None:
        require(receipt.get('capture_complete') is False, 'complete capture missing graph')
        return m,frozen,graph,metadata
    require(graph==receipt['default_graph'],'raw metadata graph differs')
    return m,frozen,graph,metadata


def verify_raw_evidence(directory, receipt, dispatch):
    m,frozen,graph,metadata=verify_graph_evidence(directory,receipt,dispatch)
    verify_case_transport(directory,receipt,dispatch,m,frozen)
    verify_compiled_evidence(directory,receipt,m,frozen,graph,metadata)


def verify_compiled_evidence(directory,receipt,m,frozen,graph,metadata):
    packages=[p for p in metadata['packages'] if p['name']=='context-graph' and p['manifest_path']==str(Path(receipt['owned_root'])/'cg/crates/context-graph/Cargo.toml')]
    require(len(packages)==1,'actual CG package missing')
    events=[json.loads(line) for line in (directory/'compile.stdout').read_text().splitlines() if line.strip()]
    require([e.get('success') for e in events if e.get('reason')=='build-finished']==[True],'raw compilation incomplete')
    engine=[e for e in events if e.get('reason')=='compiler-artifact' and e.get('package_id')==graph['package_ids']['contextdb-engine']]
    require(engine and all(e.get('fresh') is False for e in engine),'raw changed engine compilation missing')
    targets=[e for e in events if e.get('reason')=='compiler-artifact' and e.get('package_id')==packages[0]['id'] and e.get('target',{}).get('name')=='acceptance' and e.get('executable')]
    require(len(targets)==1 and targets[0]==receipt['binary']['cargo_artifact'],'raw compiled test artifact differs')
    listed=m.inventory_pair(json.loads((directory/'inventory-all.stdout').read_bytes()),json.loads((directory/'inventory-selected.stdout').read_bytes()),frozen['cases']['A'][0],receipt['binary']['path'])
    require(listed==receipt['inventory'],'raw inventory differs from receipt')
    require(sha((directory/'engine-original-Cargo.lock').read_bytes())==frozen['sources']['engine']['files']['Cargo.lock'],'original engine lock mismatch')
    require(sha((directory/'cg-original-Cargo.lock').read_bytes())==frozen['sources']['cg']['files']['Cargo.lock'],'original CG lock mismatch')


def verify_command_outputs(directory,receipt):
    commands=receipt['commands']
    require(isinstance(commands,list) and commands and len({c['name'] for c in commands})==len(commands),
            'missing/duplicate command timeline')
    for c in commands:
        require(c['complete'] is True and type(c['exit']) is int, 'partial command receipt')
        for stream in ('stdout','stderr'):
            if c['name'].endswith('-archive') and stream=='stdout':continue
            path=directory/(c['name']+'.'+stream)
            require(path.is_file() and sha(path.read_bytes())==c[stream+'_sha256'],'raw command digest mismatch')


def verify_incomplete_identity(directory,receipt,d):
    runner=Path(__file__).with_name('ci-macos-diagnostics.py')
    spec=importlib.util.spec_from_file_location('incomplete_identity_validator',runner)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    frozen=m.load_frozen(runner.parents[1])
    manifest=m.reviewed_manifest(runner.parents[1])
    require(m.sha(m.canonical(manifest))==d.get('reviewed_content_manifest_sha256'),
            'incomplete reviewed content/dispatch mismatch')
    raw={}
    for name in ('activation','selection'):
        path=directory/('raw-'+name+'.json')
        require(path.is_file() and not path.is_symlink(),
                'UNVERIFIED identity gap: missing retained raw '+name+' bytes')
        raw[name]=path.read_bytes()
    require(sha(raw['activation'])==d.get('activation_receipt_sha256'),
            'retained activation bytes digest mismatch')
    authority=json.loads(raw['activation'])
    require(isinstance(authority,dict),'activation object required')
    m.activation_binding(authority,manifest,frozen)
    require(authority==receipt.get('activation'),'retained activation/decoded receipt mismatch')
    require(authority['diagnostic_publication_approval_receipt_sha256']==
            d.get('diagnostic_publication_approval_receipt_sha256'), 'activation source approval/dispatch mismatch')
    require(sha(raw['selection'])==d.get('selection_sha256')==receipt.get('selection_sha256'),
            'retained selection bytes digest mismatch')
    config=json.loads(raw['selection'])
    require(isinstance(config,dict),'selection object required')
    m.selection(config,frozen)
    require(config==receipt.get('selection') and config['activation_receipt_sha256']==sha(raw['activation']),
            'retained selection/activation/decoded receipt mismatch')
    require(m.content_entry(m.SELECTION_PATH,raw['selection'],'100644')==manifest['files'][m.SELECTION_PATH],
            'retained selection reviewed content mismatch')
    sources=receipt.get('sources')
    require(isinstance(sources,dict) and set(sources)==set(frozen['sources']), 'incomplete source tuple missing')
    commands=receipt.get('commands',[])
    identity={}
    for repo,source in frozen['sources'].items():
        require(isinstance(sources[repo],dict) and sources[repo].get('commit')==source['commit'] and sources[repo].get('tree')==source['tree'],
                'fetched source commit/tree mismatch: '+repo)
        identity[repo]={}
        for suffix,expected,argv in (('head',source['commit'],['git','rev-parse','FETCH_HEAD']),
                                     ('tree',source['tree'],['git','rev-parse',source['commit']+'^{tree}'])):
            name=repo+'-'+suffix
            rows=[c for c in commands if c.get('name')==name]
            if not rows:
                identity[repo][suffix]='UNRUN_UNVERIFIED'
                continue
            require(len(rows)==1 and rows[0].get('argv')==argv and rows[0].get('complete') is True and
                    rows[0].get('exit')==0 and not rows[0].get('timeout',False), 'fetched source identity command mismatch: '+name)
            path=directory/(name+'.stdout')
            require(path.is_file() and path.read_bytes().decode().strip()==expected,
                    'fetched source identity output mismatch: '+name)
            identity[repo][suffix]='VERIFIED'
    return {'activation':'VERIFIED','selection':'VERIFIED','sources':identity}


def verify_incomplete(directory,capture,final,d,job):
    identity=verify_incomplete_identity(directory,final,d)
    receipt_identity(capture,final,d)
    require(final.get('capture_complete') is False and capture.get('capture_complete') is False and
            isinstance(final.get('error'),str) and final['error'].strip() and
            capture.get('error')==final['error'] and final.get('verdict') in ('FAIL','TEST_FAIL'),
            'incomplete capture requires original failure')
    require(job.get('conclusion')=='failure' and any(s.get('conclusion')=='failure' and
            'capture' in s.get('name','').lower() for s in job.get('steps',[])),
            'incomplete capture requires failed job/capture step')
    require(capture==dict(final,cleanup=capture['cleanup']), 'capture/final receipt drift outside cleanup')
    verify_command_outputs(directory,final)
    require(all(status=='VERIFIED' for source in identity['sources'].values() for status in source.values()),
            'UNVERIFIED identity gap: metadata-stage capture missing fetched source command')
    m,frozen,graph,metadata=verify_graph_evidence(directory,final,d)
    names=[c['name'] for c in final['commands']]
    case_commands=[c for c in final['commands'] if c['name']=='case-1']
    missing=[name for name in ('feature-graph-default','compile','real-model-proofs',
                             'inventory-all','inventory-selected','case-1') if name not in names]
    result={'result':'INCOMPLETE_HARNESS_FAILURE','capture_complete':False,'selected':1,
            'passed':0,'product_qualification':'UNQUALIFIED','primary_error':final['error'],'identity':identity,
            'missing_stages':missing,'trace_capture':final.get('trace_capture'),
            'actual_runs':final['runs'],'case_exit':None,'junit_present':(directory/'junit.xml').is_file()}
    if not case_commands:
        # Only the observed pre-compilation graph failure has a defined zero-case timeline.
        require(final['error'].startswith('ValueError: resolved baseline/overlay graph differs:') and
                final['verdict']=='FAIL' and final['runs']==[] and final['unrun']==1 and
                final.get('default_graph') is None and final.get('binary') is None and
                final.get('inventory') is None and final.get('trace_capture') is None and
                not any(n in names for n in ('feature-graph-default','compile','model-seed',
                    'real-model-proofs','inventory-all','inventory-selected')) and
                not any((directory/n).exists() for n in ('case-1.stdout','case-1.stderr','junit.xml',
                    'compile.stdout','inventory-all.stdout','inventory-selected.stdout')),
                'inconsistent zero-case metadata failure timeline')
        require(names[-1]=='disk-after' and names[-2]=='metadata-generate' and
                all(not c['name'].startswith(('case-','inventory-')) for c in final['commands']),
                'zero-case timeline extends beyond metadata failure')
        require(all(c['exit']==0 and not c.get('timeout',False) for c in final['commands']),
                'metadata failure timeline contains another command failure')
        result.update(case_status='UNRUN',unrun=1,executed=0,failed=0)
    else:
        # Executed/partial case evidence is never relabelled UNRUN or granted a test PASS.
        require(len(case_commands)==1 and all(n in names for n in ('compile','inventory-all','inventory-selected')),
                'case command without prerequisites')
        require(final.get('default_graph') and final.get('binary',{}).get('sha256') and final.get('inventory'), 'partial case missing graph/binary/inventory proof')
        verify_compiled_evidence(directory,final,m,frozen,graph,metadata)
        ordered=[names.index(n) for n in ('compile','inventory-all','inventory-selected','case-1')]
        require(ordered==sorted(ordered), 'case prerequisite timeline out of order')
        c=case_commands[0];row=frozen['cases']['A'][0]
        require(c['argv']==row['run_argv'] and c['cwd']==str(Path(final['owned_root'])/'cg') and
                c.get('environment')==m.CASE_ENV, 'partial case command identity mismatch')
        for command in final['commands']:
            if command['name']!='case-1':require(not any(k in command.get('environment',{}) for k in m.CASE_ENV),'case environment leaked to prerequisite')
        trace=m.trace_presence((directory/'case-1.stdout').read_bytes(),(directory/'case-1.stderr').read_bytes())
        require(final.get('trace_capture')==trace, 'partial trace receipt differs')
        if result['junit_present']:
            try:
                actual=m.junit_result((directory/'junit.xml').read_bytes(),row,c['exit'])
            except (ValueError,ET.ParseError) as error:
                result['junit_error']=type(error).__name__+': '+str(error)
                require(final['runs']==[], 'invalid JUnit has fabricated run accounting')
            else:
                require(final['runs']==[actual], 'partial JUnit/receipt accounting differs')
                result['actual_junit_result']=actual
        else:require(final['runs']==[], 'missing JUnit has fabricated run accounting')
        result.update(case_status='EXECUTED_CAPTURE_INCOMPLETE',unrun=None,executed=1,
                      failed=None,case_exit=c['exit'],trace_capture=trace)
    return result


def verify_case_transport(directory, receipt, dispatch, m, frozen):
    # Re-derive presence from raw bytes and command environment, independently of completion flags.
    require(receipt['phase']=='subscription_trace' and dispatch['phase']=='subscription_trace' and
            {k:v['commit'] for k,v in receipt['sources'].items()}==
            {k:v['commit'] for k,v in frozen['sources'].items()}==dispatch['sources'],
            'trace phase/source mismatch')
    expected_env={'SUBSCRIPTION_EXTERNAL_TRACE':'external-472-v1',
                  'NEXTEST_SUCCESS_OUTPUT':'immediate','NEXTEST_FAILURE_OUTPUT':'immediate'}
    require(receipt.get('case_environment')==expected_env, 'case environment receipt mismatch')
    commands=[c for c in receipt['commands'] if c['name']=='case-1']
    require(len(commands)==1, 'exactly one selected case command required')
    command=commands[0]; row=frozen['cases']['A'][0]
    require(command.get('environment')==expected_env and command['argv']==row['run_argv'] and
            command['cwd']==str(Path(receipt['owned_root'])/'cg') and command['complete'] is True and
            command['exit']==receipt['runs'][0]['exit'] and not command.get('timeout',False),
            'case command environment/source/exit mismatch')
    for c in receipt['commands']:
        if c['name']!='case-1':
            require(not any(k in c.get('environment',{}) for k in expected_env), 'case environment leaked to prerequisite')
    raw={stream:(directory/('case-1.'+stream)).read_bytes() for stream in ('stdout','stderr')}
    for stream,value in raw.items():
        require(sha(value)==command[stream+'_sha256'], 'case raw output digest mismatch')
    actual=m.trace_presence(raw['stdout'],raw['stderr'])
    require(actual['status']=='PRESENT', 'MISSING_TRACE: no emitted trace line retained')
    require(receipt.get('trace_capture')==actual and receipt.get('capture_complete') is True,
            'trace presence receipt differs from retained output')
    return actual


class API:
    def __init__(self,out): self.out=out
    def get(self,path,name,binary=False):
        require(path.startswith('repos/'+REPO+'/actions/'),'read-only API scope')
        p=subprocess.run(['gh','api',path],capture_output=True,timeout=60)
        (self.out/name).write_bytes(p.stdout)
        if p.returncode!=0:
            (self.out/(name+'.stderr')).write_bytes(p.stderr)
        require(p.returncode==0,'read-only API request failed: '+name)
        return p.stdout if binary else json.loads(p.stdout)


def callback_config(config):
    require(isinstance(config,dict) and set(config)=={'schema','argv'} and
            config['schema']=='mac-diagnostic-local-callback-v1', 'local callback config required')
    argv=config['argv']
    require(isinstance(argv,list) and 1<=len(argv)<=32 and
            all(isinstance(x,str) and 0<len(x)<=4096 and not any(c in x for c in ('\x00','\n','\r')) for x in argv) and
            Path(argv[0]).is_absolute(), 'callback must be bounded local absolute-executable argv')
    return argv


def callback(out,state,run_id,config):
    argv=callback_config(config)
    if state.get('failure_callback_sent'): return
    # Only locally supplied argv is executable; remote evidence is appended as one inert argument.
    message='HOSTED_MAC_FAILURE run='+str(run_id)+' evidence='+str(out)
    save(out/'first-failure.json',{'run_id':run_id,'at':time.time(),'message':message,'logs_requested':False})
    p=subprocess.run([*argv,message],shell=False,capture_output=True,timeout=30)
    require(p.returncode==0,'failure callback was not delivered; logs not fetched')
    state['failure_callback_sent']=True;save(out/'state.json',state)


def observe(d,out,api=None,notify=None,clock=time.time,pause=time.sleep):
    dispatch_binding(d)
    if notify is None:
        notify=lambda directory,state,run_id: callback(directory,state,run_id,d['callback'])
    out.mkdir(parents=True,exist_ok=True)
    state_path=out/'state.json'
    state=json.loads(state_path.read_text()) if state_path.exists() else {'failure_callback_sent':False}
    binding=sha(json.dumps(d,sort_keys=True).encode())
    require(state.get('dispatch_sha256',binding)==binding,'observer state belongs to different activation')
    state.update(dispatch_sha256=binding,pid=os.getpid(),deadline=state.get('deadline',clock()+7800),status='STARTING',successful_discoveries=0)
    save(state_path,state);api=api or API(out)
    while clock()<state['deadline']:
        response=api.get('repos/'+REPO+'/actions/runs?branch='+BRANCH+'&event=push&per_page=100','runs.json')
        runs=response['workflow_runs']
        require(isinstance(runs,list) and len(runs)<=100,'invalid repository run page')
        require(response.get('total_count',len(runs))<=100,'repository discovery exceeds bounded page; narrow scope required')
        matches=[r for r in runs if match_run(r,d)]
        require(len(matches)<=1,'multiple matching workflow runs; no latest-run fallback')
        state.update(status='READY' if not matches else 'COLLECTING',successful_discoveries=state['successful_discoveries']+1,last_discovery=clock(),discovery_endpoint='repository-runs')
        save(state_path,state)
        if not matches: pause(20);continue
        run=matches[0];run_id=run['id'];state['run_id']=run_id;save(state_path,state)
        if run.get('conclusion') in ('failure','cancelled','timed_out','action_required','startup_failure'): notify(out,state,run_id)
        jobs=api.get('repos/'+REPO+'/actions/runs/'+str(run_id)+'/attempts/1/jobs?per_page=100','jobs.json')
        if jobs['total_count']==0 and run['status']!='completed': pause(20);continue
        require(jobs['total_count']==1 and len(jobs['jobs'])==1,'single image job required')
        failed=run.get('conclusion') in ('failure','cancelled','timed_out','action_required','startup_failure') or any(
            j.get('conclusion') in ('failure','cancelled','timed_out','skipped') or any(s.get('conclusion') in ('failure','cancelled','timed_out') for s in j.get('steps',[])) for j in jobs['jobs'])
        if failed: notify(out,state,run_id)
        if run['status']!='completed': pause(20);continue
        if run.get('conclusion')!='success': notify(out,state,run_id)
        bound=dict(d,run_id=run_id)
        api.get('repos/'+REPO+'/actions/runs/'+str(run_id)+'/attempts/1/logs','logs.zip',binary=True)
        artifacts=api.get('repos/'+REPO+'/actions/runs/'+str(run_id)+'/artifacts?per_page=100','artifacts.json')
        names={f'mac-diagnostics-{run_id}-1-{kind}' for kind in ('capture','cleanup')}
        require(artifacts['total_count']==2 and len(artifacts['artifacts'])==2 and {a['name'] for a in artifacts['artifacts']}==names,'capture/cleanup artifacts missing/extra')
        extracted={}
        for a in artifacts['artifacts']:
            require(not a['expired'] and a['workflow_run']['id']==run_id and a['workflow_run']['head_sha']==d['tooling_sha'],'artifact source mismatch')
            raw=api.get('repos/'+REPO+'/actions/artifacts/'+str(a['id'])+'/zip',a['name']+'.zip',binary=True)
            check_digest(raw,a.get('digest'))
            kind=a['name'].rsplit('-',1)[1];dest=out/kind;unzip(raw,dest);extracted[kind]=dest
        capture=json.loads((extracted['capture']/'receipt.json').read_text())
        final=json.loads((extracted['cleanup']/'receipt.json').read_text())
        if final.get('capture_complete') is False:
            # Retain actual case commands/bytes even if validation later rejects partial evidence.
            case_commands=[c for c in final.get('commands',[]) if c.get('name')=='case-1']
            partial_raw={name:{'present':(extracted['capture']/name).is_file(),
                              'sha256':sha((extracted['capture']/name).read_bytes()) if (extracted['capture']/name).is_file() else None}
                         for name in ('case-1.stdout','case-1.stderr','junit.xml')}
            save(out/'partial-evidence.json',{'primary_error':final.get('error'),'identity_status':'UNVERIFIED',
                'case_commands':case_commands,'runs':final.get('runs'),'raw':partial_raw,
                'case_status':'EXECUTED_CAPTURE_INCOMPLETE' if case_commands else 'UNRUN_UNVERIFIED',
                'missing_stages':[name for name in ('compile','inventory-all','inventory-selected','case-1')
                                  if not any(c.get('name')==name for c in final.get('commands',[]))]})
            partial=verify_incomplete(extracted['capture'],capture,final,bound,jobs['jobs'][0])
            state.update(partial,status='COLLECTED_INCOMPLETE',collector_status='COLLECTED_INCOMPLETE',finished=clock())
            save(state_path,state);return state
        r=receipt_check(capture,final,bound)
        verify_raw_evidence(extracted['capture'],final,bound)
        verify_command_outputs(extracted['capture'],final)
        cases=list(ET.parse(extracted['capture']/'junit.xml').getroot().iter('testcase'))
        require(len(cases)==1 and cases[0].get('name')==CASE and cases[0].get('classname')==BINARY and cases[0].find('skipped') is None,'raw JUnit accounting mismatch')
        failed_xml=cases[0].find('failure') is not None or cases[0].find('error') is not None
        require(failed_xml==bool(r['failed']),'raw failure differs from receipt')
        state.update(status='COLLECTED',result='TEST_FAIL' if r['failed'] else 'DIAGNOSTIC_CASE_PASS',product_qualification='UNQUALIFIED',finished=clock())
        save(state_path,state);return state
    state.update(status='EVIDENCE_FAIL',error='bounded discovery/collection deadline expired');save(state_path,state)
    raise TimeoutError(state['error'])


def final_callback(out,d,state):
    # Local callback receives inert evidence text, including the original root failure when retained.
    primary=state.get('primary_error')
    if primary is None and (out/'capture/receipt.json').is_file():
        primary=json.loads((out/'capture/receipt.json').read_text()).get('error')
    message='HOSTED_MAC_FINAL '+json.dumps({'evidence':str(out),'primary_error':primary,
        'collector_status':state.get('collector_status',state.get('status')),
        'collector_error':state.get('collector_error'),'result':state.get('result')},sort_keys=True)
    p=subprocess.run([*callback_config(d['callback']),message],shell=False,capture_output=True,timeout=30)
    save(out/'final-callback.json',{'message':message,'exit':p.returncode})
    require(p.returncode==0,'final collector callback failed')


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','observe']);p.add_argument('--dispatch',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();d=json.loads(a.dispatch.read_text())
    if a.action=='prepare':
        # No activation and no live process: verify exact sources after a genuine commit, prepare bindings without claiming live readiness.
        dispatch_binding(d);a.out.mkdir(parents=True,exist_ok=False)
        save(a.out/'ready.json',{'status':'PREPARED','dispatch_sha256':sha(json.dumps(d,sort_keys=True).encode()),'observer_sha256':sha(Path(__file__).read_bytes()),'live_process_started':False,'activation_issued':False})
    else:
        try:
            state=observe(d,a.out)
            final_callback(a.out,d,state)
        except BaseException as error:
            state_path=a.out/'state.json'
            if state_path.exists():
                state=json.loads(state_path.read_text());state.update(status='EVIDENCE_FAIL',error=type(error).__name__+': '+str(error),finished=time.time());save(state_path,state)
            try:
                final_callback(a.out,d,dict(state if state_path.exists() else {},collector_status='EVIDENCE_FAIL',collector_error=type(error).__name__+': '+str(error)))
            except Exception:
                pass  # The saved original error remains authoritative when final delivery also fails.
            raise


if __name__=='__main__': main()
