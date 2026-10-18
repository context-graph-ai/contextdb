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
BASE='ef16fe66365bb3d1b39c96d22403187610ab8958'
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


def receipt_check(capture,final,d):
    require(capture['actual_tooling_sha']==d['tooling_sha'] and final['actual_tooling_sha']==d['tooling_sha'] and
            final['actual_tooling_tree']==d['tooling_tree'] and final['phase']==d['phase'] and
            str(final['run_id'])==str(d['run_id']) and int(final['run_attempt'])==1 and final['event']=='push' and
            final['ref']=='refs/heads/'+BRANCH,'receipt run/source identity mismatch')
    require(final['selection']['activation_receipt_sha256']==d['activation_receipt_sha256'] and
            final['selection']['frozen_sha256']==FROZEN and final['selection_sha256']==d['selection_sha256'],'receipt activation mismatch')
    require(final['source_post_verified'] is True and final['cleanup']['success'] is True and
            final['cleanup']['source_captured_before_cleanup'] is True and final['process_cleanup']['success'] is True,'cleanup/source evidence incomplete')
    require({r:s['commit'] for r,s in final['sources'].items()}==d['sources'],'fetched source mismatch')
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


def verify_raw_evidence(directory, receipt, dispatch):
    runner=Path(__file__).with_name('ci-macos-diagnostics.py')
    spec=importlib.util.spec_from_file_location('diagnostic_validator',runner)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    frozen=m.load_frozen(runner.parents[1])
    verify_case_transport(directory,receipt,dispatch,m,frozen)
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
    require(graph==receipt['default_graph'],'raw metadata graph differs')
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
        r=receipt_check(capture,final,bound)
        verify_raw_evidence(extracted['capture'],final,bound)
        for c in final['commands']:
            require(c['complete'] is True and c['exit'] is not None,'partial command receipt')
            for stream in ('stdout','stderr'):
                if c['name'].endswith('-archive') and stream=='stdout':continue
                path=extracted['capture']/(c['name']+'.'+stream)
                require(path.is_file() and sha(path.read_bytes())==c[stream+'_sha256'],'raw command digest mismatch')
        cases=list(ET.parse(extracted['capture']/'junit.xml').getroot().iter('testcase'))
        require(len(cases)==1 and cases[0].get('name')==CASE and cases[0].get('classname')==BINARY and cases[0].find('skipped') is None,'raw JUnit accounting mismatch')
        failed_xml=cases[0].find('failure') is not None or cases[0].find('error') is not None
        require(failed_xml==bool(r['failed']),'raw failure differs from receipt')
        state.update(status='COLLECTED',result='TEST_FAIL' if r['failed'] else 'DIAGNOSTIC_CASE_PASS',product_qualification='UNQUALIFIED',finished=clock())
        save(state_path,state);return state
    state.update(status='EVIDENCE_FAIL',error='bounded discovery/collection deadline expired');save(state_path,state)
    raise TimeoutError(state['error'])


def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','observe']);p.add_argument('--dispatch',type=Path,required=True);p.add_argument('--out',type=Path,required=True)
    a=p.parse_args();d=json.loads(a.dispatch.read_text())
    if a.action=='prepare':
        # No activation and no live process: verify exact sources after a genuine commit, prepare bindings without claiming live readiness.
        dispatch_binding(d);a.out.mkdir(parents=True,exist_ok=False)
        save(a.out/'ready.json',{'status':'PREPARED','dispatch_sha256':sha(json.dumps(d,sort_keys=True).encode()),'observer_sha256':sha(Path(__file__).read_bytes()),'live_process_started':False,'activation_issued':False})
    else:
        try: observe(d,a.out)
        except BaseException as error:
            state_path=a.out/'state.json'
            if state_path.exists():
                state=json.loads(state_path.read_text());state.update(status='EVIDENCE_FAIL',error=type(error).__name__+': '+str(error),finished=time.time());save(state_path,state)
            raise


if __name__=='__main__': main()
