#!/usr/bin/env python3
"""Arm a model-free observer before a separately authorized exact-branch push.
This utility does not create commits, publish, dispatch workflows or change refs.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time

OBSERVER=Path(__file__).with_name('observe-macos-diagnostics.py')
spec=importlib.util.spec_from_file_location('observer',OBSERVER)
o=importlib.util.module_from_spec(spec);spec.loader.exec_module(o)


def publication_binding(dispatch_path, checkout, d):
    runner=checkout/'scripts/ci-macos-diagnostics.py'
    spec=importlib.util.spec_from_file_location('publication_validator',runner)
    m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
    frozen=m.load_frozen(checkout)
    manifest_path=dispatch_path.parent/'reviewed-content.json'
    manifest=json.loads(manifest_path.read_text())
    o.require(o.sha(m.canonical(manifest))==d['reviewed_content_manifest_sha256'], 'reviewed manifest hash mismatch')
    o.require(manifest==m.reviewed_manifest(checkout), 'actual tooling differs from approved content')
    # Compare every inherited entry, as well as every addition, against actual committed trees.
    # No actual commit ID is stored inside the reviewed manifest or committed activation.
    def tree(ref):
        raw=subprocess.check_output(['git','-C',str(checkout),'ls-tree','-r','-z',ref])
        result={}
        for entry in raw.split(b'\0'):
            if not entry:continue
            meta,path=entry.split(b'\t',1);mode,kind,blob=meta.decode().split()
            o.require(kind=='blob', 'submodules forbidden in diagnostic publication')
            result[path.decode()]={'mode':mode,'blob':blob}
        return result
    before,after=tree(o.BASE),tree(d['tooling_sha'])
    additions=set(m.REVIEWED_PATHS)|{m.ACTIVATION_PATH}
    expected_base={'.github/workflows/macos-diagnostics.yml': {'blob': 'c73c8e22e1ba889b8376701c8b084ae8974c7bc4', 'mode': '100644'}, 'scripts/ci-macos-diagnostics.py': {'blob': 'ee6ba21ea8504f2527e43ce219579e86b44dda63', 'mode': '100644'}, 'scripts/diagnostic-assets/CG_trace.patch': {'blob': '23f50e3bfef4cfac03b79f05b50a501501406be0', 'mode': '100644'}, 'scripts/diagnostic-assets/activation.json': {'blob': 'dac666c2ae6159810edcd140ee76784efeb00e36', 'mode': '100644'}, 'scripts/diagnostic-assets/engine_trace.patch': {'blob': 'eb1da2f006c1fc9216ef5ff21fc2ecd05d78cb9b', 'mode': '100644'}, 'scripts/diagnostic-assets/frozen.json': {'blob': '78cf1f10f44c1330be0377c12d9f7d705739e158', 'mode': '100644'}, 'scripts/diagnostic-assets/nomic.rs': {'blob': 'd25ac48b943d36851ee4fba6b876a62c85b67c53', 'mode': '100644'}, 'scripts/dispatch-macos-diagnostics.py': {'blob': '19ed029b5435e50dce3697c144095601c423cdf9', 'mode': '100644'}, 'scripts/macos-diagnostic-selection.json': {'blob': 'c90959c112bd964bd1b1bdec144bdf8ebb45f634', 'mode': '100644'}, 'scripts/macos-diagnostic-selection.schema.json': {'blob': '9e94c45e4867ae578e931caad4e58d9fe85bac2c', 'mode': '100644'}, 'scripts/observe-macos-diagnostics.py': {'blob': '1256ee386dbf787c3922b47ea41c4fda61ffd796', 'mode': '100644'}}
    o.require({p:before.get(p) for p in additions}==expected_base, 'diagnostic parent content mismatch')
    o.require(set(after)==set(before) and all(after[p]==v for p,v in before.items() if p not in additions),
              'actual T changes inherited source or has missing/extra diagnostic paths')
    for path in additions:
        o.require(after[path]['mode']=='100644', 'published file mode mismatch')
        raw=subprocess.check_output(['git','-C',str(checkout),'show',d['tooling_sha']+':'+path])
        o.require(raw==(checkout/path).read_bytes(), 'worktree/committed bytes differ')
        if path!=m.ACTIVATION_PATH:
            o.require(m.content_entry(path,raw,after[path]['mode'])==manifest['files'][path], 'unapproved activation delta')
    config=json.loads((checkout/m.SELECTION_PATH).read_text())
    m.selection(config,frozen)
    o.require(config['activation_receipt_sha256']==d['activation_receipt_sha256'], 'selection activation hash differs')
    authority=json.loads((checkout/m.ACTIVATION_PATH).read_text())
    m.activation_binding(authority,manifest,frozen)
    o.require(authority['diagnostic_publication_approval_receipt_sha256']==d['diagnostic_publication_approval_receipt_sha256'],
              'activation source approval hash differs from local admission')
    for key,name,verdict in [('normal_hook_commit_receipt_sha256','normal-hook.json','PASS'),
                             ('diagnostic_publication_approval_receipt_sha256','diagnostic-publication-approval.json',m.APPROVAL_VERDICT),
                             ('current_inherited_trigger_recheck_receipt_sha256','trigger-recheck.json','PASS')]:
        path=dispatch_path.parent/'receipts'/name
        o.require(o.sha(path.read_bytes())==d[key], 'missing/mismatched concrete approval receipt: '+name)
        receipt=json.loads(path.read_text())
        o.require(receipt.get('verdict')==verdict, 'approval verdict mismatch: '+name)
        if key=='diagnostic_publication_approval_receipt_sha256':
            o.require(receipt.get('schema')=='mac-diagnostic-source-approval-v1' and
                      receipt.get('reviewed_content_manifest_sha256')==d['reviewed_content_manifest_sha256'] and
                      all(receipt.get(k)==v for k,v in m.source_scope(frozen).items()) and
                      receipt.get('fresh') is True and receipt.get('independent') is True and
                      isinstance(receipt.get('reviewer_family'),str) and bool(receipt['reviewer_family']) and
                      isinstance(receipt.get('implementation_family'),str) and bool(receipt['implementation_family']) and
                      receipt['reviewer_family']!=receipt['implementation_family'], 'fresh outside-family diagnostic approval required')
        else:
            o.require(receipt.get('tooling_sha')==d['tooling_sha'] and receipt.get('tooling_tree')==d['tooling_tree'],
                      'approval receipt does not cover actual tooling T/tree: '+name)
    return {'tooling_sha':d['tooling_sha'],'tooling_tree':d['tooling_tree'],
            'reviewed_content_manifest_sha256':d['reviewed_content_manifest_sha256'],
            'allowed_activation_delta':[m.ACTIVATION_PATH,m.SELECTION_PATH+':activation_receipt_sha256']}


def arm(dispatch_path,out,checkout):
    d=o.dispatch_binding(json.loads(dispatch_path.read_text()))
    o.require(d['run_id'] is None,'new activation must discover its exact run')
    def git(*args): return subprocess.check_output(['git','-C',str(checkout),*args],text=True).strip()
    o.require(git('rev-parse','HEAD')==d['tooling_sha'] and git('rev-parse','HEAD^{tree}')==d['tooling_tree'] and
              git('rev-parse','HEAD^@')==o.BASE and git('status','--porcelain','--untracked-files=no')=='','tooling commit/parent/tree/status mismatch')
    selection=checkout/'scripts/macos-diagnostic-selection.json'
    o.require(o.sha(selection.read_bytes())==d['selection_sha256'],'selection not bound to genuine commit')
    activation=checkout/'scripts/diagnostic-assets/activation.json'
    o.require(o.sha(activation.read_bytes())==d['activation_receipt_sha256'],'activation not bound')
    admission=publication_binding(dispatch_path,checkout,d)
    out.mkdir(parents=True,exist_ok=False)
    # Persist exact plan before process start or external publication. Parent retains explicit ownership.
    o.save(out/'dispatch.json',d)
    o.save(out/'publication-binding.json',admission)
    observer_deadline=time.time()+7800
    o.save(out/'state.json',{'deadline':observer_deadline,'status':'STARTING','dispatch_sha256':o.sha(json.dumps(d,sort_keys=True).encode())})
    with (out/'observer.stdout').open('wb') as stdout,(out/'observer.stderr').open('wb') as stderr:
        p=subprocess.Popen([sys.executable,str(OBSERVER),'observe','--dispatch',str(out/'dispatch.json'),'--out',str(out)],
                           stdin=subprocess.DEVNULL,stdout=stdout,stderr=stderr,start_new_session=True)
    ownership={'pid':p.pid,'deadline':observer_deadline,'observer_sha256':o.sha(OBSERVER.read_bytes()),
               'dispatch_sha256':o.sha(json.dumps(d,sort_keys=True).encode()),'publication_performed':False,
               'publication_argv':['git','-C',str(checkout),'push','https://github.com/'+o.REPO+'.git','HEAD:refs/heads/'+o.BRANCH]}
    o.save(out/'ownership.json',ownership)
    deadline=time.time()+15
    while time.time()<deadline:
        state=out/'state.json'
        if state.exists():
            s=json.loads(state.read_text())
            if (s.get('status')=='READY' and s.get('pid')==p.pid and s.get('dispatch_sha256')==ownership['dispatch_sha256'] and
                    s.get('successful_discoveries',0)>=1 and s.get('discovery_endpoint')=='repository-runs' and
                    s.get('deadline')==ownership['deadline'] and p.poll() is None):
                ownership['status']='READY';o.save(out/'ready.json',ownership);return ownership
        o.require(p.poll() is None,'observer exited before READY')
        time.sleep(.1)
    p.terminate();p.wait(timeout=5)
    raise TimeoutError('observer did not become READY; do not push')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--dispatch',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--checkout',type=Path,required=True)
    a=p.parse_args();print(json.dumps(arm(a.dispatch.resolve(),a.out.resolve(),a.checkout.resolve()),indent=2))
