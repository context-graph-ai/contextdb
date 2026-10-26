#!/usr/bin/env python3
"""Fail the stable PR status unless every required job reports success."""
import hashlib
import json
import os
from pathlib import Path
import re
import sys

REQUIRED = {"lint", "lint-macos", "test-bundles", "build", "features",
            "installed-smoke", "cross-compile", "impact", "integration"}


def block_fields(block, indent):
    """Read only the established block-mapping layout; unsupported YAML fails closed."""
    lines = block.splitlines(keepends=True)
    result = {}
    pattern = re.compile(r"^" + " " * indent + r"([\w-]+):(?: (.*))?$")
    key = None
    for line in lines:
        if not line.strip() or line.lstrip().startswith('#'):
            continue
        match = pattern.fullmatch(line.rstrip('\n'))
        if match:
            key = match[1]
            if key in result:
                raise ValueError("duplicate mapping key")
            result[key] = (match[2] or '') + '\n'
        elif key is not None and (line.startswith(' ' * (indent + 1))
                                  or line.startswith(' ' * indent + '- ')):
            result[key] += line
        else:
            raise ValueError("unsupported mapping layout")
    return result


def delivery_inventory(workflow, jobs):
    top_permissions = re.search(r"^permissions:\n(.*?)(?=^\S|\Z)", workflow, re.M | re.S)
    if (len(re.findall(r'^permissions:', workflow, re.M)) != 1 or top_permissions is None
            or block_fields(top_permissions[1], 2) != {'contents': 'read\n'}):
        raise ValueError("workflow permissions must remain contents read")
    blocks = {name: re.search(r"^  " + re.escape(name) + r":\n(.*?)(?=^  [\w-]+:|\Z)",
                              workflow, re.M | re.S)[1] for name in jobs}
    caller = block_fields(blocks['development-binaries'], 4)
    if set(caller) != {'needs', 'if', 'uses', 'permissions'}:
        raise ValueError("delivery caller fields changed")
    condition = ' '.join(caller['if'].strip().removeprefix('${{').removesuffix('}}').split())
    # safe_dump may quote a scalar; remove only its enclosing quotes.
    condition = condition.removeprefix('"').removesuffix('"')
    if (caller['needs'].strip() != '[required]'
            or caller['uses'].strip() != './.github/workflows/build.yml'
            or condition != "github.event_name == 'push' && github.ref == 'refs/heads/dev' && github.repository == 'context-graph-ai/contextdb'"
            or block_fields(caller['permissions'].lstrip('\n'), 6) !=
                {'contents': 'write\n', 'actions': 'read\n'}):
        raise ValueError("delivery caller routing/permissions changed")
    # Canonicalize the complete supported caller mapping after structural validation.
    reviewed = {'needs': ['required'], 'if': condition,
                'uses': caller['uses'].strip(),
                'permissions': {key: value.strip() for key, value in
                                block_fields(caller['permissions'].lstrip('\n'), 6).items()}}
    digest = hashlib.sha256(json.dumps(
        reviewed, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()).hexdigest()
    if digest != '21fea9b7e48f7f3d40a0eb257adc306f6806f601530d343c70bc08902d80a065':
        raise ValueError('delivery caller differs from reviewed baseline')
    for name, block in blocks.items():
        if name == 'development-binaries':
            continue
        grants = re.findall(r'^    permissions:(.*?)(?=^    [\w-]+:|\Z)',
                            block, re.M | re.S)
        if len(grants) > 1:
            raise ValueError('duplicate job permissions')
        if grants:
            permission = grants[0]
            if permission.strip() == 'read-all':
                continue
            entries = block_fields(permission.lstrip('\n'), 6)
            if any(value.strip() not in ('read', 'none') for value in entries.values()):
                raise ValueError("only delivery caller may grant write permissions")


def integration_caller(workflow):
    block = re.search(r"^  integration:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S)
    if block is None:
        raise ValueError("missing normal integration validator")
    fields = block_fields(block[1], 4)
    if (set(fields) != {'needs', 'if', 'runs-on', 'timeout-minutes', 'steps'}
            or fields['needs'].strip() != '[impact, test-bundles]'
            or fields['if'].strip() != 'always()'
            or fields['runs-on'].strip() != 'ubuntu-latest' or fields['timeout-minutes'].strip() != '5'
            or 'python3 -B scripts/ci-integration.py normal-required' not in fields['steps']
            or 'CI_COMBINED_MATRIX: ${{ needs.impact.outputs.combined_matrix }}' not in fields['steps']
            or 'CI_BUNDLE_MATRIX: ${{ needs.impact.outputs.bundle_matrix }}' not in fields['steps']):
        raise ValueError("integration must validate the published matrix after bundle execution")


def root_chain(workflow):
    expected = {'lint': None, 'features': 'lint', 'build': 'lint',
                'cross-compile': None, 'installed-smoke': None,
                'lint-macos': None}
    for name, predecessor in expected.items():
        block = re.search(r"^  " + re.escape(name) + r":\n(.*?)(?=^  [\w-]+:|\Z)",
                          workflow, re.M | re.S)
        if block is None:
            raise ValueError('missing required root-chain job: ' + name)
        fields = block_fields(block[1], 4)
        if predecessor is None:
            if 'needs' in fields or 'if' in fields:
                raise ValueError(name + ' must remain an independent root')
        elif (fields.get('needs', '').strip() != predecessor
              or fields.get('if', '').strip() != 'always() && !cancelled()'):
            raise ValueError(name + ' must run after predecessor failure unless cancelled')


def main():
    try:
        workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml").read_text()
        jobs_block = workflow.split("\njobs:\n")
        if len(jobs_block) != 2:
            raise ValueError("unsupported workflow jobs format")
        job_lines = [line for line in jobs_block[1].splitlines()
                     if line.startswith("  ") and not line.startswith("   ") and line.strip() and not line.lstrip().startswith("#")]
        jobs = []
        for line in job_lines:
            match = re.fullmatch(r"  ([\w-]+):", line)
            if match is None:
                raise ValueError("unsupported workflow job declaration")
            jobs.append(match[1])
        if len(jobs) != len(set(jobs)) or set(jobs) != REQUIRED | {"required", "development-binaries"}:
            raise ValueError("workflow jobs do not match required job inventory")
        delivery_inventory(workflow, jobs)
        integration_caller(workflow)
        root_chain(workflow)
        aggregate = re.search(r"^  required:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S)
        declared = re.findall(r"^    needs: \[([\w, -]+)\]$", aggregate[1], re.M)
        dependencies = [name.strip() for name in declared[0].split(",")] if len(declared) == 1 else []
        if len(dependencies) != len(set(dependencies)) or set(dependencies) != REQUIRED:
            raise ValueError("aggregate needs must include every required correctness job")
        needs = json.loads(os.environ["CI_NEEDS"])
        if not isinstance(needs, dict) or set(needs) != REQUIRED:
            raise ValueError("required job inventory does not match")
        failed = [name for name, job in needs.items()
                  if not isinstance(job, dict) or job.get("result") != "success"]
        if failed:
            raise ValueError("required jobs did not succeed: " + ", ".join(sorted(failed)))
    except (KeyError, ValueError, TypeError, OSError) as error:
        print(f"::error::{error}", file=sys.stderr)
        return 1
    print("All required PR checks succeeded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
