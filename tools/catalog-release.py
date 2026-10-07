#!/usr/bin/env python3
"""Prepare a reviewed, explicit plugin release entry; never publish or discover automatically."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
import zipfile

ROOT='https://github.com/OmineDev/neomega-plugins/releases/download/'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',type=Path)
    parser.add_argument('--merge-index',type=Path,help='Merge only the selected fixed candidate entry into the latest index')
    parser.add_argument('--source-ref',help='Reviewed full source commit SHA')
    parser.add_argument('--source-path',help='Reviewed repository-relative plugin directory')
    parser.add_argument('--plugin',required=True)
    parser.add_argument('--version',required=True)
    parser.add_argument('--name')
    parser.add_argument('--description', help='Short author description (maximum 2000 characters)')
    parser.add_argument('--index',required=True,type=Path)
    parser.add_argument('--permission-purposes',type=Path,help='Reviewed JSON map explaining requested capabilities; never grants authority')
    args=parser.parse_args()
    try:
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}',args.plugin): raise ValueError('invalid plugin ID')
        if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?',args.version): raise ValueError('explicit semantic version required')
        if args.merge_index:
            if args.package or args.source_ref or args.source_path:
                raise ValueError('merge uses the fixed candidate entry, not package/source arguments')
            candidate=json.loads(args.merge_index.read_text())
            matches=[e for e in candidate['plugins'] if e['plugin_id']==args.plugin and e['version']==args.version]
            if candidate.get('schema_version') != 1 or len(matches) != 1:
                raise ValueError('candidate must contain exactly one selected release')
            entry=matches[0]
        else:
            if not args.package or not args.name:
                raise ValueError('--package and --name are required for release preparation')
            expected=f'{args.plugin}-{args.version}.zip'
            if args.package.name != expected: raise ValueError('package filename must be '+expected)
            if args.package.stat().st_size > 32 << 20: raise ValueError('package exceeds size limit')
            raw=args.package.read_bytes()
            with zipfile.ZipFile(args.package) as archive:
                info=archive.getinfo('manifest.json')
                if info.file_size > 1 << 20: raise ValueError('manifest exceeds size limit')
                manifest=json.loads(archive.read(info))
                readme = archive.read('README.md') if 'README.md' in archive.namelist() else None
                if readme is not None and len(readme) > 256 * 1024:
                    raise ValueError('README exceeds 256 KiB')
                readme_text = readme.decode('utf-8') if readme is not None else ''
            if manifest['id'] != args.plugin or manifest['version'] != args.version: raise ValueError('manifest identity/version differs from reviewed release')
            purposes=json.loads(args.permission_purposes.read_text()) if args.permission_purposes else {}
            if not isinstance(purposes,dict) or not all(isinstance(k,str) and isinstance(v,str) for k,v in purposes.items()): raise ValueError('permission purposes must be a string map')
            entry={'plugin_id':args.plugin,'version':args.version,'name':args.name,
                   'url':ROOT+f'{args.plugin}-v{args.version}/{expected}',
                   'sha256':hashlib.sha256(raw).hexdigest(),
                   'permissions':manifest.get('permissions',{}),'permission_purposes':purposes}
            description = args.description
            if description is None:
                description = next((line.strip() for line in readme_text.splitlines() if line.strip() and not line.lstrip().startswith(('#', '```', '|', '![', '<'))), '')[:2000]
            if len(description) > 2000:
                raise ValueError('description exceeds 2000 characters')
            entry['description'] = description
            if readme is not None:
                entry['readme'] = {'path': 'README.md', 'sha256': hashlib.sha256(readme).hexdigest(), 'size': len(readme)}
            for field in ('subscriptions','runtime','host_api','worker_protocol','dependencies','target_os','target_arch','min_host_version'):
                if field in manifest:
                    entry[field]=manifest[field]
            if bool(args.source_ref) != bool(args.source_path):
                raise ValueError('source ref and path must be supplied together')
            if args.source_ref:
                if not re.fullmatch(r'[a-f0-9]{40}',args.source_ref):
                    raise ValueError('source ref must be a full commit SHA')
                if not re.fullmatch(r'plugins/[A-Za-z0-9][A-Za-z0-9_-]*',args.source_path):
                    raise ValueError('source path must identify one plugin directory')
                entry['source_provenance']={'repository':'https://github.com/OmineDev/neomega-plugins','ref':args.source_ref,'path':args.source_path}
        index=json.loads(args.index.read_text()) if args.index.exists() else {'schema_version':1,'plugins':[]}
        if not isinstance(index,dict) or index.get('schema_version') != 1 or not isinstance(index.get('plugins'),list): raise ValueError('invalid existing index')
        for old in index['plugins']:
            if old['plugin_id']==args.plugin and old['version']==args.version:
                if old != entry: raise ValueError('published versions are immutable; increment the version')
                print('Existing identical release entry retained')
                return
        index['plugins'].append(entry)
        index['plugins'].sort(key=lambda e:(e['plugin_id'],e['version']))
        args.index.parent.mkdir(parents=True,exist_ok=True)
        with tempfile.NamedTemporaryFile(mode='w',dir=args.index.parent,delete=False,encoding='utf-8') as output:
            temporary=Path(output.name)
            try:
                json.dump(index,output,ensure_ascii=False,indent=2)
                output.write('\n');output.flush();os.fsync(output.fileno())
                output.close();os.replace(temporary,args.index)
            finally:
                temporary.unlink(missing_ok=True)
        print(f'Prepared {args.plugin} {args.version} {entry["sha256"]}; review before publishing')
    except (OSError,KeyError,TypeError,ValueError,zipfile.BadZipFile) as error:
        parser.exit(1,f'catalog-release: {error}\n')

if __name__=='__main__': main()
