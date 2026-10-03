#!/usr/bin/env python3
"""Prepare a reviewed, explicit plugin release entry; never publish or discover automatically."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import tempfile
import zipfile

ROOT='https://github.com/OmineDev/neomega-plugins/releases/download/'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--package',required=True,type=Path)
    parser.add_argument('--plugin',required=True)
    parser.add_argument('--version',required=True)
    parser.add_argument('--name',required=True)
    parser.add_argument('--index',required=True,type=Path)
    parser.add_argument('--permission-purposes',type=Path,help='Reviewed JSON map explaining requested capabilities; never grants authority')
    parser.add_argument('--source-ref', help='Full reviewed source commit SHA in this repository')
    parser.add_argument('--source-path', help='Repository-relative plugin source directory')
    parser.add_argument('--min-host-version', help='Minimum distributed Host version, separate from host_api')
    parser.add_argument('--description', help='Short description for discovery')
    args=parser.parse_args()
    try:
        if not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}',args.plugin): raise ValueError('invalid plugin ID')
        if not re.fullmatch(r'\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?',args.version): raise ValueError('explicit semantic version required')
        expected=f'{args.plugin}-{args.version}.zip'
        if args.package.name != expected: raise ValueError('package filename must be '+expected)
        if args.package.stat().st_size > 32 << 20: raise ValueError('package exceeds size limit')
        raw=args.package.read_bytes()
        with zipfile.ZipFile(args.package) as archive:
            info=archive.getinfo('manifest.json')
            if info.file_size > 1 << 20: raise ValueError('manifest exceeds size limit')
            manifest=json.loads(archive.read(info))
        if manifest['id'] != args.plugin or manifest['version'] != args.version: raise ValueError('manifest identity/version differs from reviewed release')
        purposes=json.loads(args.permission_purposes.read_text()) if args.permission_purposes else {}
        if not isinstance(purposes,dict) or not all(isinstance(k,str) and isinstance(v,str) for k,v in purposes.items()): raise ValueError('permission purposes must be a string map')
        entry={'plugin_id':args.plugin,'version':args.version,'name':args.name,
               'url':ROOT+f'{args.plugin}-v{args.version}/{expected}',
               'sha256':hashlib.sha256(raw).hexdigest(),
               'permissions':manifest.get('permissions',{}),'permission_purposes':purposes}
        if args.source_ref is not None or args.source_path is not None:
            if not isinstance(args.source_ref, str) or not re.fullmatch('[0-9a-f]{40}', args.source_ref):
                raise ValueError('source-ref must be a full reviewed commit SHA')
            if not args.source_path:
                raise ValueError('source-path is required with source-ref')
            path = PurePosixPath(args.source_path)
            if path.is_absolute() or '..' in path.parts or str(path) != args.source_path or '\\' in args.source_path:
                raise ValueError('source-path must be a safe repository-relative directory')
            entry['source_provenance'] = {'repository': 'https://github.com/OmineDev/neomega-plugins',
                                          'ref': args.source_ref, 'path': args.source_path}
            for key in ('runtime', 'host_api', 'worker_protocol', 'dependencies'):
                entry[key] = manifest[key]
            for key in ('target_os', 'target_arch'):
                if key in manifest:
                    entry[key] = manifest[key]
        if args.min_host_version is not None:
            if not re.fullmatch(r'\d+\.\d+\.\d+', args.min_host_version):
                raise ValueError('min-host-version must be an explicit release version')
            entry['min_host_version'] = args.min_host_version
        if args.description is not None:
            entry['description'] = args.description
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
