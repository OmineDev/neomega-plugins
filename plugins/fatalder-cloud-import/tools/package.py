"""Build an installable plugin ZIP from an explicit source allowlist."""
import argparse
from pathlib import Path
import zipfile


def build(destination):
    root = Path(__file__).resolve().parents[1]
    names = ['manifest.json', 'main.py', 'config.schema.json', 'README.md', 'LICENSE']
    names += [str(p.relative_to(root)) for p in sorted((root / 'fatalder_plugin').glob('*.py'))]
    names += ['fatalder_plugin/build_defaults.json']
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name in names:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            archive.writestr(info, (root / name).read_bytes())
    return destination


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination')
    print(build(parser.parse_args().destination))
