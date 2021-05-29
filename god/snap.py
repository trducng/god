import shutil
import hashlib
import os
from pathlib import Path
from collections import defaultdict

from god.base import settings, Settings


def get_hashes(name, active=False):
    """Get the hashes of a file_path with name"""
    files = list(Path(settings.DIR_SNAP).glob('*'))
    names_hashes = defaultdict(list)
    for fn in files:
        components = fn.name.split('_')
        name_ = '_'.join(components[:-1])
        hash_ = components[-1] if active else components[-1].replace('-', '')
        name_hashes[name_].append(hash_)

    return names_hashes.get(name, [])


def add(file_path, name, force=False):
    """Add snapshot to god repo"""

    # calculate sha256
    with open(file_path, 'rb') as f_in:
        file_hash = hashlib.sha256(f_in.read()).hexdigest()

    # get hashes of the target snapshot name
    all_hashes = get_hashes(name, active=True)
    hashes = [each.replace('-', '') for each in all_hashes]
    active_hash = None
    for each in all_hashes:
        if '-' in each:
            active_hash = each.replace('-', '')

    # if there already exists
    if hashes:
        if file_hash in hashes:
            if active_hash == file_hash:
                print('Snapshot already active')
            else:
                print(f'Snapshot already active with hash {active_hash}')
            return

        if not force:
            print(f'Snapshot with name "{name}" already exists. Please pick different name')
            return

    # perform copy
    shutil.copy(
            file_path,
            Path(settings.DIR_SNAP, f'{name}_{file_hash}-'), follow_symlinks=True)

    # reprioritize old one
    if active_hash:
        shutil.copy(
            Path(settings.DIR_SNAP, f'{name}_{active_hash}-'),
            Path(settings.DIR_SNAP, f'{name}_{active_hash}'))

    return file_hash

def list_snap():
    files = list(Path(settings.DIR_SNAP).glob('*'))
    names = []
    for fn in files:
        components = fn.name.split('_')
        name_ = '_'.join(components[:-1])
        names.append(name_)

    return sorted(list(set(names)))
