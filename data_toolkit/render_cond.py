import os
import json
import sys
import importlib
import argparse
import hashlib
import pandas as pd
from easydict import EasyDict as edict
from functools import partial
from subprocess import DEVNULL, call
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import numpy as np
from utils import sphere_hammersley_sequence


BLENDER_LINK = 'https://download.blender.org/release/Blender3.0/blender-3.0.1-linux-x64.tar.xz'
BLENDER_INSTALLATION_PATH = '/tmp'
BLENDER_PATH = f'{BLENDER_INSTALLATION_PATH}/blender-3.0.1-linux-x64/blender'

def _object_seed(seed, sha256):
    if seed is None:
        return None
    digest = hashlib.sha256(f'{seed}:{sha256}'.encode('utf-8')).digest()
    return int.from_bytes(digest[:4], byteorder='little')


def _render_dir_name(render_mode):
    return 'renders_cond' if render_mode == 'lit' else f'renders_cond_{render_mode}'


def _resolve_transforms_json(sha256, transforms_json=None, transforms_root=None):
    if transforms_json is not None:
        path = os.path.expanduser(transforms_json)
        if os.path.isdir(path):
            return os.path.join(path, 'renders_cond', sha256, 'transforms.json')
        return path
    if transforms_root is not None:
        return os.path.join(os.path.expanduser(transforms_root), 'renders_cond', sha256, 'transforms.json')
    return None


def _install_blender():
    if not os.path.exists(BLENDER_PATH):
        os.system('sudo apt-get update')
        os.system('sudo apt-get install -y libxrender1 libxi6 libxkbcommon-x11-0 libsm6 libxfixes3 libgl1')
        os.system(f'wget {BLENDER_LINK} -P {BLENDER_INSTALLATION_PATH}')
        os.system(f'tar -xvf {BLENDER_INSTALLATION_PATH}/blender-3.0.1-linux-x64.tar.xz -C {BLENDER_INSTALLATION_PATH}')


def _render_cond(file_path, metadatum, root, output_dir_name, num_cond_views, seed=None, transforms_json=None, transforms_root=None, render_mode='lit'):
    sha256 = metadatum['sha256'] if isinstance(metadatum, dict) else metadatum
    object_seed = _object_seed(seed, sha256)
    input_transforms_json = _resolve_transforms_json(sha256, transforms_json, transforms_root)
    cond_views = None
    if input_transforms_json is None:
        rng = np.random.default_rng(object_seed)
        # Build conditional view camera
        yaws = []
        pitchs = []
        offset = (rng.random(), rng.random())
        for i in range(num_cond_views):
            y, p = sphere_hammersley_sequence(i, num_cond_views, offset)
            yaws.append(y)
            pitchs.append(p)
        fov_min, fov_max = 10, 70
        radius_min = np.sqrt(3) / 2 / np.sin(fov_max / 360 * np.pi)
        radius_max = np.sqrt(3) / 2 / np.sin(fov_min / 360 * np.pi)
        k_min = 1 / radius_max**2
        k_max = 1 / radius_min**2
        ks = rng.uniform(k_min, k_max, (num_cond_views,))
        radius = [1 / np.sqrt(k) for k in ks]
        fov = [2 * np.arcsin(np.sqrt(3) / 2 / r) for r in radius]
        cond_views = [{'yaw': y, 'pitch': p, 'radius': r, 'fov': f} for y, p, r, f in zip(yaws, pitchs, radius, fov)]
    elif not os.path.exists(input_transforms_json):
        print(f'Missing transforms.json for {sha256}: {input_transforms_json}', flush=True)
        return None

    args = [
        BLENDER_PATH, '-b', '-P', os.path.join(os.path.dirname(__file__), 'blender_script', 'render_cond.py'),
        '--',
        '--object', os.path.expanduser(file_path),
        '--cond_resolution', '1024',
        '--cond_output_folder', os.path.join(root, output_dir_name, sha256),
        '--engine', 'CYCLES',
        '--render_mode', render_mode,
    ]
    if cond_views is not None:
        args.extend(['--cond_views', json.dumps(cond_views)])
    if input_transforms_json is not None:
        args.extend(['--transforms_json', input_transforms_json])
    if object_seed is not None:
        args.extend(['--seed', str(object_seed)])
    if file_path.endswith('.blend'):
        args.insert(1, file_path)
    
    call(args, stdout=DEVNULL, stderr=DEVNULL)
    
    if os.path.exists(os.path.join(root, output_dir_name, sha256, 'transforms.json')):
        return {'sha256': sha256, 'cond_rendered': True}


if __name__ == '__main__':
    dataset_utils = importlib.import_module(f'datasets.{sys.argv[1]}')

    parser = argparse.ArgumentParser()
    parser.add_argument('--root', type=str, required=True,
                        help='Directory to save the metadata')
    parser.add_argument('--download_root', type=str, default=None,
                        help='Directory to save the downloaded files')
    parser.add_argument('--render_cond_root', type=str, default=None,
                        help='Directory to save the mesh dumps')
    parser.add_argument('--filter_low_aesthetic_score', type=float, default=None,
                        help='Filter objects with aesthetic score lower than this value')
    parser.add_argument('--instances', type=str, default=None,
                        help='Instances to process')
    parser.add_argument('--num_cond_views', type=int, default=16,
                        help='Number of conditional views to render')
    parser.add_argument('--seed', type=int, default=None,
                        help='Optional seed. Each object derives a deterministic seed from this value and its sha256.')
    parser.add_argument('--transforms_json', type=str, default=None,
                        help='Optional single transforms.json to reuse camera parameters. For batch rendering, prefer --transforms_root.')
    parser.add_argument('--transforms_root', type=str, default=None,
                        help='Optional root containing renders_cond/<sha256>/transforms.json files to reuse per-asset camera parameters.')
    parser.add_argument('--render_mode', type=str, default='lit', choices=['lit', 'uniform_lit', 'mra'],
                        help='Rendering mode: lit adds random lights per view; uniform_lit keeps lit materials with uniform env only; mra renders metallic/roughness/alpha channels.')
    parser.add_argument('--overwrite', action='store_true',
                        help='Render even if an output transforms.json already exists.')
    dataset_utils.add_args(parser)
    parser.add_argument('--rank', type=int, default=0)
    parser.add_argument('--world_size', type=int, default=1)
    parser.add_argument('--max_workers', type=int, default=8)
    opt = parser.parse_args(sys.argv[2:])
    opt = edict(vars(opt))
    opt.download_root = opt.download_root or opt.root
    opt.render_cond_root = opt.render_cond_root or opt.root
    output_dir_name = _render_dir_name(opt.render_mode)

    os.makedirs(os.path.join(opt.render_cond_root, output_dir_name, 'new_records'), exist_ok=True)
    
    # install blender
    print('Checking blender...', flush=True)
    _install_blender()

    # get file list
    if not os.path.exists(os.path.join(opt.root, 'metadata.csv')):
        raise ValueError('metadata.csv not found')
    metadata = pd.read_csv(os.path.join(opt.root, 'metadata.csv')).set_index('sha256')
    if os.path.exists(os.path.join(opt.root, 'aesthetic_scores', 'metadata.csv')):
        metadata = metadata.combine_first(pd.read_csv(os.path.join(opt.root, 'aesthetic_scores','metadata.csv')).set_index('sha256'))
    if os.path.exists(os.path.join(opt.download_root, 'raw', 'metadata.csv')):
        metadata = metadata.combine_first(pd.read_csv(os.path.join(opt.download_root, 'raw', 'metadata.csv')).set_index('sha256'))
    if os.path.exists(os.path.join(opt.render_cond_root, output_dir_name, 'metadata.csv')):
        metadata = metadata.combine_first(pd.read_csv(os.path.join(opt.render_cond_root, output_dir_name, 'metadata.csv')).set_index('sha256'))
    metadata = metadata.reset_index()
    if opt.instances is None:
        metadata = metadata[metadata['local_path'].notna()]
        if opt.filter_low_aesthetic_score is not None:
            metadata = metadata[metadata['aesthetic_score'] >= opt.filter_low_aesthetic_score]
        if opt.render_mode == 'lit' and 'cond_rendered' in metadata.columns:
            metadata = metadata[(metadata['cond_rendered'] != True)]
    else:
        if os.path.exists(opt.instances):
            with open(opt.instances, 'r') as f:
                instances = f.read().splitlines()
        else:
            instances = opt.instances.split(',')
        metadata = metadata[metadata['sha256'].isin(instances)]

    start = len(metadata) * opt.rank // opt.world_size
    end = len(metadata) * (opt.rank + 1) // opt.world_size
    metadata = metadata[start:end]
    records = []

    # filter out objects that are already processed
    if not opt.overwrite:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor, \
            tqdm(total=len(metadata), desc="Filtering existing objects") as pbar:
            def check_sha256(sha256):
                if os.path.exists(os.path.join(opt.render_cond_root, output_dir_name, sha256, 'transforms.json')):
                    records.append({'sha256': sha256, 'cond_rendered': True})
                pbar.update()
            executor.map(check_sha256, metadata['sha256'].values)
            executor.shutdown(wait=True)
        existing_sha256 = set(r['sha256'] for r in records)
        metadata = metadata[~metadata['sha256'].isin(existing_sha256)]

    print(f'Processing {len(metadata)} objects...')

    # process objects
    func = partial(
        _render_cond,
        root=opt.render_cond_root,
        output_dir_name=output_dir_name,
        num_cond_views=opt.num_cond_views,
        seed=opt.seed,
        transforms_json=opt.transforms_json,
        transforms_root=opt.transforms_root,
        render_mode=opt.render_mode,
    )
    cond_rendered = dataset_utils.foreach_instance(metadata, opt.render_cond_root, func, max_workers=opt.max_workers, desc='Rendering objects')
    cond_rendered = pd.concat([cond_rendered, pd.DataFrame.from_records(records)])
    cond_rendered.to_csv(os.path.join(opt.render_cond_root, output_dir_name, 'new_records', f'part_{opt.rank}.csv'), index=False)
