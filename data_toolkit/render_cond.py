'''
python data_toolkit\render_cond.py --root E:\\Qkyo\\trellis_100 --download_root E:\\Qkyo\\trellis_100\\pcd_standard_b_train_106_glb --csv_name metadata.csv --num_views 6
'''

import os
import json
import copy
import sys
import importlib
import importlib.util
import argparse
import shutil
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
BLENDER_PATH = 'C:/Program Files/Blender Foundation/Blender 4.4/blender.exe'


def _resolve_blender_path():
    env_blender_path = os.environ.get('BLENDER_PATH')
    candidates = []
    if env_blender_path:
        candidates.append(env_blender_path)
    candidates.extend([
        BLENDER_PATH,
        'C:/Program Files/Blender Foundation/Blender 4.4/blender.exe',
        'C:/Program Files/Blender Foundation/Blender 4.3/blender.exe',
        'C:/Program Files/Blender Foundation/Blender 4.2/blender.exe',
        'C:/Program Files/Blender Foundation/Blender 4.1/blender.exe',
        'C:/Program Files/Blender Foundation/Blender 4.0/blender.exe',
        'C:/Program Files/Blender Foundation/Blender 3.6/blender.exe',
        'C:/Program Files/Blender Foundation/Blender/blender.exe',
    ])
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    return shutil.which('blender')


def _install_blender():
    global BLENDER_PATH
    resolved = _resolve_blender_path()
    if resolved:
        BLENDER_PATH = resolved
        return
    if os.name == 'nt':
        raise RuntimeError('Blender not found. Please install Blender and set environment variable BLENDER_PATH, or ensure blender is in PATH.')
    os.system('sudo apt-get update')
    os.system('sudo apt-get install -y libxrender1 libxi6 libxkbcommon-x11-0 libsm6 libxfixes3 libgl1')
    os.system(f'wget {BLENDER_LINK} -P {BLENDER_INSTALLATION_PATH}')
    os.system(f'tar -xvf {BLENDER_INSTALLATION_PATH}/blender-3.0.1-linux-x64.tar.xz -C {BLENDER_INSTALLATION_PATH}')


def _load_dataset_utils(subset):
    """
    Load dataset helper module by subset name.

    Priority:
    1) data_toolkit/datasets/<subset>.py (local file, avoids conflicts with pip package `datasets`)
    2) datasets.<subset> (legacy behavior)
    """
    local_module_path = os.path.join(os.path.dirname(__file__), 'datasets', f'{subset}.py')
    if os.path.exists(local_module_path):
        module_name = f'_local_data_toolkit_datasets_{subset}'
        spec = importlib.util.spec_from_file_location(module_name, local_module_path)
        if spec is None or spec.loader is None:
            raise ImportError(f'Cannot load dataset module from {local_module_path}')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return importlib.import_module(f'datasets.{subset}')


def _is_valid_subset_name(subset):
    if subset is None:
        return False
    subset = str(subset).strip()
    if subset == '':
        return False
    # Path-like values should not be treated as dataset subset names.
    if any(ch in subset for ch in ['\\', '/', ':']):
        return False
    return True


def _try_load_dataset_utils(subset):
    if not _is_valid_subset_name(subset):
        return None
    try:
        return _load_dataset_utils(subset)
    except ModuleNotFoundError:
        return None


def _render_cond(file_path, metadatum, root, num_cond_views):
    sha256 = metadatum['sha256']
    # Build conditional view camera
    yaws = []
    pitchs = []
    offset = (np.random.rand(), np.random.rand())
    for i in range(num_cond_views):
        y, p = sphere_hammersley_sequence(i, num_cond_views, offset)
        yaws.append(y)
        pitchs.append(p)
    fov_min, fov_max = 10, 70
    radius_min = np.sqrt(3) / 2 / np.sin(fov_max / 360 * np.pi)
    radius_max = np.sqrt(3) / 2 / np.sin(fov_min / 360 * np.pi)
    k_min = 1 / radius_max**2
    k_max = 1 / radius_min**2
    ks = np.random.uniform(k_min, k_max, (1000000,))
    radius = [1 / np.sqrt(k) for k in ks]
    fov = [2 * np.arcsin(np.sqrt(3) / 2 / r) for r in radius]
    cond_views = [{'yaw': y, 'pitch': p, 'radius': r, 'fov': f} for y, p, r, f in zip(yaws, pitchs, radius, fov)]
    
    args = [
        BLENDER_PATH, '-b', '-P', os.path.join(os.path.dirname(__file__), 'blender_script', 'render_cond.py'),
        '--',
        '--object', os.path.expanduser(file_path),
        '--cond_views', json.dumps(cond_views),
        '--cond_resolution', '1024',
        '--cond_output_folder', os.path.join(root, 'renders_cond', sha256),
        '--engine', 'CYCLES',
    ]
    if file_path.endswith('.blend'):
        args.insert(1, file_path)
    
    call(args, stdout=DEVNULL, stderr=DEVNULL)
    
    if os.path.exists(os.path.join(root, 'renders_cond', sha256, 'transforms.json')):
        return {'sha256': sha256, 'cond_rendered': True}


def _resolve_local_path(local_path, download_root):
    if pd.isna(local_path):
        return None
    local_path = str(local_path).strip()
    if local_path == '':
        return None
    if os.path.isabs(local_path) and os.path.exists(local_path):
        return local_path
    if os.path.exists(local_path):
        return local_path
    joined = os.path.join(download_root, local_path)
    if os.path.exists(joined):
        return joined
    return local_path


def _resolve_file_path_from_metadata(metadatum, opt):
    candidate_keys = [
        'local_path',
        'path',
        'file_path',
        'glb_path',
        'mesh_path',
        'filename',
    ]
    for key in candidate_keys:
        file_path = _resolve_local_path(metadatum.get(key), opt.download_root)
        if file_path is not None and os.path.exists(file_path):
            return file_path

    glb_name = metadatum.get('glb_name')
    if pd.notna(glb_name):
        glb_name = str(glb_name).strip()
        if glb_name != '':
            if not glb_name.lower().endswith('.glb'):
                glb_name = f'{glb_name}.glb'
            for base_dir in [opt.glb_root, opt.download_root]:
                if not base_dir:
                    continue
                candidate = os.path.join(base_dir, glb_name)
                if os.path.exists(candidate):
                    return candidate

    sha256 = metadatum.get('sha256')
    if pd.notna(sha256):
        sha256 = str(sha256).strip()
        if sha256 != '':
            for base_dir in [opt.glb_root, opt.download_root]:
                if not base_dir:
                    continue
                candidate = os.path.join(base_dir, f'{sha256}.glb')
                if os.path.exists(candidate):
                    return candidate
    return None


def _foreach_instance_local_path(metadata, opt):
    meta_records = metadata.to_dict('records')

    def worker(metadatum):
        file_path = _resolve_file_path_from_metadata(metadatum, opt)
        if file_path is None:
            return None
        return _render_cond(file_path, metadatum, root=opt.render_cond_root, num_cond_views=opt.num_cond_views)

    outputs = []
    with ThreadPoolExecutor(max_workers=opt.max_workers) as executor:
        for out in tqdm(executor.map(worker, meta_records), total=len(meta_records), desc='Rendering objects'):
            if out is not None:
                outputs.append(out)
    return pd.DataFrame.from_records(outputs)


if __name__ == '__main__':
    pre_parser = argparse.ArgumentParser(add_help=False)
    pre_parser.add_argument('subset', nargs='?', default=None)
    pre_args, _ = pre_parser.parse_known_args(sys.argv[1:])

    dataset_utils = None
    if pre_args.subset is not None and not str(pre_args.subset).startswith('-'):
        dataset_utils = _try_load_dataset_utils(pre_args.subset)

    parser = argparse.ArgumentParser()
    parser.add_argument('subset', nargs='?', default=None,
                        help='Dataset subset. Optional if metadata already has local_path.')
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
    parser.add_argument('--num_cond_views', '--num_views', dest='num_cond_views', type=int, default=16,
                        help='Number of conditional views to render')
    parser.add_argument('--csv_name', type=str, default='metadata.csv',
                        help='Name of the CSV file containing metadata')
    parser.add_argument('--glb_root', type=str, default=None,
                        help='Directory containing .glb files. Used when local_path is missing in metadata.')
    if dataset_utils is not None:
        dataset_utils.add_args(parser)
    parser.add_argument('--rank', type=int, default=0)
    parser.add_argument('--world_size', type=int, default=1)
    parser.add_argument('--max_workers', type=int, default=8)
    opt = parser.parse_args(sys.argv[1:])
    opt = edict(vars(opt))
    opt.download_root = opt.download_root or opt.root
    opt.render_cond_root = opt.render_cond_root or opt.root

    os.makedirs(os.path.join(opt.render_cond_root, 'renders_cond', 'new_records'), exist_ok=True)
    
    # install blender
    print('Checking blender...', flush=True)
    _install_blender()

    # get file list
    csv_path = opt.csv_name if os.path.isabs(opt.csv_name) else os.path.join(opt.root, opt.csv_name)
    if not os.path.exists(csv_path):
        raise ValueError('metadata.csv not found')
    metadata = pd.read_csv(csv_path).set_index('sha256')
    if os.path.exists(os.path.join(opt.root, 'aesthetic_scores', 'metadata.csv')):
        metadata = metadata.combine_first(pd.read_csv(os.path.join(opt.root, 'aesthetic_scores','metadata.csv')).set_index('sha256'))
    if os.path.exists(os.path.join(opt.download_root, 'raw', 'metadata.csv')):
        metadata = metadata.combine_first(pd.read_csv(os.path.join(opt.download_root, 'raw', 'metadata.csv')).set_index('sha256'))
    if os.path.exists(os.path.join(opt.render_cond_root, 'renders_cond', 'metadata.csv')):
        metadata = metadata.combine_first(pd.read_csv(os.path.join(opt.render_cond_root, 'renders_cond', 'metadata.csv')).set_index('sha256'))
    metadata = metadata.reset_index()
    opt.glb_root = opt.glb_root or opt.download_root
    if opt.instances is None:
        if opt.filter_low_aesthetic_score is not None:
            metadata = metadata[metadata['aesthetic_score'] >= opt.filter_low_aesthetic_score]
        if 'cond_rendered' in metadata.columns:
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
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor, \
        tqdm(total=len(metadata), desc="Filtering existing objects") as pbar:
        def check_sha256(sha256):
            if os.path.exists(os.path.join(opt.render_cond_root, 'renders_cond', sha256, 'transforms.json')):
                records.append({'sha256': sha256, 'cond_rendered': True})
            pbar.update()
        executor.map(check_sha256, metadata['sha256'].values)
        executor.shutdown(wait=True)
    existing_sha256 = set(r['sha256'] for r in records)
    metadata = metadata[~metadata['sha256'].isin(existing_sha256)]

    print(f'Processing {len(metadata)} objects...')

    # process objects
    if dataset_utils is not None:
        func = partial(_render_cond, root=opt.render_cond_root, num_cond_views=opt.num_cond_views)
        cond_rendered = dataset_utils.foreach_instance(metadata, opt.render_cond_root, func, max_workers=opt.max_workers, desc='Rendering objects')
    else:
        cond_rendered = _foreach_instance_local_path(metadata, opt)

    existing_df = pd.DataFrame.from_records(records)
    if len(cond_rendered) == 0:
        cond_rendered = existing_df
    elif len(existing_df) != 0:
        cond_rendered = pd.concat([cond_rendered, existing_df], ignore_index=True)
    cond_rendered.to_csv(os.path.join(opt.render_cond_root, 'renders_cond', 'new_records', f'part_{opt.rank}.csv'), index=False)
