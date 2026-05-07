import os
import argparse
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import pandas as pd
from utils import get_file_hash

def add_args(parser: argparse.ArgumentParser):
    pass

def foreach_instance(metadata, output_dir, func, max_workers=None, desc='Processing objects', no_file=False) -> pd.DataFrame:
    import os
    from concurrent.futures import ThreadPoolExecutor
    from tqdm import tqdm
    import tempfile
    import zipfile
    
    # load metadata
    metadata = metadata.to_dict('records')

    # processing objects
    records = []
    max_workers = max_workers or os.cpu_count()
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor, \
            tqdm(total=len(metadata), desc=desc) as pbar:
            def worker(metadatum):
                sha256 = metadatum['sha256']
                try:
                    if no_file:
                        file = None
                    else:
                        local_path = metadatum['local_path']
                        file = os.path.join(output_dir, local_path)
                    record = func(file, metadatum)
                    if record is not None:
                        records.append(record)
                    pbar.update()
                except Exception as e:
                    print(f"Error processing object {sha256}: {e}")
                    pbar.update()
            
            executor.map(worker, metadata)
            executor.shutdown(wait=True)
    except:
        print("Error happened during processing.")
        
    return pd.DataFrame.from_records(records)

def arrange_metadata(metadata, output_dir):
    metadata = metadata.to_dict('records')

    records = []

    for metadatum in metadata:
        if 'sha256' not in metadatum:
            assert False, "sha256 is required"
        sha256 = metadatum['sha256']

        # 增加新列
        if 'local_path' not in metadatum or pd.isna(metadatum['local_path']):
            metadatum['local_path'] = os.path.join(
                "glbs",
                f"{sha256}.glb"
            )

        records.append(metadatum)

    # 写回 dataframe
    new_metadata = pd.DataFrame(records)

    # 输出
    output_path = os.path.join(output_dir, "metadata.csv")
    new_metadata.to_csv(output_path, index=False)

    return new_metadata