'''
python data_toolkit\\filter_metadata_glb.py --metadata_csv E:\\\\Qkyo\\\\trellis\\\\metadata_train_106_with_local_path_filtered.csv --glb_root E:\\Qkyo\\trellis_100\\pcd_standard_b_train_106_glb --recursive
'''

import argparse
from pathlib import Path

import pandas as pd


def enumerate_glb_paths(glb_root: Path, recursive: bool) -> dict[str, Path]:
    """小写文件名 -> 磁盘路径；重名时保留字典序更靠前的路径。"""
    pattern = "**/*.glb" if recursive else "*.glb"
    out: dict[str, Path] = {}
    for p in sorted(glb_root.glob(pattern), key=lambda x: str(x).lower()):
        if not p.is_file():
            continue
        key = p.name.lower()
        if key not in out:
            out[key] = p
    return out


def build_match_names(df: pd.DataFrame, path_col: str) -> pd.Series:
    if path_col in df.columns:
        names = df[path_col].fillna("").astype(str).map(lambda x: Path(x).name.lower())
        return names

    if "glb_name" in df.columns:
        return df["glb_name"].fillna("").astype(str).map(lambda x: f"{x}.glb".lower())

    raise ValueError(
        f"Column '{path_col}' not found, and fallback column 'glb_name' is also missing."
    )


def _to_relative_posix(path: Path, relative_to: Path) -> str:
    rel = path.resolve().relative_to(relative_to.resolve())
    return rel.as_posix()


def main():
    parser = argparse.ArgumentParser(
        description="按 glb_root 中实际存在的 .glb 过滤 metadata，更新 local_path，并可删除目录中未被 CSV 引用的多余 .glb。"
    )
    parser.add_argument("--metadata_csv", type=str, required=True, help="Path to metadata.csv")
    parser.add_argument("--glb_root", type=str, required=True, help="Folder containing .glb files")
    parser.add_argument(
        "--path_col",
        type=str,
        default="local_path",
        help="Column containing GLB path/name. If missing, fallback to glb_name + '.glb'.",
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Recursively scan subfolders under --glb_root",
    )
    parser.add_argument(
        "--relative_to",
        type=str,
        default=None,
        help="计算写入 path_col（默认 local_path）的相对路径时的基准目录。默认: glb_root 的上一级目录。",
    )
    parser.add_argument(
        "--no_delete_orphan_glbs",
        action="store_true",
        help="不删除 glb_root 下多余 .glb（默认会删除未被保留 CSV 行引用的文件）。",
    )
    parser.add_argument(
        "--output_csv",
        type=str,
        default=None,
        help="Output csv path. Default: <metadata_stem>_filtered.csv in same folder.",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="Overwrite input metadata.csv directly.",
    )
    args = parser.parse_args()

    metadata_csv = Path(args.metadata_csv)
    glb_root = Path(args.glb_root)

    if not metadata_csv.exists():
        raise FileNotFoundError(f"metadata csv not found: {metadata_csv}")
    if not glb_root.exists() or not glb_root.is_dir():
        raise NotADirectoryError(f"glb folder not found: {glb_root}")

    relative_to = Path(args.relative_to) if args.relative_to else glb_root.parent
    if not relative_to.exists():
        raise NotADirectoryError(f"--relative_to not found: {relative_to}")

    df = pd.read_csv(metadata_csv)
    glb_paths = enumerate_glb_paths(glb_root, args.recursive)
    glb_names = set(glb_paths.keys())
    match_names = build_match_names(df, args.path_col)

    filtered_df = df[match_names.isin(glb_names)].copy()
    # 同一 glb 多行时只保留一行，且总行数不超过 glb_root 中 .glb 个数
    cap = len(glb_paths)
    filtered_df["_match_key"] = match_names.loc[filtered_df.index].astype(str)
    filtered_df = filtered_df.drop_duplicates(subset=["_match_key"], keep="first")
    filtered_df = filtered_df.head(cap)
    match_keys = filtered_df["_match_key"].tolist()
    filtered_df = filtered_df.drop(columns=["_match_key"])

    new_paths: list[str] = []
    for key in match_keys:
        abs_path = glb_paths[key]
        new_paths.append(_to_relative_posix(abs_path, relative_to))

    filtered_df[args.path_col] = new_paths

    kept_keys = set(match_keys)
    deleted_files: list[Path] = []
    delete_orphans = not args.no_delete_orphan_glbs
    if delete_orphans:
        for key, abs_path in glb_paths.items():
            if key in kept_keys:
                continue
            try:
                abs_path.unlink()
                deleted_files.append(abs_path)
            except OSError as e:
                print(f"WARN: failed to delete {abs_path}: {e}")

    if args.inplace:
        output_csv = metadata_csv
    elif args.output_csv:
        output_csv = Path(args.output_csv)
    else:
        output_csv = metadata_csv.with_name(f"{metadata_csv.stem}_filtered.csv")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    filtered_df.to_csv(output_csv, index=False)

    print(f"Input rows        : {len(df)}")
    print(f".glb under root  : {cap}")
    print(f"Kept rows         : {len(filtered_df)}")
    print(f"relative_to       : {relative_to}")
    print(f"Updated column    : {args.path_col}")
    if deleted_files:
        print(f"Deleted orphan glb: {len(deleted_files)}")
    elif delete_orphans:
        print("Deleted orphan glb: 0")
    print(f"Output            : {output_csv}")


if __name__ == "__main__":
    main()
