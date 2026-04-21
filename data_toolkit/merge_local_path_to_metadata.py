"""
把 downloaded_0.csv 里的 local_path 按 sha256 回填到 metadata.csv。

示例：
python data_toolkit/merge_local_path_to_metadata.py \
  --metadata_csv E:/Qkyo/trellis/metadata_train_106.csv \
  --downloaded_csv E:/Qkyo/trellis/downloaded_0.csv \
  --output_csv E:/Qkyo/trellis/metadata_train_106_with_local_path.csv \
  --extract_glb_name

python merge_local_path_to_metadata.py --metadata_csv E:/Qkyo/trellis/metadata_train_106.csv --downloaded_csv E:/Qkyo/trellis/downloaded_0.csv --output_csv E:/Qkyo/trellis/metadata_train_106_with_local_path.csv --extract_glb_name

如果希望直接覆盖 metadata.csv：
python data_toolkit/merge_local_path_to_metadata.py \
  --metadata_csv E:/Qkyo/trellis/metadata.csv \
  --downloaded_csv E:/Qkyo/trellis/downloaded_0.csv \
  --inplace
"""

import argparse
from pathlib import Path

import pandas as pd


def _get_glb_name(path_value: str) -> str:
    if not isinstance(path_value, str) or path_value.strip() == "":
        return ""
    return Path(path_value).name


def main() -> None:
    parser = argparse.ArgumentParser(
        description="按 sha256 把 downloaded CSV 的 local_path 回填到 metadata.csv"
    )
    parser.add_argument("--metadata_csv", type=str, required=True, help="目标 metadata.csv")
    parser.add_argument("--downloaded_csv", type=str, required=True, help="来源 downloaded_0.csv")
    parser.add_argument(
        "--output_csv",
        type=str,
        default=None,
        help="输出路径。默认生成 <metadata_stem>_updated.csv",
    )
    parser.add_argument(
        "--inplace",
        action="store_true",
        help="是否直接覆盖 metadata.csv",
    )
    parser.add_argument(
        "--extract_glb_name",
        action="store_true",
        help="是否额外写入 glb_name 列（从 local_path 提取文件名）",
    )
    parser.add_argument(
        "--glb_name_col",
        type=str,
        default="glb_name",
        help="提取 glb 名称时写入的列名，默认 glb_name",
    )
    parser.add_argument(
        "--only_fillna",
        action="store_true",
        help="仅填充 metadata 中 local_path 为空的记录（默认会覆盖匹配到的 local_path）",
    )

    args = parser.parse_args()

    metadata_csv = Path(args.metadata_csv)
    downloaded_csv = Path(args.downloaded_csv)

    if not metadata_csv.exists():
        raise FileNotFoundError(f"metadata csv 不存在: {metadata_csv}")
    if not downloaded_csv.exists():
        raise FileNotFoundError(f"downloaded csv 不存在: {downloaded_csv}")

    metadata_df = pd.read_csv(metadata_csv)
    downloaded_df = pd.read_csv(downloaded_csv)

    required_cols = {"sha256", "local_path"}
    if not required_cols.issubset(set(downloaded_df.columns)):
        missing = required_cols - set(downloaded_df.columns)
        raise ValueError(f"downloaded csv 缺少列: {sorted(missing)}")
    if "sha256" not in metadata_df.columns:
        raise ValueError("metadata csv 缺少列: ['sha256']")

    # 只保留有效 local_path，按 sha256 去重（保留最后一条）
    source = downloaded_df[["sha256", "local_path"]].copy()
    source["local_path"] = source["local_path"].astype(str)
    source = source[source["sha256"].notna()]
    source = source[source["local_path"].str.strip() != ""]
    source = source.drop_duplicates(subset=["sha256"], keep="last")

    # 仅基于 metadata 原有行做更新：
    # - metadata 有、download 没有：保留 metadata 原值
    # - download 有、metadata 没有：不会新增到结果里
    result_df = metadata_df.copy()
    source_map = source.set_index("sha256")["local_path"]
    mapped_local_path = result_df["sha256"].map(source_map)

    if "local_path" not in result_df.columns:
        result_df["local_path"] = pd.NA

    if args.only_fillna:
        mask = result_df["local_path"].isna() | (result_df["local_path"].astype(str).str.strip() == "")
        write_mask = mask & mapped_local_path.notna()
        result_df.loc[write_mask, "local_path"] = mapped_local_path[write_mask]
    else:
        write_mask = mapped_local_path.notna()
        result_df.loc[write_mask, "local_path"] = mapped_local_path[write_mask]

    updated_count = int(write_mask.sum())

    if args.extract_glb_name:
        result_df[args.glb_name_col] = result_df["local_path"].map(_get_glb_name)

    if args.inplace:
        output_csv = metadata_csv
    elif args.output_csv:
        output_csv = Path(args.output_csv)
    else:
        output_csv = metadata_csv.with_name(f"{metadata_csv.stem}_updated.csv")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_csv(output_csv, index=False)

    print(f"metadata 总行数: {len(metadata_df)}")
    print(f"downloaded 可用记录数: {len(source)}")
    print(f"实际写入 local_path 的行数: {updated_count}")
    print(f"结果总行数(应与 metadata 一致): {len(result_df)}")
    if args.extract_glb_name:
        print(f"已写入列: {args.glb_name_col}")
    print(f"输出文件: {output_csv}")


if __name__ == "__main__":
    main()
