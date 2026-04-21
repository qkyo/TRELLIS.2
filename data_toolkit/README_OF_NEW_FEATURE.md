# Metadata 工具脚本用法说明

## merge_local_path_to_metadata.py

**功能**：
将本地下载的 `.glb` 文件路径（local_path）批量合并/写入到已有的 metadata CSV 文件中。适用于下载后补全或修正 metadata 的 `local_path` 字段。

**常用命令行参数**：
- `--metadata_csv`：原始 metadata CSV 路径（必填）
- `--local_path_csv`：包含 sha256 和 local_path 的 CSV 路径（必填）
- `--output_csv`：输出合并后的 CSV 路径（可选，默认自动命名）

**典型用法**：
```sh
python data_toolkit/merge_local_path_to_metadata.py --metadata_csv <原始metadata.csv> --local_path_csv <local_path.csv> --output_csv <输出.csv>
```

---

## filter_metadata_glb.py

**功能**：
根据实际存在于 `glb_root` 目录下的 `.glb` 文件，过滤 metadata CSV，仅保留有对应文件的行，并可自动更新 `local_path`/`relative_path` 字段，支持递归查找和自动删除未被引用的多余 `.glb` 文件。

**常用命令行参数**：
- `--metadata_csv`：原始 metadata CSV 路径（必填）
- `--glb_root`：存放 .glb 文件的目录（必填）
- `--recursive`：递归查找子目录下的 .glb 文件（可选）
- `--path_col`：metadata 中存放路径/文件名的列名（默认 `local_path`）
- `--relative_to`：生成相对路径的基准目录（默认 `glb_root` 的上级）
- `--delete_orphan_glbs/--no-delete_orphan_glbs`：是否删除未被引用的多余 .glb（默认开启）
- `--output_csv`：输出 CSV 路径（可选）
- `--inplace`：直接覆盖原 metadata.csv（可选）

**典型用法**：
```sh
python data_toolkit/filter_metadata_glb.py --metadata_csv <metadata.csv> --glb_root <glb目录> --recursive
```

---

**应用场景**：
- `merge_local_path_to_metadata.py`：用于补全/修正 metadata 的本地路径字段。
- `filter_metadata_glb.py`：用于清理 metadata 和 glb 文件，确保一一对应，去除多余或缺失项。

如需详细参数说明，可运行 `python <脚本名> --help` 查看。
