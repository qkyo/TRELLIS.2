import os
# os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"  # Can save GPU memory
os.environ["PYTORCH_ALLOC_CONF"] = "max_split_size_mb:2048"  # Can save GPU memory
import trimesh
from PIL import Image
from trellis2.pipelines import Trellis2TexturingPipeline
from pathlib import Path

# Configuration
# GLB_FOLDER = r"D:\texture-vae\stylized\glbs_without_color"
# IMAGE_FOLDER = r"D:\texture-vae\stylized\2.5d"
# OUTPUT_FOLDER = r"D:\outputs\trellis2\texture_stylized_mesh"

GLB_FOLDER = r"./input"
IMAGE_FOLDER = r"./input"
OUTPUT_FOLDER = r"./output"

# Optional runtime overrides (recommended in Linux):
#   GLB_FOLDER=/path/to/glbs IMAGE_FOLDER=/path/to/images OUTPUT_FOLDER=/path/to/out python batch_texturing.py
GLB_FOLDER = os.environ.get("GLB_FOLDER", GLB_FOLDER)
IMAGE_FOLDER = os.environ.get("IMAGE_FOLDER", IMAGE_FOLDER)
OUTPUT_FOLDER = os.environ.get("OUTPUT_FOLDER", OUTPUT_FOLDER)

def collect_glb_files(folder: Path):
    patterns = ("*.glb", "*.GLB")
    files = []
    for pat in patterns:
        files.extend(folder.glob(pat))
        files.extend(folder.rglob(pat))
    # De-duplicate while keeping order
    return list(dict.fromkeys(files))


glb_folder = Path(GLB_FOLDER)
image_folder = Path(IMAGE_FOLDER)
output_folder = Path(OUTPUT_FOLDER)

# Create output folder if it doesn't exist
os.makedirs(output_folder, exist_ok=True)

# 1. Load Pipeline
print("Loading pipeline...")
pipeline = Trellis2TexturingPipeline.from_pretrained("microsoft/TRELLIS.2-4B", config_file="texturing_pipeline.json")
pipeline.cuda()
print("Pipeline loaded successfully!")

# 2. Get all GLB files
print(f"GLB folder   : {glb_folder} (exists={glb_folder.exists()})")
print(f"Image folder : {image_folder} (exists={image_folder.exists()})")
print(f"Output folder: {output_folder}")

glb_files = collect_glb_files(glb_folder) if glb_folder.exists() else []
print(f"Found {len(glb_files)} GLB files to process")

if not glb_files:
    print("No GLB files found. Quick checks:")
    print("  1) Verify GLB_FOLDER points to the correct directory")
    print("  2) Script now searches recursively and supports .glb/.GLB")
    print("  3) If running on Linux, prefer Linux absolute paths via environment variables")

# 3. Process each GLB file
for idx, glb_path in enumerate(glb_files, 1):
    try:
        # Get base name without extension
        base_name = glb_path.stem
        
        # Find corresponding PNG image
        image_path = image_folder / f"{base_name}.png"
        
        if not image_path.exists():
            print(f"[{idx}/{len(glb_files)}] Warning: Image not found for {base_name}, skipping...")
            continue
        
        print(f"[{idx}/{len(glb_files)}] Processing {base_name}...")
        
        # Load mesh and image
        image = Image.open(str(image_path))

        mesh = trimesh.load(str(glb_path), force='scene')

        if isinstance(mesh, trimesh.Scene):
            if len(mesh.geometry) == 0:
                raise ValueError("Scene contains no geometry")

            # Merge all meshes in the scene into one mesh.
            mesh = trimesh.util.concatenate(
                tuple(mesh.geometry.values())
            )
        
        # Run pipeline
        output = pipeline.run(mesh, image)
        
        # Export result
        output_path = output_folder / f"{base_name}_textured.glb"
        output.export(str(output_path), extension_webp=True)
        
        print(f"[{idx}/{len(glb_files)}] ✓ Saved to {output_path}")
        
    except Exception as e:
        print(f"[{idx}/{len(glb_files)}] ✗ Error processing {base_name}: {str(e)}")
        continue

print(f"\nBatch processing complete! Processed {len(glb_files)} files.")
print(f"Output saved to: {output_folder}")
