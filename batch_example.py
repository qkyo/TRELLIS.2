'''
这个脚本被用来渲染所有examole里面的例子
'''


print("STEP 0: start")

import torch
print("STEP 1: torch imported")

print("CUDA available:", torch.cuda.is_available())

print("STEP 2: before any TRELLIS import")

# ⚠️ 注意：TRELLIS 相关 import 全放下面
from trellis2.renderers import EnvMap   # 或你实际的 import 路径

print("STEP 3: after EnvMap import")

t = torch.tensor([1.0], device="cuda")
print("STEP 4: cuda tensor created", t)

import os
os.environ['OPENCV_IO_ENABLE_OPENEXR'] = '1'
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"  # Can save GPU memory
import cv2
import imageio
from PIL import Image
import torch
from trellis2.pipelines import Trellis2ImageTo3DPipeline
from trellis2.utils import render_utils
from trellis2.renderers import EnvMap
import o_voxel
import os

os.environ["FLASH_ATTENTION_DISABLE"] = "1"
os.environ["CUDA_LAUNCH_BLOCKING"] = "1"
os.environ["TORCH_SHOW_CPP_STACKTRACES"] = "1"
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# 1. Setup Environment Map
t=torch.tensor(
    cv2.cvtColor(cv2.imread('assets/hdri/forest.exr', cv2.IMREAD_UNCHANGED), cv2.COLOR_BGR2RGB),
    dtype=torch.float32, device=device
)
print("tensor device:", t.device, t.shape, t.dtype)
envmap = EnvMap(t)

# 2. Load Pipeline
pipeline = Trellis2ImageTo3DPipeline.from_pretrained("microsoft/TRELLIS.2-4B")
pipeline.cuda()

# 3. Get all images from directory
from pathlib import Path
image_dir = Path("assets/example_image_02")
image_files = sorted([f for f in image_dir.glob("*") if f.suffix.lower() in ['.png', '.jpg', '.jpeg', '.webp']])
print(f"Found {len(image_files)} images to process")

# Create output directory
output_dir = Path("outputs_02")
output_dir.mkdir(exist_ok=True)

# Process each image
for idx, image_path in enumerate(image_files):
    print(f"\n{'='*60}")
    print(f"Processing [{idx+1}/{len(image_files)}]: {image_path.name}")
    print(f"{'='*60}")
    
    # Load Image & Run
    image = Image.open(image_path)
    mesh = pipeline.run(image)[0]
    mesh.simplify(16777216) # nvdiffrast limit
    
    # Generate output filename
    output_name = image_path.stem
    
    # 4. Render Video
    print(f"Rendering video for {output_name}...")
    video = render_utils.make_pbr_vis_frames(render_utils.render_video(mesh, envmap=envmap))
    video_path = output_dir / f"{output_name}.mp4"
    imageio.mimsave(str(video_path), video, fps=15)
    print(f"Saved video: {video_path}")
    
    # 5. Export to GLB
    print(f"Exporting GLB for {output_name}...")
    glb = o_voxel.postprocess.to_glb(
        vertices            =   mesh.vertices,
        faces               =   mesh.faces,
        attr_volume         =   mesh.attrs,
        coords              =   mesh.coords,
        attr_layout         =   mesh.layout,
        voxel_size          =   mesh.voxel_size,
        aabb                =   [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
        decimation_target   =   1000000,
        texture_size        =   4096,
        remesh              =   True,
        remesh_band         =   1,
        remesh_project      =   0,
        verbose             =   False
    )
    glb_path = output_dir / f"{output_name}.glb"
    glb.export(str(glb_path), extension_webp=False)
    print(f"Saved GLB: {glb_path}")

print(f"\n{'='*60}")
print(f"All done! Processed {len(image_files)} images")
print(f"Outputs saved to: {output_dir.absolute()}")
print(f"{'='*60}")