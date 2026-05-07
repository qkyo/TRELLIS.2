import argparse
from pathlib import Path

import numpy as np
import torch
import trimesh
import o_voxel

import trellis2.models as models
import trellis2.modules.sparse as sp
from trellis2.utils import render_utils

try:
    import imageio
except ImportError:
    imageio = None
import cv2


def load_mesh(glb_path: Path) -> trimesh.Trimesh:
    mesh = trimesh.load(str(glb_path), force="scene")
    if isinstance(mesh, trimesh.Scene):
        if len(mesh.geometry) == 0:
            raise ValueError("Input scene contains no geometry.")
        mesh = trimesh.util.concatenate(tuple(mesh.geometry.values()))
    if not isinstance(mesh, trimesh.Trimesh):
        raise ValueError(f"Unsupported mesh type: {type(mesh)}")
    return mesh


def normalize_mesh(mesh: trimesh.Trimesh) -> trimesh.Trimesh:
    vertices = mesh.vertices
    v_min = vertices.min(axis=0)
    v_max = vertices.max(axis=0)
    center = (v_min + v_max) / 2
    scale = 0.99999 / (v_max - v_min).max()
    vertices = (vertices - center) * scale

    # Keep axis convention consistent with training/pipeline preprocess.
    y = vertices[:, 1].copy()
    vertices[:, 1] = -vertices[:, 2]
    vertices[:, 2] = y
    return trimesh.Trimesh(vertices=vertices, faces=mesh.faces, process=False)


@torch.no_grad()
def encode_decode_shape(
    mesh: trimesh.Trimesh,
    encoder,
    decoder,
    resolution: int,
    device: torch.device,
):
    vertices = torch.from_numpy(mesh.vertices).float()
    faces = torch.from_numpy(mesh.faces).long()

    voxel_indices, dual_vertices, intersected = o_voxel.convert.mesh_to_flexible_dual_grid(
        vertices.cpu(),
        faces.cpu(),
        grid_size=resolution,
        aabb=[[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
        face_weight=1.0,
        boundary_weight=0.2,
        regularization_weight=1e-2,
        timing=False,
    )

    vertices_sparse = sp.SparseTensor(
        feats=dual_vertices * resolution - voxel_indices,
        coords=torch.cat([torch.zeros_like(voxel_indices[:, 0:1]), voxel_indices], dim=-1),
    ).to(device)
    intersected_sparse = vertices_sparse.replace(intersected).to(device)

    z = encoder(vertices_sparse, intersected_sparse)
    if hasattr(decoder, "set_resolution"):
        decoder.set_resolution(resolution)
    recon = decoder(z)
    return recon[0] if isinstance(recon, list) else recon


def main():
    parser = argparse.ArgumentParser(description="GLB -> Shape VAE encode/decode -> renderer video")
    parser.add_argument("--input_glb", type=str, required=True, help="Path to input GLB")
    parser.add_argument("--output_video", type=str, default="vae_recon.mp4", help="Output MP4 path")
    parser.add_argument("--output_glb", type=str, default="vae_recon.glb", help="Output reconstructed GLB path")
    parser.add_argument("--shape_resolution", type=int, default=1024, help="Shape latent resolution")
    parser.add_argument("--render_resolution", type=int, default=512, help="Renderer resolution")
    parser.add_argument("--num_frames", type=int, default=120, help="Number of frames in output video")
    parser.add_argument(
        "--enc_pretrained",
        type=str,
        default="microsoft/TRELLIS.2-4B/ckpts/shape_enc_next_dc_f16c32_fp16",
        help="Pretrained shape encoder",
    )
    parser.add_argument(
        "--dec_pretrained",
        type=str,
        default="microsoft/TRELLIS.2-4B/ckpts/shape_dec_next_dc_f16c32_fp16",
        help="Pretrained shape decoder",
    )
    args = parser.parse_args()

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    input_glb = Path(args.input_glb)
    if not input_glb.exists():
        raise FileNotFoundError(f"Input GLB not found: {input_glb}")

    print("Loading mesh...")
    mesh = normalize_mesh(load_mesh(input_glb))

    print("Loading shape VAE encoder/decoder...")
    encoder = models.from_pretrained(args.enc_pretrained).eval().to(device)
    decoder = models.from_pretrained(args.dec_pretrained).eval().to(device)

    print("Running encoder -> decoder...")
    recon_mesh = encode_decode_shape(
        mesh=mesh,
        encoder=encoder,
        decoder=decoder,
        resolution=args.shape_resolution,
        device=device,
    )

    output_glb = Path(args.output_glb)
    output_glb.parent.mkdir(parents=True, exist_ok=True)
    trimesh.Trimesh(
        vertices=recon_mesh.vertices.detach().cpu().numpy(),
        faces=recon_mesh.faces.detach().cpu().numpy(),
        process=False,
    ).export(str(output_glb))
    print(f"Saved reconstructed mesh: {output_glb}")

    print("Rendering video...")
    render_result = render_utils.render_video(
        recon_mesh,
        resolution=args.render_resolution,
        num_frames=args.num_frames,
        return_types=["normal"],
    )
    frames = render_result["normal"]

    output_video = Path(args.output_video)
    output_video.parent.mkdir(parents=True, exist_ok=True)
    if imageio is not None:
        imageio.mimsave(str(output_video), frames, fps=15)
    else:
        h, w = frames[0].shape[:2]
        writer = cv2.VideoWriter(
            str(output_video),
            cv2.VideoWriter_fourcc(*"mp4v"),
            15,
            (w, h),
        )
        for frame in frames:
            writer.write(cv2.cvtColor(frame, cv2.COLOR_RGB2BGR))
        writer.release()
    print(f"Saved render video: {output_video}")


if __name__ == "__main__":
    main()
