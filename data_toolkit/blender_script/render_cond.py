import argparse, sys, os, math, re, glob
from typing import *
import bpy
from mathutils import Vector, Matrix
import numpy as np
import json
import glob


"""=============== BLENDER ==============="""

IMPORT_FUNCTIONS: Dict[str, Callable] = {
    "obj": bpy.ops.import_scene.obj,
    "glb": bpy.ops.import_scene.gltf,
    "gltf": bpy.ops.import_scene.gltf,
    "usd": bpy.ops.import_scene.usd,
    "fbx": bpy.ops.import_scene.fbx,
    "stl": bpy.ops.import_mesh.stl,
    "usda": bpy.ops.import_scene.usda,
    "dae": bpy.ops.wm.collada_import,
    "ply": bpy.ops.import_mesh.ply,
    "abc": bpy.ops.wm.alembic_import,
    "blend": bpy.ops.wm.append,
}

EXT = {
    'PNG': 'png',
    'JPEG': 'jpg',
    'OPEN_EXR': 'exr',
    'TIFF': 'tiff',
    'BMP': 'bmp',
    'HDR': 'hdr',
    'TARGA': 'tga'
}


def init_render(engine='CYCLES', resolution=512):
    bpy.context.scene.render.engine = engine
    bpy.context.scene.render.resolution_x = resolution
    bpy.context.scene.render.resolution_y = resolution
    bpy.context.scene.render.resolution_percentage = 100
    bpy.context.scene.render.image_settings.file_format = 'PNG'
    bpy.context.scene.render.image_settings.color_mode = 'RGBA'
    bpy.context.scene.render.film_transparent = True
    
    bpy.context.scene.cycles.device = 'GPU'
    bpy.context.scene.cycles.samples = 32
    bpy.context.scene.cycles.filter_type = 'BOX'
    bpy.context.scene.cycles.filter_width = 1
    bpy.context.scene.cycles.diffuse_bounces = 1
    bpy.context.scene.cycles.glossy_bounces = 1
    bpy.context.scene.cycles.transparent_max_bounces = 3
    bpy.context.scene.cycles.transmission_bounces = 3
    bpy.context.scene.cycles.use_denoising = True
        
    bpy.context.preferences.addons['cycles'].preferences.get_devices()
    bpy.context.preferences.addons['cycles'].preferences.compute_device_type = 'CUDA'
    

def init_scene() -> None:
    """Resets the scene to a clean state.

    Returns:
        None
    """
    # delete everything
    for obj in bpy.data.objects:
        bpy.data.objects.remove(obj, do_unlink=True)

    # delete all the materials
    for material in bpy.data.materials:
        bpy.data.materials.remove(material, do_unlink=True)

    # delete all the textures
    for texture in bpy.data.textures:
        bpy.data.textures.remove(texture, do_unlink=True)

    # delete all the images
    for image in bpy.data.images:
        bpy.data.images.remove(image, do_unlink=True)
        

def init_camera():
    cam = bpy.data.objects.new('Camera', bpy.data.cameras.new('Camera'))
    bpy.context.collection.objects.link(cam)
    bpy.context.scene.camera = cam
    cam.data.sensor_height = cam.data.sensor_width = 32
    cam_constraint = cam.constraints.new(type='TRACK_TO')
    cam_constraint.track_axis = 'TRACK_NEGATIVE_Z'
    cam_constraint.up_axis = 'UP_Y'
    cam_empty = bpy.data.objects.new("Empty", None)
    cam_empty.location = (0, 0, 0)
    bpy.context.scene.collection.objects.link(cam_empty)
    cam_constraint.target = cam_empty
    return cam


def init_uniform_lighting():
    # Clear existing lights
    bpy.ops.object.select_all(action="DESELECT")
    bpy.ops.object.select_by_type(type="LIGHT")
    bpy.ops.object.delete()
    
    # Create environment light
    if bpy.context.scene.world is None:
        world = bpy.data.worlds.new("World")
        bpy.context.scene.world = world
    else:
        world = bpy.context.scene.world
        
    # Enabling nodes
    world.use_nodes = True
    node_tree = world.node_tree
    nodes = node_tree.nodes
    links = node_tree.links
    
    # Remove default nodes
    for node in nodes:
        nodes.remove(node)

    # Create background node
    bg_node = nodes.new(type="ShaderNodeBackground")
    bg_node.inputs["Color"].default_value = (1.0, 1.0, 1.0, 1.0)
    bg_node.inputs["Strength"].default_value = 1.0
    output_node = nodes.new(type="ShaderNodeOutputWorld")
    links.new(bg_node.outputs["Background"], output_node.inputs["Surface"])


def init_no_lighting():
    # Clear explicit lights. Shadeless material modes use emission, so the world can stay dark.
    bpy.ops.object.select_all(action="DESELECT")
    bpy.ops.object.select_by_type(type="LIGHT")
    bpy.ops.object.delete()

    if bpy.context.scene.world is None:
        world = bpy.data.worlds.new("World")
        bpy.context.scene.world = world
    else:
        world = bpy.context.scene.world

    world.use_nodes = True
    nodes = world.node_tree.nodes
    links = world.node_tree.links
    for node in nodes:
        nodes.remove(node)

    bg_node = nodes.new(type="ShaderNodeBackground")
    bg_node.inputs["Color"].default_value = (1.0, 1.0, 1.0, 1.0)
    bg_node.inputs["Strength"].default_value = 0.0
    output_node = nodes.new(type="ShaderNodeOutputWorld")
    links.new(bg_node.outputs["Background"], output_node.inputs["Surface"])


def init_random_lighting(camera_dir: np.ndarray) -> None:
    # Clear existing lights
    bpy.ops.object.select_all(action="DESELECT")
    bpy.ops.object.select_by_type(type="LIGHT")
    bpy.ops.object.delete()
    
    # Create environment light
    if bpy.context.scene.world is None:
        world = bpy.data.worlds.new("World")
        bpy.context.scene.world = world
    else:
        world = bpy.context.scene.world
        
    # Enabling nodes
    world.use_nodes = True
    node_tree = world.node_tree
    nodes = node_tree.nodes
    links = node_tree.links
    
    # Remove default nodes
    for node in nodes:
        nodes.remove(node)
    
    # Random place lights
    num_lights = np.random.randint(1, 4)
    total_strength = 1.5
    for i in range(num_lights):
        new_light = bpy.data.objects.new(f"Light_{i}", bpy.data.lights.new(f"Light_{i}", type="POINT"))
        bpy.context.collection.objects.link(new_light)
        
        new_light_distance = 1 / np.random.uniform(1/100, 1/10)
        new_light_dir = np.random.randn(3)
        new_light_dir[2] += 0.6
        new_light_dir = new_light_dir / np.linalg.norm(new_light_dir)
        new_light_location = new_light_dir * new_light_distance
        new_light_camera_strength_ratio = max(np.sum(camera_dir * new_light_dir) * 0.5 + 0.5, 0)
        new_light_max_energy = total_strength / (np.sum(camera_dir * new_light_dir) * 0.45 + 0.55)
        new_light_strength = np.sqrt(np.random.uniform(0.01, 1)) * new_light_max_energy
        new_light_camera_strength = new_light_camera_strength_ratio * new_light_strength
        total_strength -= new_light_camera_strength
        
        new_light.location = (new_light_location[0], new_light_location[1], new_light_location[2])
        new_light.data.color = (1.0, 1.0, 1.0)
        new_light.data.energy = new_light_strength * new_light_distance**2 * 31.4
        new_light.data.shadow_soft_size = np.random.uniform(0.1, 0.1 * new_light_distance)
        
    # Create background node
    bg_node = nodes.new(type="ShaderNodeBackground")
    bg_node.inputs["Color"].default_value = (1.0, 1.0, 1.0, 1.0)
    bg_node.inputs["Strength"].default_value = total_strength
    output_node = nodes.new(type="ShaderNodeOutputWorld")
    links.new(bg_node.outputs["Background"], output_node.inputs["Surface"])


def load_object(object_path: str) -> None:
    """Loads a model with a supported file extension into the scene.

    Args:
        object_path (str): Path to the model file.

    Raises:
        ValueError: If the file extension is not supported.

    Returns:
        None
    """
    file_extension = object_path.split(".")[-1].lower()
    if file_extension is None:
        raise ValueError(f"Unsupported file type: {object_path}")

    if file_extension == "usdz":
        # install usdz io package
        dirname = os.path.dirname(os.path.realpath(__file__))
        usdz_package = os.path.join(dirname, "io_scene_usdz.zip")
        bpy.ops.preferences.addon_install(filepath=usdz_package)
        # enable it
        addon_name = "io_scene_usdz"
        bpy.ops.preferences.addon_enable(module=addon_name)
        # import the usdz
        from io_scene_usdz.import_usdz import import_usdz

        import_usdz(context, filepath=object_path, materials=True, animations=True)
        return None

    # load from existing import functions
    import_function = IMPORT_FUNCTIONS[file_extension]

    print(f"Loading object from {object_path}")
    if file_extension == "blend":
        import_function(directory=object_path, link=False)
    elif file_extension in {"glb", "gltf"}:
        import_function(filepath=object_path, merge_vertices=True, import_shading='NORMALS')
    else:
        import_function(filepath=object_path)
        

def delete_invisible_objects() -> None:
    """Deletes all invisible objects in the scene.

    Returns:
        None
    """
    # bpy.ops.object.mode_set(mode="OBJECT")
    bpy.ops.object.select_all(action="DESELECT")
    for obj in bpy.context.scene.objects:
        if obj.hide_viewport or obj.hide_render:
            obj.hide_viewport = False
            obj.hide_render = False
            obj.hide_select = False
            obj.select_set(True)
    bpy.ops.object.delete()

    # Delete invisible collections
    invisible_collections = [col for col in bpy.data.collections if col.hide_viewport]
    for col in invisible_collections:
        bpy.data.collections.remove(col)


def unhide_all_objects() -> None:
    """Unhides all objects in the scene.

    Returns:
        None
    """
    for obj in bpy.context.scene.objects:
        obj.hide_set(False)
        
        
def convert_to_meshes() -> None:
    """Converts all objects in the scene to meshes.

    Returns:
        None
    """
    bpy.ops.object.select_all(action="DESELECT")
    bpy.context.view_layer.objects.active = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"][0]
    for obj in bpy.context.scene.objects:
        obj.select_set(True)
    bpy.ops.object.convert(target="MESH")
    
        
def triangulate_meshes() -> None:
    """Triangulates all meshes in the scene.

    Returns:
        None
    """
    bpy.ops.object.select_all(action="DESELECT")
    objs = [obj for obj in bpy.context.scene.objects if obj.type == "MESH"]
    bpy.context.view_layer.objects.active = objs[0]
    for obj in objs:
        obj.select_set(True)
    bpy.ops.object.mode_set(mode="EDIT")
    bpy.ops.mesh.reveal()
    bpy.ops.mesh.select_all(action="SELECT")
    bpy.ops.mesh.quads_convert_to_tris(quad_method="BEAUTY", ngon_method="BEAUTY")
    bpy.ops.object.mode_set(mode="OBJECT")
    bpy.ops.object.select_all(action="DESELECT")
    

def scene_bbox() -> Tuple[Vector, Vector]:
    """Returns the bounding box of the scene.

    Taken from Shap-E rendering script
    (https://github.com/openai/shap-e/blob/main/shap_e/rendering/blender/blender_script.py#L68-L82)

    Returns:
        Tuple[Vector, Vector]: The minimum and maximum coordinates of the bounding box.
    """
    bbox_min = (math.inf,) * 3
    bbox_max = (-math.inf,) * 3
    found = False
    scene_meshes = [obj for obj in bpy.context.scene.objects.values() if isinstance(obj.data, bpy.types.Mesh)]
    for obj in scene_meshes:
        found = True
        for coord in obj.bound_box:
            coord = Vector(coord)
            coord = obj.matrix_world @ coord
            bbox_min = tuple(min(x, y) for x, y in zip(bbox_min, coord))
            bbox_max = tuple(max(x, y) for x, y in zip(bbox_max, coord))
    if not found:
        raise RuntimeError("no objects in scene to compute bounding box for")
    return Vector(bbox_min), Vector(bbox_max)


def normalize_scene() -> Tuple[float, Vector]:
    """Normalizes the scene by scaling and translating it to fit in a unit cube centered
    at the origin.

    Mostly taken from the Point-E / Shap-E rendering script
    (https://github.com/openai/point-e/blob/main/point_e/evals/scripts/blender_script.py#L97-L112),
    but fix for multiple root objects: (see bug report here:
    https://github.com/openai/shap-e/pull/60).

    Returns:
        Tuple[float, Vector]: The scale factor and the offset applied to the scene.
    """
    scene_root_objects = [obj for obj in bpy.context.scene.objects.values() if not obj.parent]
    if len(scene_root_objects) > 1:
        # create an empty object to be used as a parent for all root objects
        scene = bpy.data.objects.new("ParentEmpty", None)
        bpy.context.scene.collection.objects.link(scene)

        # parent all root objects to the empty object
        for obj in scene_root_objects:
            obj.parent = scene
    else:
        scene = scene_root_objects[0]

    bbox_min, bbox_max = scene_bbox()
    scale = 1 / max(bbox_max - bbox_min)
    scene.scale = scene.scale * scale

    # Apply scale to matrix_world.
    bpy.context.view_layer.update()
    bbox_min, bbox_max = scene_bbox()
    offset = -(bbox_min + bbox_max) / 2
    scene.matrix_world.translation += offset
    bpy.ops.object.select_all(action="DESELECT")
    
    return scale, offset


def get_transform_matrix(obj: bpy.types.Object) -> list:
    pos, rt, _ = obj.matrix_world.decompose()
    rt = rt.to_matrix()
    matrix = []
    for ii in range(3):
        a = []
        for jj in range(3):
            a.append(rt[ii][jj])
        a.append(pos[ii])
        matrix.append(a)
    matrix.append([0, 0, 0, 1])
    return matrix


def _get_material_output(mat: bpy.types.Material) -> bpy.types.Node:
    output = mat.node_tree.nodes.get('Material Output')
    if output is None:
        output = mat.node_tree.nodes.new(type='ShaderNodeOutputMaterial')
    return output


def _get_principled_node(mat: bpy.types.Material) -> bpy.types.Node:
    principled = mat.node_tree.nodes.get('Principled BSDF')
    if principled is not None:
        return principled
    for node in mat.node_tree.nodes:
        if node.type == 'BSDF_PRINCIPLED':
            return node
    return None


def _socket_source(socket: bpy.types.NodeSocket):
    if socket is not None and socket.is_linked:
        return socket.links[0].from_socket
    return None


def _socket_default(socket: bpy.types.NodeSocket, default):
    if socket is None:
        return default
    return socket.default_value


def _socket_scalar_default(socket: bpy.types.NodeSocket, default: float) -> float:
    value = _socket_default(socket, default)
    if isinstance(value, (float, int)):
        return float(value)
    return float(value[0])


def _socket_color_default(socket: bpy.types.NodeSocket, default=(1.0, 1.0, 1.0, 1.0)):
    value = _socket_default(socket, default)
    if isinstance(value, (float, int)):
        return (float(value), float(value), float(value), 1.0)
    if len(value) == 3:
        return (value[0], value[1], value[2], 1.0)
    return value


def _ensure_mesh_materials() -> None:
    default_mat = None
    for obj in bpy.context.scene.objects:
        if obj.type != 'MESH':
            continue
        if len(obj.data.materials) > 0:
            continue
        if default_mat is None:
            default_mat = bpy.data.materials.new('Default_Material')
            default_mat.diffuse_color = (1.0, 1.0, 1.0, 1.0)
        obj.data.materials.append(default_mat)


def _set_shadeless_materials(material_mode: str) -> None:
    _ensure_mesh_materials()
    for mat in bpy.data.materials:
        mat.use_nodes = True
        mat.blend_method = 'BLEND'
        mat.show_transparent_back = True

        node_tree = mat.node_tree
        nodes = node_tree.nodes
        links = node_tree.links
        principled = _get_principled_node(mat)
        output = _get_material_output(mat)

        base_socket = principled.inputs.get('Base Color') if principled else None
        metallic_socket = principled.inputs.get('Metallic') if principled else None
        roughness_socket = principled.inputs.get('Roughness') if principled else None
        alpha_socket = principled.inputs.get('Alpha') if principled else None

        base_source = _socket_source(base_socket)
        metallic_source = _socket_source(metallic_socket)
        roughness_source = _socket_source(roughness_socket)
        alpha_source = _socket_source(alpha_socket)

        base_default = _socket_color_default(base_socket, mat.diffuse_color)
        metallic_default = _socket_scalar_default(metallic_socket, 0.0)
        roughness_default = _socket_scalar_default(roughness_socket, 0.5)
        alpha_default = _socket_scalar_default(alpha_socket, mat.diffuse_color[3])

        for link in list(output.inputs['Surface'].links):
            links.remove(link)

        emission = nodes.new(type='ShaderNodeEmission')
        emission.inputs['Strength'].default_value = 1.0

        if material_mode == 'mra':
            combine = nodes.new(type='ShaderNodeCombineRGB')
            if metallic_source is not None:
                links.new(metallic_source, combine.inputs['R'])
            else:
                combine.inputs['R'].default_value = metallic_default
            if roughness_source is not None:
                links.new(roughness_source, combine.inputs['G'])
            else:
                combine.inputs['G'].default_value = roughness_default
            if alpha_source is not None:
                links.new(alpha_source, combine.inputs['B'])
            else:
                combine.inputs['B'].default_value = alpha_default
            links.new(combine.outputs['Image'], emission.inputs['Color'])
        else:
            if base_source is not None:
                links.new(base_source, emission.inputs['Color'])
            else:
                emission.inputs['Color'].default_value = base_default

        transparent = nodes.new(type='ShaderNodeBsdfTransparent')
        mix = nodes.new(type='ShaderNodeMixShader')
        if alpha_source is not None:
            links.new(alpha_source, mix.inputs['Fac'])
        else:
            mix.inputs['Fac'].default_value = alpha_default
        links.new(transparent.outputs['BSDF'], mix.inputs[1])
        links.new(emission.outputs['Emission'], mix.inputs[2])
        links.new(mix.outputs['Shader'], output.inputs['Surface'])


def _set_camera_from_transform(cam: bpy.types.Object, frame: dict) -> None:
    for constraint in list(cam.constraints):
        cam.constraints.remove(constraint)
    cam.matrix_world = Matrix(frame['transform_matrix'])
    cam.data.lens = 16 / np.tan(frame['camera_angle_x'] / 2)


def main(arg):
    if arg.seed is not None:
        np.random.seed(arg.seed)

    if arg.object.endswith(".blend"):
        delete_invisible_objects()
    else:
        init_scene()
        load_object(arg.object)
    print('[INFO] Scene initialized.')
    
    # normalize scene
    scale, offset = normalize_scene()
    print('[INFO] Scene normalized.')
    
    # Initialize camera and lighting
    cam = init_camera()
    use_shadeless = arg.render_mode == 'mra'
    if use_shadeless:
        init_no_lighting()
        _set_shadeless_materials('mra')
    else:
        # lit / uniform_lit: same materials as default lit pipeline; uniform_lit skips per-view random lights below
        init_uniform_lighting()
    print('[INFO] Camera and lighting initialized.')
        
    # ============= Render conditional views =============
    init_render(engine=arg.engine, resolution=arg.cond_resolution)
    os.makedirs(arg.cond_output_folder, exist_ok=True)
    # Create a list of views
    to_export = {
        "aabb": [[-0.5, -0.5, -0.5], [0.5, 0.5, 0.5]],
        "scale": scale,
        "offset": [offset.x, offset.y, offset.z],
        "frames": []
    }
    if arg.transforms_json is not None:
        with open(arg.transforms_json, 'r') as f:
            source_transforms = json.load(f)
        views = source_transforms['frames']
    else:
        views = json.loads(arg.cond_views)

    for i, view in enumerate(views):
        if arg.transforms_json is not None:
            _set_camera_from_transform(cam, view)
            cam_dir = np.array(tuple(cam.location))
            cam_dir = cam_dir / np.linalg.norm(cam_dir)
            camera_angle_x = view['camera_angle_x']
            file_path = view.get('file_path', f'{i:03d}.png')
        else:
            cam_dir = np.array([
                np.cos(view['yaw']) * np.cos(view['pitch']),
                np.sin(view['yaw']) * np.cos(view['pitch']),
                np.sin(view['pitch'])
            ])
            cam.location = (
                view['radius'] * cam_dir[0],
                view['radius'] * cam_dir[1],
                view['radius'] * cam_dir[2]
            )
            cam.data.lens = 16 / np.tan(view['fov'] / 2)
            camera_angle_x = view['fov']
            file_path = f'{i:03d}.png'
        if arg.render_mode == 'lit':
            init_random_lighting(cam_dir)
        
        bpy.context.scene.render.filepath = os.path.join(arg.cond_output_folder, file_path)
            
        # Render the scene
        bpy.ops.render.render(write_still=True)
        bpy.context.view_layer.update()
            
        # Save camera parameters
        metadata = {
            "file_path": file_path,
            "camera_angle_x": camera_angle_x,
            "transform_matrix": get_transform_matrix(cam)
        }
        to_export["frames"].append(metadata)
    
    # Save the camera parameters
    with open(os.path.join(arg.cond_output_folder, 'transforms.json'), 'w') as f:
        json.dump(to_export, f, indent=4)

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Renders given obj file by rotation a camera around it.')
    parser.add_argument('--object', type=str, help='Path to the 3D model file to be rendered.')
    parser.add_argument('--cond_views', type=str, help='JSON string of views. Contains a list of {yaw, pitch, radius, fov} object.')
    parser.add_argument('--transforms_json', type=str, default=None, help='Path to an existing transforms.json to reuse camera parameters.')
    parser.add_argument('--cond_output_folder', type=str, default='/tmp', help='The path the output will be dumped to.')
    parser.add_argument('--cond_resolution', type=int, default=1024, help='Resolution of the conditional images.')
    parser.add_argument('--engine', type=str, default='CYCLES', help='Blender internal engine for rendering. E.g. CYCLES, BLENDER_EEVEE, ...')
    parser.add_argument('--render_mode', type=str, default='lit', choices=['lit', 'uniform_lit', 'mra'], help='lit: uniform env + random lights per view; uniform_lit: same lit materials but uniform env only; mra: shadeless channel output.')
    parser.add_argument('--seed', type=int, default=None, help='Optional seed for Blender-side random lighting.')
    argv = sys.argv[sys.argv.index("--") + 1:]
    args = parser.parse_args(argv)
    main(args)
    