"""Endpoint for downloading files.

Note: this is only for development. Do not use it in production.
"""
# import flask
# from flask import send_from_directory
# import dataserver
# # imports for experimental functions
# import os
# import numpy as np
# from PIL import Image, ImageFilter
# from rio_tiler.io import COGReader


# @dataserver.app.route("/download/<filename>")
# def download_file(filename):
#     """Download a file."""
#     return send_from_directory(
#         dataserver.app.config["DOWNLOAD_FOLDER"], filename
#     )


# # @dataserver.app.route("/tiles/<zoom>/<x>/<y>.png")
# # def serve_tile(zoom, x, y):
# #     """Serve tile images for the map."""
# #     return send_from_directory(f"static/tiles/{zoom}", f"{x}_{y}.png")


# @dataserver.app.route("/tiles/<z>/<x>/<y>.png")
# def serve_tile(z, x, y):
#     """Serve tile images for the map."""
#     if os.path.exists(f"static/tiles/{z}/{x}_{y}.png"):
#         return send_from_directory(f"static/tiles/{z}", f"{x}_{y}.png")
#     tif_path = "output.tif"
#     with COGReader(tif_path) as cog:
#         try:
#             # Get the data for this specific tile

#             tile_data_3d, _ = cog.tile(int(x), int(y), int(z))

#             tile_data = tile_data_3d[0, :, :]

#             range_val = tile_data.max() - tile_data.min()
#             if range_val != 0:
#                 img_data = 255 - ((tile_data - tile_data.min()) *
#                                   (255 / range_val)).astype(np.uint8)
#             else:
#                 img_data = np.zeros_like(tile_data, dtype=np.uint8)

#             # Convert the data to an image
#             img = Image.fromarray(img_data, 'L')

#             # Rescale the image
#             rescaled_img = img.filter(ImageFilter.GaussianBlur(radius=4))
#             rescaled_img.save(f'dataserver/static/tiles/{z}/{x}_{y}.png')
#         except Exception as error:
#             print(error)
#     return send_from_directory(f"static/tiles/{z}", f"{x}_{y}.png")
