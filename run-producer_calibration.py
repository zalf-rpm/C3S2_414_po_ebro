#!/usr/bin/python
# -*- coding: UTF-8

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/. */

# Authors:
# Michael Berg-Mohnicke <michael.berg@zalf.de>
#
# Maintainers:
# Currently maintained by the authors.
#
# Copyright (C: Leibniz Centre for Agricultural Landscape Research (ZALF)

from datetime import datetime
import re
import capnp
import json
import numpy as np
import os
from pathlib import Path
from pyproj import CRS, Transformer
import sqlite3
import sys
import time
import zmq
import geopandas as gpd
import rasterio

import monica_io3
import soil_io3
import monica_run_lib as Mrunlib

PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_MAS_INFRASTRUCTURE_REPO = PATH_TO_REPO / "../mas-infrastructure"
PATH_TO_PYTHON_CODE = PATH_TO_MAS_INFRASTRUCTURE_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from pkgs.common import common
from pkgs.model import monica_io3

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_MAS_INFRASTRUCTURE_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
fbp_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "fbp.capnp"), imports=abs_imports)

PATHS = {
    # adjust the local path to your environmen
    "ow-local-remote": {
        # "include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
    },
    "mbm-local-remote": {
        # "include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
    },
    "mbm-local-local": {
        # "include-file-base-path": "/home/berg/GitHub/monica-parameters/", # path to monica-parameters
        "path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/run/user/1000/gvfs/sftp:host=login01.cluster.zalf.de,user=rpm/beegfs/common/data/climate/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
    },
    "hpc-local-remote": {
        # "path-to-climate-dir": "/beegfs/common/data/soil/global_soil_dataset_for_earth_system_modeling/",
        # mounted path to archive or hard drive with climate data
        "path-to-climate-dir": "/beegfs/common/data/climate/",  # mounted path to archive or hard drive with climate data
        "monica-path-to-climate-dir": "/monica_data/climate-data/",
        #"monica-path-to-climate-dir": "/beegfs/common/data/climate/",
        # mounted path to archive accessable by monica executable
        "path-to-data-dir": "./data/",  # mounted path to archive or hard drive with data
    }
}

def run_producer(server={"server": None, "port": None}):
    context = zmq.Context()
    socket = context.socket(zmq.PUSH)  # pylint: disable=no-member
    # config_and_no_data_socket = context.socket(zmq.PUSH)

    config = {
        "mode": "hpc-local-remote",
        "server-port": server["port"] if server["port"] else "6666",
        "server": server["server"] if server["server"] else "login01.cluster.zalf.de",
        "start-row": "0",
        "end-row": "-1",
        "path_to_dem_grid": "",
        "sim.json": "sim_calibration.json",
        "crop.json": "crop_calibration.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_calibration.csv", 
        "run-setups": "[1]",
        "reader_sr": None,
        "path_to_out": "out/",
        "only_nuts3_region_ids": "[]",  ## Define on rundeck ##
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    path_to_out_file = config["path_to_out"] + "/producer.out"
    if not os.path.exists(config["path_to_out"]):
        try:
            os.makedirs(config["path_to_out"])
        except OSError:
            print("run-calibration-producer.py: Couldn't create dir:", config["path_to_out"], "!")
    with open(path_to_out_file, "a") as _:
        _.write(f"config: {config}\n")

    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} start producer in producer\n") 

    nuts3_region_ids = json.loads(config["only_nuts3_region_ids"])

    # read setup from csv file
    setups = Mrunlib.read_sim_setups(config["setups-file"])
    run_setups = json.loads(config["run-setups"])
    print("read sim setups: ", config["setups-file"])

    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} setup read\n") 

    if len(run_setups) < 1:
        return

    setup_id = run_setups[0]
    setup = setups[setup_id]

    region = setup["region"]
    crop_id = setup["crop-id"]
    crop_code = crop_id.split("_")[0]

     # select paths
    paths = PATHS[config["mode"]]

    DATA_SOIL_DB = f"data/{region}/{region}_soil.sqlite"
    DATA_GRID_HEIGHT = f"data/{region}/{region}_dem_4326.asc"
    DATA_GRID_SLOPE = f"data/{region}/{region}_slope_4326.asc"
    DATA_GRID_SOIL = f"data/{region}/{region}_soil_4326.asc"
    DATA_GRID_CROPS = f"data/{region}/{region}_crop_{crop_code}_4326.asc" ## Define per crop ##
    TEMPLATE_PATH_LATLON = f"data/{region}/{region}_latlon_to_rowcol.json"

    # Additional data for masking the regions
    NUTS3_REGIONS = f"data/{region}/{region}_nuts3_4326.shp"
    NUTS3_REGIONS_GRID = f"data/{region}/{region}_nuts3_regions_4326.asc"

    #TEMPLATE_PATH_HARVEST = "{path_to_data_dir}/projects/monica-germany/ILR_SEED_HARVEST_doys_{crop_id}.csv"

    gdf = gpd.read_file(NUTS3_REGIONS)

    # soil_db_path = paths["path-to-data-dir"] + DATA_SOIL_DB
    soil_db_path = DATA_SOIL_DB
    # subprocess.run(["wget", "-O", soil_db_path, SOIL_DB_URL], check=True)
    #print("Downloaded soil db successfully.")

    # open soil db connection
    # soil_db_con = sqlite3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB)
    soil_db_con = sqlite3.connect(soil_db_path)
    print("Connected to soil db successfully.")
    # soil_db_con = cas_sq3.connect(paths["path-to-data-dir"] + DATA_SOIL_DB) #CAS.
    # connect to monica proxy (if local, it will try to connect to a locally started monica)
    socket.connect("tcp://" + config["server"] + ":" + str(config["server-port"]))

    # transforms geospatial coordinates from one coordinate reference system to another
    # transform wgs84 into gk5
    soil_crs_to_x_transformers = {}
    wgs84_crs = CRS.from_epsg(4326)
    # transformers[wgs84] = Transformer.from_crs(wgs84_crs, gk5_crs, always_xy=True)

    #ilr_seed_harvest_data = defaultdict(
    #    lambda: {"interpolate": None, "data": defaultdict(dict), "is-winter-crop": None})

    # Load grids
    ## note numpy is able to load from a compressed file, ending with .gz or .bz2

    # soil data
    path_to_soil_grid = paths["path-to-data-dir"] + DATA_GRID_SOIL
    soil_epsg_code = int(path_to_soil_grid.split("/")[-1].split("_")[3])
    soil_crs = CRS.from_epsg(soil_epsg_code)
    if wgs84_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[wgs84_crs] = Transformer.from_crs(soil_crs, wgs84_crs)
    soil_metadata, _ = Mrunlib.read_header(path_to_soil_grid)
    soil_grid = np.loadtxt(path_to_soil_grid, dtype=int, skiprows=6)
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} read: {path_to_soil_grid}\n")

    # height data
    path_to_dem_grid = paths["path-to-data-dir"] + DATA_GRID_HEIGHT
    dem_epsg_code = int(path_to_dem_grid.split("/")[-1].split("_")[3])
    dem_crs = CRS.from_epsg(dem_epsg_code)
    if dem_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[dem_crs] = Transformer.from_crs(soil_crs, dem_crs)
    dem_metadata, _ = Mrunlib.read_header(path_to_dem_grid)
    dem_grid = np.loadtxt(path_to_dem_grid, dtype=float, skiprows=6)
    dem_interpolate = Mrunlib.create_ascii_grid_interpolator(dem_grid, dem_metadata)
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} read: {path_to_dem_grid}\n")

    # slope data
    path_to_slope_grid = paths["path-to-data-dir"] + DATA_GRID_SLOPE
    slope_epsg_code = int(path_to_slope_grid.split("/")[-1].split("_")[3])
    slope_crs = CRS.from_epsg(slope_epsg_code)
    if slope_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[slope_crs] = Transformer.from_crs(soil_crs, slope_crs)
    slope_metadata, _ = Mrunlib.read_header(path_to_slope_grid)
    slope_grid = np.loadtxt(path_to_slope_grid, dtype=float, skiprows=6)
    slope_interpolate = Mrunlib.create_ascii_grid_interpolator(slope_grid, slope_metadata)
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} read: {path_to_slope_grid}\n")

    # crop mask data
    path_to_crop_grid = paths["path-to-data-dir"] + DATA_GRID_CROPS
    crop_epsg_code = int(path_to_crop_grid.split("/")[-1].split("_")[4])
    crop_crs = CRS.from_epsg(crop_epsg_code)
    if crop_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[crop_crs] = Transformer.from_crs(soil_crs, crop_crs)
    crop_meta, _ = Mrunlib.read_header(path_to_crop_grid)
    crop_grid = np.loadtxt(path_to_crop_grid, dtype=int, skiprows=6)
    crop_interpolate = Mrunlib.create_ascii_grid_interpolator(crop_grid, crop_meta)
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} read: {path_to_crop_grid}\n")

    # nuts3_regions
    path_to_nuts3_regions_grid = paths["path-to-data-dir"] + NUTS3_REGIONS_GRID
    nuts3_regions_epsg_code = int(path_to_nuts3_regions_grid.split("/")[-1].split("_")[4])
    nuts3_regions_crs = CRS.from_epsg(nuts3_regions_epsg_code)
    if nuts3_regions_crs not in soil_crs_to_x_transformers:
        soil_crs_to_x_transformers[nuts3_regions_crs] = Transformer.from_crs(soil_crs, nuts3_regions_crs)
    nuts3_regions_metadata, _ = Mrunlib.read_header(path_to_nuts3_regions_grid)
    nuts3_regions_grid = np.loadtxt(path_to_nuts3_regions_grid, dtype=float, skiprows=6)
    nuts3_regions_interpolate = Mrunlib.create_ascii_grid_interpolator(nuts3_regions_grid, nuts3_regions_metadata)
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} read: {path_to_nuts3_regions_grid}\n")

    cdict = {}
    # path to latlon-to-rowcol.json
    path = paths["path-to-data-dir"] + TEMPLATE_PATH_LATLON
    climate_data_interpolator = (
        Mrunlib.create_climate_geoGrid_interpolator_from_json_file(
            path, wgs84_crs, soil_crs, cdict
        )
    )
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} created climate_data to soil_crs interpolator: {path}\n")

    # Create the function for the mask. This function will later use the additional column in a setup file!
    def create_mask_from_shapefile(NUTS3_REGIONS, region_name, path_to_soil_grid):
        regions_df = gpd.read_file(NUTS3_REGIONS)
        region = regions_df[regions_df["NUTS_ID"] == region_name]

        # This is needed to read the transformation data correctly from the file. With the original opening it does not work
        with rasterio.open(path_to_soil_grid) as dataset:
            soil_grid = dataset.read(1)
            transform = dataset.transform

        rows, cols = soil_grid.shape
        mask = rasterio.features.geometry_mask([region.geometry.values[0]], out_shape=(rows, cols), transform=transform,
                                               invert=True)

        return mask

    conman = common.ConnectionManager()
    reader = conman.try_connect(config["reader_sr"], cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
    if reader:
        sent_env_count = 0
        while True:
            msg = reader.read().wait()
            # check for end of data from in port
            if msg.which() == "done":
                break

            with open(path_to_out_file, "a") as _:
                _.write(f"{datetime.now()} connected\n") 

            env_template = None
            start_setup_time = None
            try:
                in_ip = msg.value.as_struct(fbp_capnp.IP)
                s: str = in_ip.content.as_text()
                params = json.loads(s)  # keys: MaxAssimilationRate, AssimilateReallocation, RootPenetrationRate
                if "only_nuts3_region_ids" in params:
                    nuts3_region_ids = params["only_nuts3_region_ids"]
                    del params["only_nuts3_region_ids"]

                start_setup_time = time.perf_counter()

                #setup = setups[setup_id]
                #crop_id = setup["crop-id"]
                #region_name = setup["region_name"]

                ## extract crop_id from crop-id name that has possible an extension
                crop_id_short = crop_id.split('_')[0]

                #with open(config["path_to_out"] + "/spot_setup.out", "a") as _:
                #    _.write(f"{datetime.now()} setup started producer\n") 

                #if region_name and len(region_name) > 0:
                    # Create the soil mask for the specific region
                    #path_to_soil_grid_ow = paths["path-to-data-dir"] + DATA_GRID_SOIL_OW
                    #mask = create_mask_from_shapefile(NUTS3_REGIONS, region_name, path_to_soil_grid_ow)

                    # Apply the soil mask to the soil grid
                    #soil_grid_copy = soil_grid.copy()
                    #soil_grid[mask == False] = -8888
                    #soil_grid[soil_grid_copy == -9999] = -9999

                # add crop id from setup file
                #try:
                #    # read seed/harvest dates for each crop_id
                #    path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir=paths["path-to-data-dir"],
                #                                                crop_id=crop_id_short)
                #    print("created seed harvest gk5 interpolator and read data: ", path_harvest)
                #    Mrunlib.create_seed_harvest_geoGrid_interpolator_and_read_data(path_harvest, wgs84_crs, utm32_crs,
                #                                                                          ilr_seed_harvest_data)
                #except IOError:
                #    path_harvest = TEMPLATE_PATH_HARVEST.format(path_to_data_dir=paths["path-to-data-dir"],
                #                                                crop_id=crop_id_short)
                #    print("Couldn't read file:", path_harvest)
                #    continue

                #with open(path_to_out_file, "a") as _:
                #    _.write(f"{datetime.now()} crop added producer\n") 

                # read template sim.json
                with open(setup.get("sim.json", config["sim.json"])) as _:
                    sim_json = json.load(_)
                # change start and end date according to setup
                if setup["start_date"]:
                    sim_json["climate.csv-options"]["start-date"] = str(setup["start_date"])
                if setup["end_date"]:
                    sim_json["climate.csv-options"]["end-date"] = str(setup["end_date"])
                    # sim_json["include-file-base-path"] = paths["include-file-base-path"]

                # read template site.json
                with open(setup.get("site.json", config["site.json"])) as _:
                    site_json = json.load(_)

                with open(path_to_out_file, "a") as _:
                    _.write(f"{datetime.now()} read site and sim json producer\n\n")

                #site_json["EnvironmentParameters"]["rcp"] = scenario

                # read template crop.json
                with open(setup.get("crop.json", config["crop.json"])) as _:
                    crop_json = json.load(_)
                    crop_json["cropRotation"][2] = crop_id
                    real_crop_id = None
                    # set value of calibration params
                    for ws in crop_json["cropRotationTemplates"][crop_id][0]["worksteps"]:
                        if "Sowing" in ws["type"]:
                            real_crop_id = ws["crop"][2]
                    if real_crop_id:
                        ps = crop_json["crops"][real_crop_id]["cropParams"]
                        for sampled_name, sampled_value in params.items():
                            # separate array index
                            parts = sampled_name.rsplit("_", 1)
                            if len(parts) == 2 and parts[1].isdigit():
                                base_name = parts[0]
                                position_in_array = int(parts[1])
                            else:
                                base_name = sampled_name
                                position_in_array = None
                            
                            # check whether sampled value is a scaling factor
                            is_factor = base_name.endswith("Factor")
                            pname = base_name.removesuffix("Factor")

                            # parameter group (species, cultivar)
                            if pname in ps["species"]:
                                ptype = "species"
                            elif pname in ps["cultivar"]:
                                ptype = "cultivar"
                            else:
                                continue

                            # explicitly specificed position in array
                            if position_in_array is not None:
                                if is_factor:
                                    ps[ptype][pname][position_in_array] *= sampled_value
                                else:
                                    ps[ptype][pname][position_in_array] = sampled_value
                                continue

                            # default target positions
                            indices = None
                            if pname == "StageTemperatureSum":
                                indices = range(0,6)
                            elif pname == "VernalisationRequirement":
                                indices = range(0,6)
                            elif pname == "BaseDaylength":
                                indices = range(2,4)
                            elif pname == "DaylengthRequirement":
                                indices = range(1,4)
                            elif pname == "SpecificLeafArea":
                                indices = range(0,6)

                            # apply sampled value
                            if indices is not None:
                                for index in indices:
                                    if is_factor:
                                        ps[ptype][pname][index] *= sampled_value
                                    else:
                                        ps[ptype][pname][index] = sampled_value
                            else:
                                if is_factor:
                                    ps[ptype][pname] *= sampled_value
                                else:
                                    ps[ptype][pname] = sampled_value

                            # additional parameter changes
                            if pname == "StageTemperatureSum" and is_factor:
                                ps["cultivar"]["BeginSensitivePhaseHeatStress"] *= sampled_value
                                ps["cultivar"]["EndSensitivePhaseHeatStress"] *= sampled_value

                    else:
                        with open(path_to_out_file, "a") as _:
                            _.write(
                                f"{datetime.now()} Error couldn't find sowing workstep in crop.json\n"
                            )
                        exit(1)

                crop_json["CropParameters"]["__enable_vernalisation_factor_fix__"] = setup[
                    "use_vernalisation_fix"] if "use_vernalisation_fix" in setup else False

                # create environment template from json templates
                env_template = monica_io3.create_env_json_from_json_config({
                    "crop": crop_json,
                    "site": site_json,
                    "sim": sim_json,
                    "climate": ""
                })

                scols = int(soil_metadata["ncols"])
                srows = int(soil_metadata["nrows"])
                scellsize = int(soil_metadata["cellsize"])
                xllcorner = int(soil_metadata["xllcorner"])
                yllcorner = int(soil_metadata["yllcorner"])
                nodata_value = int(soil_metadata["nodata_value"])

                # unknown_soil_ids = set()
                soil_id_cache = {}
                #with open(path_to_out_file, "a") as _:
                #    _.write(f"{datetime.now()} All Rows x Cols: {srows} x {scols}\n")
                for srow in range(0, srows):
                    #with open(path_to_out_file, "a") as _:
                    #    _.write(f"{srow}, ")
                    #print(srow, end=", ")

                    if srow < int(config["start-row"]):
                        continue
                    elif int(config["end-row"]) > 0 and srow > int(config["end-row"]):
                        break

                    for scol in range(0, scols):
                        soil_id = int(soil_grid[srow, scol])
                        if soil_id == nodata_value:
                            continue

                        # get coordinate of clostest climate element of real soil-cell
                        sh = yllcorner + (scellsize / 2) + (srows - srow - 1) * scellsize
                        sr = xllcorner + (scellsize / 2) + scol * scellsize
                        # inter = crow/ccol encoded into integer
                        crow, ccol = climate_data_interpolator(sr, sh)
                        crow = int(crow)
                        ccol = int(ccol)

                        crop_grid_id = int(crop_grid[srow, scol])
                        # print(crop_grid_id)
                        if crop_grid_id != 1 or soil_id == -8888:
                            continue

                        tcoords = {}

                        if nuts3_regions_crs not in tcoords:
                            tcoords[nuts3_regions_crs] = soil_crs_to_x_transformers[nuts3_regions_crs].transform(sr, sh)
                        n3r, n3h = tcoords[nuts3_regions_crs]
                        nuts3_region_id = int(nuts3_regions_interpolate(n3r, n3h))
                        if not nuts3_region_id or (len(nuts3_region_ids) > 0 and nuts3_region_id not in nuts3_region_ids):
                            continue

                        if soil_id in soil_id_cache:
                            soil_profile = soil_id_cache[soil_id]
                        else:
                            soil_profile = soil_io3.soil_parameters(soil_db_con, soil_id)
                            soil_id_cache[soil_id] = soil_profile
                        if not soil_profile or len(soil_profile) == 0:
                            continue

                        #worksteps = env_template["cropRotation"][0]["worksteps"]
                        #sowing_ws = next(filter(lambda ws: ws["type"][-6:] == "Sowing", worksteps))
                        #harvest_ws = next(filter(lambda ws: ws["type"][-7:] == "Harvest", worksteps))

                        #ilr_interpolate = ilr_seed_harvest_data[crop_id_short]["interpolate"]
                        #seed_harvest_cs = ilr_interpolate(sr, sh) if ilr_interpolate else None

                        # set external seed/harvest dates
                        
                        # if seed_harvest_cs:
                        #     seed_harvest_data = ilr_seed_harvest_data[crop_id_short]["data"][seed_harvest_cs]
                        #     if seed_harvest_data:
                        #         is_winter_crop = ilr_seed_harvest_data[crop_id_short]["is-winter-crop"]
                        #
                        #         if setup[
                        #             "sowing-date"] == "fixed":  # fixed indicates that regionally fixed sowing dates will be used
                        #             sowing_date = seed_harvest_data["sowing-date"]
                        #         elif setup[
                        #             "sowing-date"] == "auto":  # auto indicates that automatic sowng dates will be used that vary between regions
                        #             sowing_date = seed_harvest_data["latest-sowing-date"]
                        #         elif setup[
                        #             "sowing-date"] == "fixed1":  # fixed1 indicates that a fixed sowing date will be used that is the same for entire germany
                        #             sowing_date = sowing_ws["date"]
                        #
                        #         sds = [int(x) for x in sowing_date.split("-")]
                        #         sd = date(2001, sds[1], sds[2])
                        #         sdoy = sd.timetuple().tm_yday
                        #
                        #         if setup[
                        #             "harvest-date"] == "fixed":  # fixed indicates that regionally fixed harvest dates will be used
                        #             harvest_date = seed_harvest_data["harvest-date"]
                        #         elif setup[
                        #             "harvest-date"] == "auto":  # auto indicates that automatic harvest dates will be used that vary between regions
                        #             harvest_date = seed_harvest_data["latest-harvest-date"]
                        #         elif setup[
                        #             "harvest-date"] == "auto1":  # fixed1 indicates that a fixed harvest date will be used that is the same for entire germany
                        #             harvest_date = harvest_ws["latest-date"]
                        #
                        #         hds = [int(x) for x in harvest_date.split("-")]
                        #         hd = date(2001, hds[1], hds[2])
                        #         hdoy = hd.timetuple().tm_yday
                        #
                        #         esds = [int(x) for x in seed_harvest_data["earliest-sowing-date"].split("-")]
                        #         esd = date(2001, esds[1], esds[2])
                        #
                        #         # sowing after harvest should probably never occur in both fixed setup!
                        #         if setup["sowing-date"] == "fixed" and setup["harvest-date"] == "fixed":
                        #             # calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        #             if is_winter_crop:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        #             else:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        #             sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        #             harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month,
                        #                                                                calc_harvest_date.day)
                        #         elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto":
                        #             if is_winter_crop:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        #             else:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        #             sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        #             harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month,
                        #                                                                       calc_harvest_date.day)
                        #         elif setup["sowing-date"] == "fixed" and setup["harvest-date"] == "auto1":
                        #             if is_winter_crop:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        #             else:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        #             sowing_ws["date"] = seed_harvest_data["sowing-date"]
                        #             harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], hds[1], hds[2])
                        #
                        #         elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "fixed":
                        #             sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(
                        #                 esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        #             calc_sowing_date = date(2000, 12, 31) + timedelta(days=max(hdoy + 1, sdoy))
                        #             sowing_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(sds[0], calc_sowing_date.month,
                        #                                                                      calc_sowing_date.day)
                        #             harvest_ws["date"] = seed_harvest_data["harvest-date"]
                        #
                        #         elif setup["sowing-date"] == "auto" and setup["harvest-date"] == "auto":
                        #             sowing_ws["earliest-date"] = seed_harvest_data["earliest-sowing-date"] if esd > date(
                        #                 esd.year, 6, 20) else "{:04d}-{:02d}-{:02d}".format(sds[0], 6, 20)
                        #             if is_winter_crop:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        #             else:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        #             sowing_ws["latest-date"] = seed_harvest_data["latest-sowing-date"]
                        #             harvest_ws["latest-date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month,
                        #                                                                       calc_harvest_date.day)
                        #         elif setup["sowing-date"] == "fixed1" and setup["harvest-date"] == "fixed":
                        #             # calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy-1))
                        #             if is_winter_crop:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=min(hdoy, sdoy - 1))
                        #             else:
                        #                 calc_harvest_date = date(2000, 12, 31) + timedelta(days=hdoy)
                        #             sowing_ws["date"] = sowing_date
                        #             # print(seed_harvest_data["sowing-date"])
                        #             harvest_ws["date"] = "{:04d}-{:02d}-{:02d}".format(hds[0], calc_harvest_date.month,
                        #                                                                calc_harvest_date.day)
                        
                        # check if current grid cell is used for agriculture
                        #if setup["landcover"]:
                        #    if landuse_crs not in tcoords:
                        #        tcoords[landuse_crs] = soil_crs_to_x_transformers[landuse_crs].transform(sr, sh)
                        #    lur, luh = tcoords[landuse_crs]
                        #    landuse_id = landuse_interpolate(lur, luh)
                        #    if landuse_id not in [2, 3, 4]:
                        #        continue

                        if dem_crs not in tcoords:
                            tcoords[dem_crs] = soil_crs_to_x_transformers[dem_crs].transform(sr, sh)
                        demr, demh = tcoords[dem_crs]
                        height_nn = dem_interpolate(demr, demh)

                        if slope_crs not in tcoords:
                            tcoords[slope_crs] = soil_crs_to_x_transformers[slope_crs].transform(sr, sh)
                        slr, slh = tcoords[slope_crs]
                        slope = slope_interpolate(slr, slh)

                        # env_template["params"]["userCropParameters"]["__enable_T_response_leaf_expansion__"] = setup[
                        #     "LeafExtensionModifier"]

                        env_template["params"]["siteParameters"]["SoilProfileParameters"] = soil_profile

                        if setup["elevation"]:
                            env_template["params"]["siteParameters"]["heightNN"] = float(height_nn)

                        if setup["slope"]:
                            env_template["params"]["siteParameters"]["slope"] = slope / 100.0

                        if setup["latitude"]:
                            clat, _ = cdict[(crow, ccol)]
                            env_template["params"]["siteParameters"]["Latitude"] = clat

                        if setup["CO2"]:
                            env_template["params"]["userEnvironmentParameters"]["AtmosphericCO2"] = float(setup["CO2"])

                        if setup["O3"]:
                            env_template["params"]["userEnvironmentParameters"]["AtmosphericO3"] = float(setup["O3"])

                        env_template["params"]["simulationParameters"]["UseNMinMineralFertilisingMethod"] = setup[ ## Muss ich das auf true setzen?##
                            "fertilization"]
                        env_template["params"]["simulationParameters"]["UseAutomaticIrrigation"] = setup["irrigation"]

                        env_template["params"]["simulationParameters"]["NitrogenResponseOn"] = setup["NitrogenResponseOn"]

                        env_template["csvViaHeaderOptions"] = sim_json["climate.csv-options"]

                        climate_csv_path = (paths["monica-path-to-climate-dir"] +
                                            f"C3S2_414_po_ebro/AgERA5_2014_to_2025/{region}/row-{crow}/col-{ccol}.csv")
                        env_template["pathToClimateCSV"] = climate_csv_path

                        env_template["customId"] = {
                            "setup_id": setup_id,
                            "srow": srow, "scol": scol,
                            "crow": int(crow), "ccol": int(ccol),
                            "soil_id": soil_id,
                            "env_id": sent_env_count,
                            "nuts3_region_id": nuts3_region_id,
                            #"is_sensitivity_analysis": is_sensitivity_analysis,
                            #"param_name": p_name,
                            #"param_value": p_value,
                            "nodata": False
                        }

                        socket.send_json(env_template)

                        #with open(path_to_out_file, "a") as _:
                        #    _.write(f"{datetime.now()} sent env: {sent_env_count}, customId: {env_template['customId']}\n")
                        #print("sent env ", sent_env_count, " customId: ", env_template["customId"])
                        sent_env_count += 1

            # print("unknown_soil_ids:", unknown_soil_ids)
            except Exception as e:
                with open(path_to_out_file, "a") as _:
                    _.write(f"{datetime.now()} raised exception: {e}\n")
                print("Exception raised:", e)
                raise e            

            if env_template:# and is_sensitivity_analysis:
                env_template["pathToClimateCSV"] = ""
                env_template["customId"] = {
                    "setup_id": setup_id,
                    "no_of_sent_envs": sent_env_count,
                    #"is_sensitivity_analysis": is_sensitivity_analysis,
                }
                socket.send_json(env_template)

            stop_setup_time = time.perf_counter()
            print("\nSetup ", sent_env_count, " envs took ", (stop_setup_time - start_setup_time), " seconds")
            sent_env_count = 0

    print("exiting run_producer()")


if __name__ == "__main__":
    run_producer()
