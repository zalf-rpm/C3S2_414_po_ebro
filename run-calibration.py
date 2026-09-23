from datetime import datetime
import capnp
from collections import defaultdict
import json
import csv
import matplotlib.pyplot as plt
import monica_run_lib
import numpy as np
import os
from pathlib import Path
import spotpy
import subprocess as sp
import sys
import time
import uuid

import calibration_spotpy_setup_MONICA
from calibration_utils import update_parameter_values


PATH_TO_REPO = Path(os.path.realpath(__file__)).parent
PATH_TO_MAS_INFRASTRUCTURE_REPO = PATH_TO_REPO / "../mas-infrastructure"
PATH_TO_PYTHON_CODE = PATH_TO_MAS_INFRASTRUCTURE_REPO / "src/python"
if str(PATH_TO_PYTHON_CODE) not in sys.path:
    sys.path.insert(1, str(PATH_TO_PYTHON_CODE))

from pkgs.common import common

PATH_TO_CAPNP_SCHEMAS = (PATH_TO_MAS_INFRASTRUCTURE_REPO / "capnproto_schemas").resolve()
abs_imports = [str(PATH_TO_CAPNP_SCHEMAS)]
fbp_capnp = capnp.load(str(PATH_TO_CAPNP_SCHEMAS / "fbp.capnp"), imports=abs_imports)


def get_reader_writer_srs_from_channel(path_to_channel_binary, chan_name=None):
    chan = sp.Popen([
        path_to_channel_binary,
        "--name=chan_{}".format(chan_name if chan_name else str(uuid.uuid4())),
        "--output_srs",
    ], stdout=sp.PIPE, text=True)
    reader_sr = None
    writer_sr = None
    while True:
        s = chan.stdout.readline().split("=", maxsplit=1)
        id, sr = s if len(s) == 2 else (None, None)
        if id and id == "readerSR":
            reader_sr = sr.strip()
        elif id and id == "writerSR":
            writer_sr = sr.strip()
        if reader_sr and writer_sr:
            break
    return {"chan": chan, "reader_sr": reader_sr, "writer_sr": writer_sr}


local_run = False

def get_config(server=None, prod_port=None, cons_port=None):
    config = {
        "mode": "mbm-local-remote",
        "prod-port": prod_port if prod_port else "6666",  # local: 6667, remote 6666
        "cons-port": cons_port if cons_port else "7777",  # local: 6667, remote 6666
        "server": server if server else "login01.cluster.zalf.de",
        "sim.json": "sim.json",
        "crop.json": "crop.json",
        "site.json": "site.json",
        "setups-file": "sim_setups_calibration.csv",
        "path_to_out": "out/",
        "run-setups": "[1]", ## Define on rundeck ##
        "path_to_channel": "/home/berg/GitHub/mas-infrastructure/src/cpp/common/_cmake_debug/channel" if local_run else
        "/home/rpm/start_manual_test_services/GitHub/mas-infrastructure/src/cpp/common/_cmake_release/channel",
        "path_to_python": "python" if local_run else "/home/rpm/.conda/envs/clim4cast/bin/python",
        "repetitions": "5", ## Define on rundeck ##
        "all_nuts3_regions_one_by_one": False, ## Define on rundeck, from cz (not use) ##
        "only_nuts3_region_ids": "[]",  ## Define on rundeck, from cz (not use) ##
    }

    common.update_config(config, sys.argv, print_config=True, allow_new_keys=False)

    return config
    
def run_calibration(config, setup_id):
    # Read current setup
    setups = monica_run_lib.read_sim_setups(config["setups-file"])

    if setup_id not in setups:
        raise ValueError(
            f"Setup {setup_id} not found in "
            f"{config['setups-file']}"
        )

    setup = setups[setup_id]

    region = setup["region"]
    crop_id = setup["crop-id"]
    crop_code = crop_id.split("_")[0]

    parameter_setting_file = (setup["parameter-setting-file"])

    # This is what will be passed to producer
    current_run_setup = json.dumps([setup_id])
    
    path_to_out_folder = config['path_to_out']
    if not os.path.exists(path_to_out_folder):
        try:
            os.makedirs(path_to_out_folder)
        except OSError:
            print("run-calibration.py: Couldn't create dir:", path_to_out_folder, "!")
    path_to_out_file = f"{path_to_out_folder}/setup{setup_id}_run-calibration.out"
    with open(path_to_out_file, "a") as _:
        _.write(f"{datetime.now()} starting setup {setup_id}, config: {config}\n")

    procs = []

    prod_chan_data = get_reader_writer_srs_from_channel(config["path_to_channel"], f"prod_chan_setup{setup_id}")
    procs.append(prod_chan_data["chan"])
    cons_chan_data = get_reader_writer_srs_from_channel(config["path_to_channel"], f"cons_chan_setup{setup_id}")
    procs.append(cons_chan_data["chan"])

    procs.append(sp.Popen([
        config["path_to_python"],
        "run-producer_calibration.py",
        "mode=mbm-local-remote" if local_run else "mode=hpc-local-remote",
        f"server={config['server']}",
        f"server-port={config['prod-port']}",
        f"setups-file={config['setups-file']}",
        f"run-setups={current_run_setup}",
        f"reader_sr={prod_chan_data['reader_sr']}",
        f"path_to_out={config['path_to_out']}",
    ]))

    procs.append(sp.Popen([
        config["path_to_python"],
        "run-consumer_calibration.py",
        # "mode=remoteConsumer-remoteMonica",
        f"server={config['server']}",
        f"port={config['cons-port']}",
        f"run-setups={current_run_setup}",
        f"writer_sr={cons_chan_data['writer_sr']}",
        f"path_to_out={config['path_to_out']}",
    ]))

    crop_code = crop_id.split("_")[0]
    parameter_setting_file = setup["parameter-setting-file"]

    # Read observations
    obs_to_sim = {
        "yield": "Yield",
        "SOSD": "StemElongationDOY",
        "MAXD": "AnthesisDOY",
        "EOSD": "MaturityDOY"
    }

    def read_observations(path, variable, scale=1.0):
        observations = []
        region_id_to_name = {}
        with open(path) as file:
            dialect = csv.Sniffer().sniff(file.read(), delimiters=';,\t')
            file.seek(0)
            reader = csv.reader(file, dialect)

            #get year from header
            header = next(reader, None) 
            years = [int(y) for y in header[1:-1]]

            for row in reader:
                id = int(row[-1])
                name = row[0].strip()
                region_id_to_name[id] = name
                for i, year in enumerate(years, start=1):
                    text = row[i].strip()
                    value = (np.nan if not text or text.upper() == "NA" else float(text))
                    if not np.isnan(value) and value < 0:
                        value = np.nan
                    observations.append({"id": id,
                                         "year": year,
                                         "variable": variable,
                                         "sim_variable": obs_to_sim[variable],
                                         "value": value * scale})
        return observations, region_id_to_name

    observations = []
    nuts3_region_id_to_name = {}

    calibration_target = setup["calibration-target"]
    obs_vars = calibration_target.split("|")
    for obs_var in obs_vars:
        path = f"data/{region}/{region}_{crop_code}_{obs_var}.csv"
        scale = 1000.0 if obs_var == "yield" else 1.0
        obs, names = read_observations(path, obs_var, scale)
        observations.extend(obs)
        nuts3_region_id_to_name.update(names)

    observations.sort(
        key=lambda r: [r["id"], r["year"], r["variable"]]
    )

    # read parameters which are to be calibrated
    params = []
    with open(parameter_setting_file) as params_csv: # Define per crop #
        dialect = csv.Sniffer().sniff(params_csv.read(), delimiters=';,\t')
        params_csv.seek(0)
        reader = csv.reader(params_csv, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            p = {"name": row[0]}
            if len(row[1]) > 0:
                p["array"] = int(row[1])
            for n, i in [("low", 2), ("high", 3), ("step", 4), ("optguess", 5), ("minbound", 6), ("maxbound", 7)]:
                if len(row[i]) > 0:
                    p[n] = float(row[i])
            if len(row) == 9 and len(row[8]) > 0:
                p["derive_function"] = lambda _, _2: eval(row[8])
            params.append(p)

    # read weights
    weights = {}
    with open(f"weights/Weights_{crop_code}_{region}.csv") as weights_csv: # Define per crop #
        dialect = csv.Sniffer().sniff(weights_csv.read(), delimiters=';,\t')
        weights_csv.seek(0)
        reader = csv.reader(weights_csv, dialect)
        next(reader, None)  # skip the header
        for row in reader:
            weights[int(row[2])] = float(row[4])
    #print("weights:", weights)

    con_man = common.ConnectionManager()
    cons_reader = con_man.try_connect(cons_chan_data["reader_sr"], cast_as=fbp_capnp.Channel.Reader, retry_secs=1)
    prod_writer = con_man.try_connect(prod_chan_data["writer_sr"], cast_as=fbp_capnp.Channel.Writer, retry_secs=1)

    # configure MONICA setup for spotpy
    only_nuts3_region_ids = json.loads(config["only_nuts3_region_ids"])

    to_be_run_only_nuts3_region_ids = []
    if config["all_nuts3_regions_one_by_one"]:
        if len(only_nuts3_region_ids) > 0:
            to_be_run_only_nuts3_region_ids = list([id_] for id_ in sorted(only_nuts3_region_ids))
        else:
            to_be_run_only_nuts3_region_ids = list([id_] for id_ in sorted(nuts3_region_id_to_name.keys()))
    else:
        to_be_run_only_nuts3_region_ids = [only_nuts3_region_ids]

    spot_setup = None
    for current_only_nuts3_region_ids in to_be_run_only_nuts3_region_ids:
        # start timer 
        start_time = time.time()

        run_name = f"setup{setup_id}"
        if current_only_nuts3_region_ids:
            nuts3_label = "-".join(map(str, current_only_nuts3_region_ids))
            run_name += f"_{nuts3_label}"

        filtered_observations = observations
        if len(current_only_nuts3_region_ids) > 0:
            filtered_observations = list(filter(lambda d: d["id"] in current_only_nuts3_region_ids, observations))
            if len(filtered_observations) == 0:
                continue
        if spot_setup:
            del spot_setup
        #print("selected weight for region:", weights[current_only_nuts3_region_ids[0]], flush=True)
        #spot_setup = calibration_spotpy_setup_MONICA.spot_setup(params, filtered_observations, prod_writer, cons_reader,
                                                                #path_to_out_folder, current_only_nuts3_region_ids)

        # Assign each observation its region-specific weight. An empty region filter selects all regions.
        missing_weight_ids = sorted({observation["id"] for observation in filtered_observations if observation ["id"]
                                     not in weights})
        if missing_weight_ids:
            raise ValueError(f"Missing weights for NUTS3 regions: {missing_weight_ids}")

        weights_per_observation = np.array([weights[observation["id"]] for observation in filtered_observations])

        spot_setup_out_file = f"{run_name}_spot_setup.out"
        spot_setup = calibration_spotpy_setup_MONICA.spot_setup(params, filtered_observations, prod_writer, cons_reader,
                                                                path_to_out_folder, spot_setup_out_file,current_only_nuts3_region_ids,
                                                                weights_per_observation)

        # spot_setup = calibration_spotpy_setup_MONICA.spot_setup(params, filtered_observations, prod_writer, cons_reader,
        #                                                 path_to_out_folder, current_only_nuts3_region_ids, weights[current_only_nuts3_region_ids[0]])

        rep = int(config["repetitions"]) #initial number was 10
        results = []
        #Set up the sampler with the model above
        sampler = spotpy.algorithms.sceua(spot_setup, dbname=f"{path_to_out_folder}/{run_name}_SCEUA_monica_results", dbformat="csv")
        # sampler = spotpy.algorithms.dream(spot_setup, dbname=f"{path_to_out_folder}/{run_name}_DREAM_monica_results", dbformat="csv")
        #Run the sampler to produce the paranmeter distribution
        #and identify optimal parameters based on objective function
        #ngs = number of complexes
        #kstop = max number of evolution loops before convergence
        #peps = convergence criterion
        #pcento = percent change allowed in kstop loops before convergence
        with open(f"{path_to_out_folder}/{spot_setup_out_file}", "a") as _:
            _.write(f"{datetime.now()} setup{setup_id} sampler starts run-cal\n")

        sampler.sample(rep, ngs=len(params)*2+1, kstop = 100 , peps=0.0001, pcento=0.0001)


        # sampler.sample(rep, nChains = 20, nCr = 3, eps = (10e-6), convergence_limit=1.0)

        with open(f"{path_to_out_folder}/{spot_setup_out_file}", "a") as _:
            _.write(f"{datetime.now()} sampler ends run-cal\n")
        # end timer
        end_time = time.time()
        time_taken = end_time - start_time
        if time_taken > 10:
            with open(f"{path_to_out_folder}/{spot_setup_out_file}", "a") as _:
                _.write(f"{datetime.now()} Time taken to calibrate: {time_taken:.2f} seconds\n")
            #print(f"Time taken to calibrate: {time_taken:.2f} seconds")

        # Print final results
        def print_status_final(status, stream):
            # 1. Result
            print("*** Final SPOTPY summary ***", file=stream)
            print(
                "Total Duration: "
                + str(round((time.time() - status.starttime), 2))
                + " seconds"
            , file=stream)
            print("Total Repetitions:", status.rep, file=stream)

            if status.optimization_direction == "minimize":
                print("Minimal objective value: %g" % (status.objectivefunction_min), file=stream)
                print("Corresponding parameter setting:", file=stream)
                for i in range(status.parameters):
                    text = "%s: %g" % (status.parnames[i], status.params_min[i])
                    print(text, file=stream)

            if status.optimization_direction == "maximize":
                print("Maximal objective value: %g" % (status.objectivefunction_max), file=stream)
                print("Corresponding parameter setting:", file=stream)
                for i in range(status.parameters):
                    text = "%s: %g" % (status.parnames[i], status.params_max[i])
                    print(text, file=stream)

            if status.optimization_direction == "grid":
                print("Minimal objective value: %g" % (status.objectivefunction_min), file=stream)
                print("Corresponding parameter setting:", file=stream)
                for i in range(status.parameters):
                    text = "%s: %g" % (status.parnames[i], status.params_min[i])
                    print(text, file=stream)

                print("Maximal objective value: %g" % (status.objectivefunction_max), file=stream)
                print("Corresponding parameter setting:", file=stream)
                for i in range(status.parameters):
                    text = "%s: %g" % (status.parnames[i], status.params_max[i])
                    print(text, file=stream)

            # 2. Arguments
            print("\n*** Run arguments ***", file=stream)

            for arg in sys.argv[1:]:
                if "=" in arg:
                    key = arg.split("=", 1)[0]
                    print(f"{key}: {config[key]}", file=stream)
            print(f"current-setup: {setup_id}", file=stream)

            # 3. Calibration setup
            print("\n*** Calibration setup ***", file=stream)

            for key, value in setup.items():
                print(f"{key}: {value}", file=stream)
            #print("******************************\n", file=stream)

        path_to_best_out_file = f"{path_to_out_folder}/{run_name}_best.out"
        with open(path_to_best_out_file, "w") as _:
            print_status_final(sampler.status, _)

        # Write calibrated parameter file
        def save_calibrated_cultivar(setup, config, crop_code, best_params, path_to_out_folder, run_name, optimization):
            # read sim.json
            with open(setup.get("sim.json", config["sim.json"])) as _:
                sim_json = json.load(_)

            include_base_path = Path(sim_json["include-file-base-path"])

            if not include_base_path.is_absolute():
                include_base_path = PATH_TO_REPO / include_base_path

            # read crop.json
            with open(setup.get("crop.json", config["crop.json"])) as _:
                crop_json = json.load(_)

            crop_params = crop_json["crops"][crop_code]["cropParams"]
            species_file = include_base_path / crop_params["species"][1]
            cultivar_file = include_base_path / crop_params["cultivar"][1]

            # read original parameter file
            with open(species_file) as _:
                species = json.load(_)

            with open(cultivar_file) as _:  
                cultivar = json.load(_)

            ps = {"species": species, "cultivar": cultivar}

            # update parameter values
            ps = update_parameter_values(ps, best_params)

            # Write species, cultivar file
            calibrated_species_files = f"{path_to_out_folder}/{run_name}_calibrated_species_{optimization}.json"
            with open(calibrated_species_files, "w") as _:
                json.dump(ps["species"], _, indent=2)

            calibrated_cultivar_files = f"{path_to_out_folder}/{run_name}_calibrated_cultivar_{optimization}.json"
            with open(calibrated_cultivar_files, "w") as _:
                json.dump(ps["cultivar"], _, indent=2)

        if sampler.status.optimization_direction == "minimize" or sampler.status.optimization_direction == "grid":
            best_params = {
                name: float(value)
                for name, value in zip(sampler.status.parnames, sampler.status.params_min)
            }
            save_calibrated_cultivar(setup, config, crop_code, best_params, path_to_out_folder, run_name, "min")

        if sampler.status.optimization_direction == "maximize" or sampler.status.optimization_direction == "grid":
            best_params = {
                name: float(value)
                for name, value in zip(sampler.status.parnames, sampler.status.params_max)
            }
            save_calibrated_cultivar(setup, config, crop_code, best_params, path_to_out_folder, run_name, "max")


        #Extract the parameter samples from distribution
        results = spotpy.analyser.load_csv_results(f"{path_to_out_folder}/{run_name}_SCEUA_monica_results")

        # Plot how the objective function was minimized during sampling
        #font = {"family": "calibri",
        #        "weight": "normal",
        #        "size": 18}
        fig = plt.figure(1, figsize=(9, 6))
        #plt.plot(results["like1"],  marker='o')
        plt.plot(results["like1"], "r+")
        plt.show()
        plt.ylabel("W_RMSE")
        plt.xlabel("Iteration")
        fig.savefig(f"{path_to_out_folder}/{run_name}_SCEUA_objectivefunctiontrace_MONICA.png", dpi=150)
        plt.close(fig)

        # OW addition
        #df = pd.read_csv (f"{path_to_out_folder}/{run_name}_SCEUA_monica_results.csv")
        #columns_of_interest = ['like1','parSpecificLeafArea', 'parMaxAssimilationRate', 'parDaylengthRequirement', 'parBaseDaylength', 'parCropSpecificMaxRootingDepth']
        #df_selected = df[columns_of_interest]
        #lowest_like1_values = df_selected.nsmallest(100, 'like1')['like1']
        #df_lowest_like1 = df_selected[df_selected['like1'].isin(lowest_like1_values)]

        # Drop the 'like1' column from the DataFrame as it's no longer needed for plotting
        #df_lowest_like1 = df_lowest_like1.drop(columns=['like1'])

        #Drop any non-numeric columns (like 'chain') before creating the pair plot
        #df_lowest_like1_numeric = df_lowest_like1.select_dtypes(include='number')
        #fig1 = sns.pairplot(df_lowest_like1_numeric)
        #fig1.savefig(f"{path_to_out_folder}/{run_name}_SCEUA_pair_MONICA.png", dpi=150)
        #plt.close(fig1.fig)



        # Plot the percentage differences
        #fig = plt.figure(1, figsize=(9, 6))
        #plt.plot(results["like1"], "r+")
        #plt.ylabel("Percentage Difference (%)")
        #plt.xlabel("Iteration")
        #plt.show()

        # Save the plot
        #fig.savefig(f"{path_to_out_folder}/{run_name}_SCEUA_percentage_difference_MONICA.png", dpi=150)
        #plt.close(fig)


        del results
    # kill the two channels and the producer and consumer
    for proc in procs:
        proc.terminate()

    # Wait for all process to terminate cleanly, and force-kill any that remain after 10 seconds before starting the next setup
    for proc in procs:
        try:
            proc.wait(timeout=10)
        except sp.TimeoutExpired:
            proc.kill()
            proc.wait()
    
    print("sampler_MONICA.py finished")

if __name__ == "__main__":
    config = get_config()
    run_setups = json.loads(config["run-setups"])

    for setup_id in run_setups:
        run_calibration(config, setup_id)


