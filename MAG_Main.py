#%% ================================================================
"""
Imports
"""

import MAG_Util as MAG_Util
import MAG_Experiment as MAG_Experiment

#%% ================================================================
"""
Main driver for experiment
"""
if __name__ == "__main__":
    # create the experiment object
    run_experiment = MAG_Experiment.Experiment()
    # generate the filename for the results
    filename_output = "Results_GPU_" + MAG_Util.get_current_timestamp() + ".csv"
    # run the experiment, values are hardcoded in the function already
    run_experiment.run_experiment_HERE(argv_filename_results=filename_output)

# %% ================================================================
# END