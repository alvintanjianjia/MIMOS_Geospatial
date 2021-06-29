#%% ================================================================
"""
Imports
"""

# still need pandas for path reconstruction
import pandas as pandas
# import networkx as networkx
# replace with the GPU ones
import cugraph as cugraph
import cudf as cudf

import sys as sys
import random as random
import time as time
import datetime as datetime

#%% ================================================================
"""
File IO
"""

def load_file(argv_filename="", argv_type="list", argv_separator=None):
    """
    Load a file and return its content
        argv_filename is the filename for the file to be loaded
        argv_type (string) denote the datastructure that the file would be loaded into
    Return
        current_list is a list of content, each line is a list item
        current_dictionary is a dictioanry of content, with the first token as the key and subsequent tokens as a string
    """
    file_data = open(argv_filename, "r")
    # load into list line by line, after striping
    if argv_type == "list":
        current_list = []
        for current_line in file_data:
            current_line = current_line.strip()
            if current_line == "":
                continue
            current_list.append(current_line)
        file_data.close()
        return current_list 
    # load into dictionary, with first split as the 
    if argv_type == "dictionary":
        current_dictionary = {}
        for current_line in file_data:
            current_line = current_line.strip()
            if current_line == "":
                continue
            current_line = current_line.split(argv_separator)
            current_dictionary[current_line[0]] = " ".join(current_line[1:])
        file_data.close()
        return current_dictionary

def load_dataset(argv_filename="", argv_separator=",", argv_type="dictionary", argv_name=False):
    """
    Function to load dataset into a different datatypes
    This is a more complete function than the one above, but not being used as it is not needed
    """
    file_data = open(argv_filename, "r")
    if argv_type == "dictionary":
        current_dictionary = {}
        counter_line = 0
        for current_line in file_data:
            current_line = current_line.strip()
            if current_line == "":
                continue
            current_line = current_line.split(argv_separator)
            if argv_name is False:
                current_dictionary["point_"+str(counter_line)] = " ".join(current_line)
            else:
                current_dictionary[current_line[0]] = " ".join(current_line[1:])
            counter_line += 1
        file_data.close()
        return current_dictionary
    elif argv_type == "list":
        current_list = []
        for current_line in file_data:
            current_line = current_line.strip()
            if current_line == "":
                continue
            current_line = current_line.split(argv_separator)
            current_list.append(" ".join(current_line))
        file_data.close()
        return current_list
    else:
        return None

#%% ================================================================
"""
Dataframes
with pandas then converted to cudf
skip geopandas for now, as it isn't available on the server
"""

def load_into_dataframe(argv_filename="", argv_pandas_type=None, argv_pandas_geomname="geometry", argv_dropNA=True):
    """
    Load the filename into a dataframe
        argv_filename is the filename for the CSV file to be read into a Pandas DF
        argv_pandas_type is the type of Pandas we would be using such as geopandas.
            If None, then would be default panda.
        argv_pandas_geomname is the colname for the column with the GIS data when using GeoPandas
        argv_dropNA is the boolean variable to determine if missing values are dropped
            Would use the default dropNA(), future extension here could provide more flexible drops
    Would always skip blank lines by default.
    Return
        return_dataframe is the cudf DF object
    """
    if argv_pandas_type is None:
        return_dataframe = pandas.read_csv(filepath_or_buffer=argv_filename, skip_blank_lines=True)
        return_dataframe = cudf.DataFrame.from_pandas(return_dataframe)
    elif argv_pandas_type == "geopandas":
        # no need for geopandas, thus won't be included for now
        # return_dataframe = geopandas.read_file(filename=argv_filename, GEOM_POSSIBLE_NAMES=argv_pandas_geomname, skip_blank_lines=True)
        pass
    # ignore missing (complete one by default)
    if argv_dropNA:
        return_dataframe = return_dataframe.dropna()
    # return the dataframe
    return return_dataframe

def load_into_dataframes(argv_dict_filenames={}, argv_pandas_type=None, argv_pandas_geomname="geometry", argv_dropNA=True):
    """
    Load the filenames into a dictionary of dataframes
        argv_dict_filenames is a dictionary of filenamnes to be loaded into the dataframe
        argv_pandas_type is the type (string) of pandas to use (normal vs geopandas)
            if pandas, it will be cudf
        argv_pandas_geomname is the colname for the geometry column for geopandas
        argv_dropNA is a boolean variable to control if rows with missing values would be dropped
    Return
        return_dict_dfs a dictionary of dataframes loaded from the files
    """
    return_dict_dfs = {}
    for current_key in argv_dict_filenames.keys():
        current_filename = argv_dict_filenames[current_key]
        return_dict_dfs[current_key] = load_into_dataframe(
            argv_filename=current_filename,
            argv_dropNA=argv_dropNA)
    return return_dict_dfs

def concat_dataframes(argv_dfs=[], argv_duplicates=True, argv_ignore_index=True):
    """
    Concat dataframes together
        argv_dfs is a list of dataframes
        argv_duplicates check if duplicates are allowed, a boolean
        argv_ignore_index is a boolean to ignore the index
    Return
        return_df the concatenated dataframe
    """
    # we not using cudf concat here first as there is some noted bug identified in 18 July 2020
    # https://github.com/rapidsai/cudf/issues/5718
    return_df = pandas.concat(argv_dfs, ignore_index=argv_ignore_index)
    if not argv_duplicates:
        return_df = return_df.drop_duplicates(keep="first")
    return return_df

def split_dataframes_byCol(argv_df, argv_col=""):
    """
    Split a dataframe into multiple based on the unique values
        argv_df is the dataframe object which would be used for splitting
        argv_col is the column name in the dataframe of the object
    Return
        return_dfs a dictionary of Dataframes splitted based on their unique value (as the key for the dictionary as well)
    """
    return_dfs = {}
    # get unique values
    current_values_unique = argv_df[argv_col].unique()
    # loop through the unique values
    for current_value in current_values_unique:
        return_dfs[current_value] = argv_df.loc[argv_df[argv_col] == current_value]
    return return_dfs


#%% ================================================================
"""
Graph and graph layers
"""

def generate_graph(argv_df, argv_source="", argv_destination="", argv_edge=[], argv_directed=True):
    """
    Generate a graph with the edges weighted according to a list of values from a dataframe
        argv_df is the dataframe object where the graph would be generated
        argv_source is the colname as the source for all of the edges in the graph
        argv_destination is the colname as the destination for all of the edges in the graph
        argv_edge is a list of colnames for edges value (multi edges are supported)
        argv_directed is a boolean to determined if the fraph is directed.
            Graph generated is directed by default for our use case.
    Return
        return_graph is a cugraph graph object
    """
    return_graph = None
    if argv_directed:
        return_graph = cugraph.from_cudf_edgelist(
            df=argv_df,
            source=argv_source,
            destination=argv_destination,
            edge_attr=argv_edge,
            create_using=cugraph.DiGraph
        )
    #kl_selangor_graph_distance.from_cudf_edgelist(edge_df_distance, source=0, destination=1, edge_attr=2)    
    
    else:
        return_graph = cugraph.from_cudf_edgelist(
            df=argv_df,
            source=argv_source,
            destination=argv_destination,
            edge_attr=argv_edge
        )
    #print(return_graph)
    return return_graph

def generate_graph_layers_different(
    argv_df, argv_source="", argv_destination="", argv_weight="", 
    argv_intermediates_vertices=[[]], argv_intermediates_weights=[],
    argv_type="dataframe"
    ):
    """
    Generated layered graph
        argv_source/ argv_destination/ argv_weight are the edge details
        argv_intermediates_vertices/ argv_intermediates_weights are array of
            intermediate vertices [[v1,v2,v3], [v2,v5,v10,v11]]
            weights connecting the intermediate nodes [5,2]
        there should be at least 2 layers (1 intermediate link)
        argv_type is how we do the layered, either via graph or dataframe
            for now only dataframe as it is easier and more efficient
    Return
        return_df is the dataframe of the layered graph 
    """
    # for dataframe
    if argv_type == "dataframe":
        current_u = argv_df[argv_source].to_arrow().to_pylist()
        current_v = argv_df[argv_destination].to_arrow().to_pylist()
        current_w = argv_df[argv_weight].to_arrow().to_pylist()
        current_u_new = []
        current_v_new = []
        current_w_new = []
        # duplicate it for multiple layers first
        current_count_layers = len(argv_intermediates_vertices)
        # note: the +1 is needed as k intermediate layer would have k layers total
        for count_layer in range(current_count_layers+1):
            for count_row in range(len(current_u)):
                current_u_new.append(str(current_u[count_row]) + "_" + str(count_layer))
                current_v_new.append(str(current_v[count_row]) + "_" + str(count_layer))
                current_w_new.append(float(current_w[count_row]))
        # add in the intermediate laters
        for count_layer in range(current_count_layers):
            current_intermediate_vertices = argv_intermediates_vertices[count_layer]
            current_intermediate_weight = argv_intermediates_weights[count_layer]
            for current_intermeidate_vertex in current_intermediate_vertices:
                current_u_new.append(str(current_intermeidate_vertex) + "_" + str(count_layer))
                current_v_new.append(str(current_intermeidate_vertex) + "_" + str(count_layer+1))
                current_w_new.append(float(current_intermediate_weight))
        # create the df
        # early runs have this as Pandas
        # new experiment is not run with cuDF yet
        return_df = cudf.DataFrame(
            data=list(zip(current_u_new, current_v_new, current_w_new)),
            columns=[argv_source, argv_destination, argv_weight]
        )
        #print(return_df)
        return return_df
    # return nothing when the wrong type is selected
    return None

def run_dijkstra(argv_graph, argv_source, argv_target, argv_weight_edge, argv_path=True):
    """
    Run dijkstra
        argv_graph is the cugraph graph to run Dijkstra on
        argv_source and argv_target are vertex IDs
        argv_weight_edge is the variable used for the graph weight in Dijkstra
            Note graph could be multi edge, thus we would want to specific what is the weight we are being minimum
    """
    # start timer to track SSSP runtime
    run_timer()
    distances_distance_table = cugraph.shortest_path(argv_graph, argv_source)
    # stop the timer, to track SSSP runtime
    timer_dijkstra = run_timer()

    # for only returning the table
    if argv_path:
        pass
    else:
        return distances_distance_table, timer_dijkstra

    # start timer to track the path reconstruction runtime
    run_timer()
    distances_distance = distances_distance_table.to_pandas()    
    current_distance = distances_distance[distances_distance['vertex'] == argv_target]['distance'].values[0]    
    #print(distances_distance[distances_distance['vertex'] == argv_target]['predecessor'].values, 'here 2')
    #print(distances_distance[distances_distance['vertex'] == argv_target]['predecessor'].values[0], 'here 3')    
    distances_distance_path = []
    distances_distance_path.append(argv_target)    
    if current_distance < sys.float_info.max:
        #print('start')
        while argv_target != argv_source:            
            #print(distances_distance_path)
            #print(argv_target, 'argv_target')
            #print(distances_distance[distances_distance['vertex'] == argv_target]['predecessor'].values,'dun match ur lanjiao')
            argv_target = distances_distance[distances_distance['vertex'] == argv_target]['predecessor'].values[0]
            distances_distance_path.append(argv_target)
    # stop the timer for SSSP runtime
    timer_pathbuilding = run_timer()
    #print('end')
    #print(current_distance, distances_distance_path, 'current distance, current_path')
    #current_distance, current_path = cugraph.sssp(
    #    G=argv_graph,
    #    source=argv_source,
    #    target=argv_target,
    #    weight=argv_weight_edge
    #)    
    #print(current_distance, 'current_distance')
    #print(distances_distance_path)    
    return current_distance, distances_distance_path, timer_dijkstra, timer_pathbuilding

def clean_layered_path(argv_path, argv_df_path={}):
    """
    Function to pre-processed the returned path from Dijkstra, completing the path for visualization on the map
        argv_path is the path from Dijsktra through a layered graph; the vertices would have the "_level" in the ID
        argv_df_path is the dictionary where the concat(start, " ", end) as the key and the full path coordinates as the value
    Return
        return_path_full is a string of coordinates that acts as the full path of map plotting (with , as separator).
        return_detour is a list of coordinates that are the detour points, this helps us mark the detour points on the map
        return_detour_long/ return_detour_lat is the same, but just broken down for long and lat for visualization
    """
    # clean
    return_path = []
    return_detour = []
    return_detour_long = []
    return_detour_lat = []
    for current_point in argv_path:
        current_point = current_point.split("_")[0]
        return_path.append(current_point)
    # loop through
    return_path_full = []
    for i in range(len(return_path)-1):
        current_point_start = return_path[i]
        current_point_end = return_path[i+1]
        # if start and end the same (ie moving through layers)
        if current_point_start == current_point_end:
            return_detour.append(current_point_start)
            current_point_start_tokens = current_point_start.split()
            return_detour_long.append(str(current_point_start_tokens[0]))
            return_detour_lat.append(str(current_point_start_tokens[1]))
            continue
        # continue to the next one
        current_endpoint = current_point_start + " " + current_point_end
        # print(current_endpoint)
        # print(argv_df_path[current_endpoint])
        # """
        if current_endpoint in argv_df_path:
            current_path = argv_df_path[current_endpoint]
            current_path = current_path.replace("], ", "] , ")
            current_path = current_path.split(" , ")
            current_path[0] = current_path[0].replace("[[", "[")
            # remove the end point, except for the last one as they do repeat
            if i != len(return_path)-2:
                current_path.pop(-1)
            else:
                current_path[-1] = current_path[-1].replace("]]", "]")
            # current_path = current_path.split(", ")
            # print(current_path)
            return_path_full.extend(current_path)
        else:
            raise Exception("Error: No path")
        # """
    # return the values, concat the path
    return_path_full = ", ".join(return_path_full)
    return return_path_full, return_detour, return_detour_long, return_detour_lat

#%% ================================================================
"""
Random functions
Mainly used for evaluations
"""

def set_random_seed(argv_seed=1234):
    """
    Simple function to reset the seeds
        argv_seed is the seed value
    """
    random.seed(argv_seed)

def select_random(argv_values, argv_count=10):
    """
    Randomly select from a list of values
    Note that repetitions are not allowed
        argv_values is a list of values to be selected
        argv_count is the total number of items to be selected
        precondition that argv_count <= argv_values
    Return
        return_selected is a list of randomly selected value
    """
    return_selected = []
    while len(return_selected) < argv_count:
        current_value = random.choice(argv_values)
        if current_value in return_selected:
            continue
        return_selected.append(current_value)
    return return_selected

def generate_random_lists(argv_values, argv_counts=[], argv_unique=False):
    """
    From the provided list, generate multiple lists from the given sizes
        argv_values is a list of values to be selected
        argv_counts is a list of sizes of the lists to be generated
        argv_unique is a boolean variable to ensure that the items in the randomly generated lists are all unique
    Return
        return_lists is a list of random lists generated based on the criteria
    """
    return_lists = []
    current_values = argv_values.copy()
    # build the list for each size
    for current_count in argv_counts:
        # select random item based on size
        current_list = select_random(argv_values=current_values, argv_count=current_count)
        if argv_unique:
            # then remove from original list
            for current_item in current_list:
                current_values.remove(current_item)
        return_lists.append(current_list)
    # return it
    return return_lists

#%% ================================================================
"""
Timer functions
"""

# have the start time as the global so we can make comparison
START_TIME = time.time()

def run_timer():
    """
    Function that return the different in time between now and the previous time
    Return
        return_time is the time difference between now and the previous time
    """
    global START_TIME
    return_time = time.time() - START_TIME
    START_TIME = time.time()
    return return_time

def get_current_timestamp():
    """
    Gets the current time and format into a timestamp format
    This is useful for auto-naming filename
    Return
        return_timestamp is the current timestamp formatted in YYYYMMDD_HHmm format
    """
    current_timestamp = datetime.datetime.now()
    return_timestamp = str(current_timestamp.year) + "{:02d}".format(current_timestamp.month) + "{:02d}".format(current_timestamp.day) + "_" + "{:02d}".format(current_timestamp.hour) + "{:02d}".format(current_timestamp.minute)
    # print(return_timestamp)
    return return_timestamp