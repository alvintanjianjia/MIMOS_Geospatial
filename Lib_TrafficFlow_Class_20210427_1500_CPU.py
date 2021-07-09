#%% ================================================================
"""
Import the libraries
"""

# simple python libraries
import os
import re
import operator
import time
# random
import random
# statistics
import statistics

# pandas for dataframes
import pandas as pandas
# shapes
import shapely as shapely
# graph
import networkx as networkx

# for map drawing
import folium as folium

#%% ================================================================
"""
Control variables
"""
FLAG_printDetail = False

#%% ================================================================
"""
Dataframe manager class
Manages the dataframes, allowing us to easily load dataframes for resuability
"""

class Manager_Dataframe:

    def __init__(self, argv_filename=None):
        """
        The init function store the name of the files to be loaded into dataframes
            argv_filename is the filename of CSV data to be read into dataframes
                if single filename (string) then it is a file that contain all filenames
                if multiple filenames (dictionary) then it is already a dictionary of filenames
        """
        # the attributes
        self._filenames = {}
        self._dataframes = {}
        # check filenames are provided or not
        if argv_filename is None:
            pass
        # if single filename, it is the file that contains all of the filenames
        elif isinstance(argv_filename, str):
            self._filenames = load_file(
                argv_filename=argv_filename,
                argv_type="dictionary",
                argv_separator=","
                )
        # if it is a dictionary of filenames
        elif isinstance(argv_filename, dict):
            for current_key in argv_filename:
                self._filenames[current_key] = argv_filename[current_key]

    def load_dataframes(self, argv_dropNA=False):
        """
        Load dataframe from the filenames, into as an attribute of the manager class
            argv_dropNA is the flag to determine if missing values would be dropped wjhen the dataframe is read
        """
        # check if there is data to load
        if len(self._filenames) == 0:
            return False
        # load in the data
        self._dataframes = {}
        for current_fileID in self._filenames:
            current_filename = self._filenames[current_fileID]
            current_dataframe = load_into_dataframe(
                argv_filename=current_filename,
                argv_dropNA=argv_dropNA
                )
            self._dataframes[current_fileID] = current_dataframe

    def get_dataframe(self, argv_id=None):
        """
        Return the dataframe of choice
            argv_id is the dataframe ID to retrieve
                if left as None, retrieve all of the dataframes (dictionary)
        """
        if argv_id is None:
            return self._dataframes
        if argv_id in self._dataframes:
            return self._dataframes[argv_id]
        return None

    def print_filenames(self, argv_id=None):
        """
        Print the filenames
            argv_id is the dataframe ID to retrieve the filename which generate the dataframe
                if left as None, print out all of the dataframe filenames
        """
        if argv_id is None:
            for current_id in self._filenames:
                current_filename = self._filenames[current_id]
                print(str(current_id) + ": " + str(current_filename))
        elif argv_id in self._filenames[argv_id]:
            current_filename = self._filenames[argv_id]
            print(str(argv_id) + ": " + str(current_filename))

    def print_dataframes(self, argv_id=None):
        """
        Print the dataframe
            argv_id is the dataframe ID to print out the details (summary information)
                if left as None, would print out all of the dataframes information
        """
        if argv_id is None:
            for current_id in self._dataframes:
                print(current_id)
                current_dataframe = self._dataframes[current_id]
                print(current_dataframe.info())
                print(current_dataframe.head())
        elif argv_id in self._dataframes:
            print(argv_id)
            current_dataframe = self._dataframes[argv_id]
            print(current_dataframe.info())
            print(current_dataframe.head())

    def split_dataframe(self, argv_id=None, argv_col="", argv_inplace=False):
        """
        Split the single dataframe into multiple dataframes based on unique values in the column
            argv_id is the dataframe ID to be splitted
                if left as None, the last dataframe would be splitted (just being lazy for single dataframes)
            argv_col is the column name where the datagrame would be splitted accordingly
                split using the helper function split_dataframes_byCol()
                will split based on unique value in the column
            argv_inplace is the boolean value towards replacing all of the dataframes with the splitted one
                default value is False
        Returns True if process is successul
        """
        # get the dataframe based on the ID
        current_dataframe = None
        if argv_id is None:
            for current_id in self._dataframes:
                current_dataframe = self._dataframes[current_id]
        elif argv_id in self._dataframes:
            current_dataframe = self._dataframes[argv_id]
        else:
            return False
        # split the dataframe        
        current_dfs = split_dataframes_byCol(argv_df=current_dataframe, argv_col=argv_col)
        # check if it is in-place
        if argv_inplace:
            self._dataframes = current_dfs
        else:
            for current_id in current_dfs:
                self._dataframes[current_id] = current_dfs[current_id]
        # done
        return True

#%% ================================================================
"""
Graph class for layered graph
"""

class Graph_Layered:
    
    def __init__(self, argv_dataframe=None, argv_source="", argv_destination="", argv_weight=[]):
        """
        Scope this down to only simple graph with single weight
            argv_dataframe is the dataframe to be used for the layered graph
            argv_source is the colname that would be used as the source of the edges for the graph
            argv_destination is the colname that would be used as the destination of the edges for the graph
            argv_weight is a list of colnames to be used as the weights for the edges of the graph
                This is written to be more generic but in general we would be using single weighted edge
        """
        # the attributes
        self._dataframe = argv_dataframe
        self._source = argv_source
        self._destination = argv_destination
        self._weight = argv_weight
        self._graph = None
        self._intermediates_vertices = [[]]
        self._intermediates_weights = []

    def get_stat_weights(self, argv_weight=None):
        """
        Return the statistics for the edge weight
        This is useful for the experiment variants of different weight values
            argv_weight is the colname for the weight which the statistics is requested for
        Return the statistics in a dictionary with keys
            min
            max
            mean
            median
            zero
        """
        # if no weight variable is selected then just use the first weight value
        if argv_weight is None:
            # get the stats
            current_values = self._dataframe[self._weight[0]].tolist()
            return_min = min(current_values)
            return_max = max(current_values)
            return_mean = sum(current_values) / max(len(current_values),1)
            return_median = statistics.median(current_values)
            # put the results in a dictionary
            return_stats = {}
            return_stats["min"] = return_min
            return_stats["max"] = return_max
            return_stats["mean"] = return_mean
            return_stats["median"] = return_median
            return_stats["zero"] = 0
        else:
            # get the stats
            current_values = self._dataframe[argv_weight].tolist()
            return_min = min(current_values)
            return_max = max(current_values)
            return_mean = sum(current_values) / max(len(current_values),1)
            return_median = statistics.median(current_values)
            # put the results in a dictionary
            return_stats = {}
            return_stats["min"] = return_min
            return_stats["max"] = return_max
            return_stats["mean"] = return_mean
            return_stats["median"] = return_median
            return_stats["zero"] = 0
        return return_stats

    def generate_graph(self, argv_directed=True):
        """
        Generate a graph from the given dataframe, storing it within the graph object
            argv_directed is the boolean to determine if the graph is directed or not
        """
        self._graph = generate_graph(
            argv_df=self._dataframe,
            argv_source=self._source, argv_destination=self._destination, argv_edge=self._weight, 
            argv_directed=argv_directed)

    def generate_layer_graph(self, argv_intermediates_vertices=[[]], argv_intermediates_weights=[]):
        """
        Generate a layered graph from the dataframe, using the information stored within the attributes
        The layered graph is generated through a dataframe approach (pandas) before being fed directly into networkx for the graph
        Note graph is always directed for the layered graph approach
            argv_intermediates_vertices is the list of vertices (vertex ID) to be used as intermediate vertices
            argv_intermediates_weights is the list of weights (numeric) to be used for the edges connecting these intermediate vertices
        """
        # update the attributes first
        self._intermediates_vertices = argv_intermediates_vertices
        self._intermediates_weights = argv_intermediates_weights
        # generate the layers, store within dataframe
        current_df_layered = generate_graph_layers_different(
            argv_df=self._dataframe,
            argv_source=self._source, argv_destination=self._destination, argv_weight=self._weight[0], 
            argv_intermediates_vertices=argv_intermediates_vertices, argv_intermediates_weights=argv_intermediates_weights,
            argv_type="dataframe"
            )
        # generate the graph
        self._graph = generate_graph(
            argv_df=current_df_layered,
            argv_source=self._source, argv_destination=self._destination, argv_edge=self._weight, 
            argv_directed=True)

    def print_graph_details(self):
        """
        Simple function to print graph information, useful for future experiments when we update it to return values
        """
        print("Nodes: " + str(self._graph.number_of_nodes()))
        print("Edges: " + str(self._graph.number_of_edges()))
        try:
            print("Density: " + str(networkx.density(self._graph)))
        except:
            pass

    def run_dijsktra(self, argv_start="", argv_end=""):
        """
        Function that runs Dijkstra from the start to the end point, if route exist
        Uses the run_dijkstra() helper function
        Arguments
            argv_start is the vertexID (string) for the starting point of path
            argv_end is the vertexID (string) for the ending point of the path
        Returns
            return_distance return the total distance from the start to the end
            return_path_nodes is the list of nodes (in the graph); not the actual route for the map because there is a detailed route between vertexA and vertexB
        Throws exception if there is no route
        """
        return_distance, return_path_nodes, timer_dijkstra, timer_pathbuilding = run_dijkstra(
            argv_graph=self._graph,
            argv_source=str(argv_start) + "_0",
            argv_target=str(argv_end) + "_" + str(len(self._intermediates_vertices)),
            argv_weight_edge=self._weight[0]
        )
        return return_distance, return_path_nodes, timer_dijkstra, timer_pathbuilding

    def build_dict_routes(self, argv_start="", argv_end="", argv_geom=""):
        """
        Function that would build a dictionary of routes (the geom) from the dataframe
        This dictionary would be useful in reconstructing the entire complete route from the path of points; in order to visualize it on the map.
            argv_start is the colname of the source vertex ID in the graph
            argv_end is the colname of the destination vertex ID in the graph
            argv_geom is th colname where the geometry of the path from the source to the destination vertex
        Return
            dict_routes is the dictionary of routes with key = concat(start, " ", end) and value is the list of path
        """
        # get the values from the df
        current_geometries = self._dataframe[argv_geom].tolist()
        current_starts = self._dataframe[argv_start].tolist()
        current_ends = self._dataframe[argv_end].tolist()
        # add them into dictionary if unique
        dict_routes = {}
        for i in range(len(current_geometries)):
            current_geometry = current_geometries[i]
            current_start = current_starts[i]
            current_end = current_ends[i]
            # use this as 
            current_point = " ".join([current_start, current_end])
            # add to dictionary
            if current_point not in dict_routes:
                dict_routes[current_point] = current_geometry
        # return the dictionary for use
        return dict_routes



#%% ================================================================
"""
File IO
"""

#%% Set CWD
def set_CWD():
    """
    Change to CWD to where the file is located
    This is generally not required except for certain Python setups
    """
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    if FLAG_printDetail:
        print(os.getcwd())

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
with pandas
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
        return_dataframe is the pandas DF object
    """
    if argv_pandas_type is None:
        return_dataframe = pandas.read_csv(filepath_or_buffer=argv_filename, skip_blank_lines=True)
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
        return_graph is a NetworkX graph object
    """
    return_graph = None
    if argv_directed:
        return_graph = networkx.from_pandas_edgelist(
            df=argv_df,
            source=argv_source,
            target=argv_destination,
            edge_attr=argv_edge,
            create_using=networkx.DiGraph()
        )
    else:
        return_graph = networkx.from_pandas_edgelist(
            df=argv_df,
            source=argv_source,
            target=argv_destination,
            edge_attr=argv_edge
        )
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
        current_u = argv_df[argv_source].tolist()
        current_v = argv_df[argv_destination].tolist()
        current_w = argv_df[argv_weight].tolist()
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
        return_df = pandas.DataFrame(
            data=list(zip(current_u_new, current_v_new, current_w_new)),
            columns=[argv_source, argv_destination, argv_weight]
        )
        return return_df
    # return nothing when the wrong type is selected
    return None

def run_dijkstra(argv_graph, argv_source, argv_target, argv_weight_edge):
    """
    Run dijkstra
        argv_graph is the networkx graph to run Dijkstra on
        argv_source and argv_target are vertex IDs
        argv_weight_edge is the variable used for the graph weight in Dijkstra
            Note graph could be multi edge, thus we would want to specific what is the weight we are being minimum
    """
    #sssp_paths = networkx.single_source_shortest_path(
    #   G=argv_graph,
    #   source=argv_source
    #)
    
    run_timer()
    sssp_paths = networkx.dijkstra_predecessor_and_distance(
        G=argv_graph,
        source=argv_source
    )
    timer_dijkstra = run_timer()
    
    run_timer()
    
    current_path = []
    current_path.append(argv_target)
    current_distance = sssp_paths[1][argv_target]
    while argv_target != argv_source:
        #print(argv_target, 'argv_target in loop')
        argv_target = sssp_paths[0][argv_target][0] 
        current_path.append(argv_target)
            
    timer_pathbuilding = run_timer()
    #print(current_distance, current_path, 'current distance + path')
    
    #current_distance, current_path = networkx.single_source_dijkstra(
    #    G=argv_graph,
    #    source=argv_source,
    #    target=argv_target,
    #    weight=argv_weight_edge
    #)
    
    current_path.reverse()
    
    return current_distance, current_path, timer_dijkstra, timer_pathbuilding 

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

# import timer for benchmarking
import time
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

import datetime
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


#%% ================================================================
"""
Class for the experiment
"""

class Experiment:
    """
    Special class created to run the experiments
    """

    def __init__(
        self,
        argv_dict_dataframes={}, argv_df_intermediate=None,
        argv_source="", argv_destination="", argv_weight="", argv_path=""
        ):
        """
        Setup the dictionaries and variables
            argv_dict_dataframes is the dictionary of dataframes to be used for the experiments
            argv_df_intermediate is the dataframe with a column for points to be used as intermediate vertices i nthe layerd graph
            argv_source is the colname for the source of edges
            argv_destination is the colname for the destination of edges
            argv_weight is the list of colnames for columns to be used 
            argv_path is the variable to be used to generate the complete route/ path from the vertices
        """
        # the dataset
        self._dict_dataframes = argv_dict_dataframes
        self._df_intermediate = argv_df_intermediate
        # the graph details
        self._source = ""
        self._destination = ""
        self._weight = [""]
        # got the pathing
        self._path = ""

    def load_dataframes(
        self,
        argv_filename_dataframes="",
        argv_id=None,
        argv_colname_split=""
        ):
        """
        Load the dataframe for the experiment into the attribute, it is a dictionary of dataframes
            argv_filename_dataframes is the filename for the text document that list all of the dataframes filename
            argv_id is the ID of the dataframe that would be used for the experiment
            argv_colname_split is the colname of the column that would be used to 
        """
        manager_dataframe = Manager_Dataframe(argv_filename_dataframes)
        manager_dataframe.load_dataframes(argv_dropNA=False)
        manager_dataframe.split_dataframe(argv_id=argv_id, argv_col=argv_colname_split, argv_inplace=True)
        self._dict_dataframes = manager_dataframe.get_dataframe()

    def load_intermediate(
        self,
        argv_filename_dataframes="",
        argv_id=""
        ):
        """
        Load the intermediate for the experiment into the attribute, it is a dataframe
            argv_filename_dataframes is the filename for the text document that list all of the dataframes filename
            argv_id is the ID of the dataframe that would be used for the intermediate points
        """
        manager_intermediate = Manager_Dataframe(argv_filename_dataframes)
        manager_intermediate.load_dataframes(argv_dropNA=False)
        self._df_intermediate = manager_intermediate.get_dataframe(argv_id=argv_id)

    def write_to_file(
        self,
        argv_content="",
        argv_filename=None
        ):
        """
        Function to append content to a file; could create a new file if the filename is provided
            argv_content is the stirng content to be written
            argv_filename is the filename for the content to be written
                append if filename not provided
        """
        if argv_filename is not None:
            self._filename_write = argv_filename
            self._writer = open(self._filename_write, "w+")
            self._writer.close()
        # open it as append
        self._writer = open(self._filename_write, "a+")
        self._writer.write(argv_content)
        self._writer.close()

    def generate_intermediates(
        self,
        argv_colname="POINT_nearest",
        argv_sizes=[],
        argv_weights=[],
        argv_unique=False
        ):
        """
        Function to generate the intermediate points
            argv_colname is the colname of the DF to be used as random intermediate points
            argv_sizes is a list of sizes to be used for the random list of intermediates
            argv_weights is a list of weights to be used as weights for the itnermediate points
                if list is empty, then the weights would be set to 0
            argv_unique is the boolean control for the lists to have unique values between them
        Return
            return_intermediates is a list of lists with randomly selected points
            return_intermediates_weight is a list of weights to be used as weight of intermediate nodes
                Note at the moment we do not use different weights between intermediate points, this can be a future experiment to work on
        """
        # the return as a list of list of points
        return_intermediates = []
        return_intermediates_weight = []
        # get all the points to be used aas intermediate
        current_points = self._df_intermediate[argv_colname].tolist()
        # print(current_points)
        # for each size, generate a random list of the size
        return_intermediates = generate_random_lists(
            argv_values=current_points,
            argv_counts=argv_sizes,
            argv_unique=argv_unique)
        # for the weight
        if len(argv_weights) == 0:
            return_intermediates_weight = [0] * len(return_intermediates)
        else:
            return_intermediates_weight = argv_weights
        # return it
        return return_intermediates, return_intermediates_weight

    def run_experiment_HERE(
        self,
        argv_filename_results="",
        argv_seed=1234,
        argv_selected_starts = [],
        argv_selected_ends = [],
        argv_start_count=10,
        argv_end_count=10,
        argv_count_layers_max=5,
        argv_size_layers=[],
        argv_fullpath=False,
        ):
        """
        Main function hardcoded for the experiment
            argv_filename_results is the filename for the results to be written
            argv_seed is the seed that governs the randomness for the experiments
            argv_selected_starts / argv_selected_ends are the lists of points to be used as the selected start and end points for the dijsktra
                if left empty aka len()=0 then random points would be selected based on argv_start_count and argv_end_count
            argv_start_count / argv_end_count is the number of randomly selected start/ end points
            argv_count_layers_max is the maximum number of possible layers
            argv_size_layers is a list of layer sizes
                if left empty, then the sizes would be from 2**1 to 2**7
            argv_fullpath is the boolean control if we want to output the full path for GIS plots.
        """
        # set the seed
        random.seed(argv_seed)

        # intermediate details
        current_count_layers_max = argv_count_layers_max
        current_size_layers = argv_size_layers
        if len(current_size_layers) == 0:
            current_size_layers = []
            for i in range(1,8,1):
                current_size_layers.append(2**i)

        # load the dataframe from the traffic data
        filename_dfs = "Listing_TrafficFlow_Cleaned.csv"
        current_id_df = "Selangor_Cleaned"
        current_colname_split = "PBT"
        self.load_dataframes(
            argv_filename_dataframes=filename_dfs,
            argv_id=current_id_df,
            argv_colname_split = current_colname_split
            )
        # load the intermediate from selangor POI
        filename_dfs = "Listing_POI.csv"
        current_id_df = "Selangor_Petrol"
        self.load_intermediate(
            argv_filename_dataframes=filename_dfs,
            argv_id=current_id_df
        )

        # set the graph details
        self._source = "start_point"
        self._destination = "end_point"
        self._weight = ["TMC_LE"]
        self._path = "geometry"

        # setup writer for results and write to header
        current_result_header = []
        current_result_header.append("current_dataframeID")
        current_result_header.append("count_layers")
        current_result_header.append("size_layers")
        current_result_header.append("weight_layers")
        current_result_header.append("timer_layer_graph")
        current_result_header.append("current_point_start")
        current_result_header.append("current_point_end")
        current_result_header.append("timer_dijkstra")
        current_result_header.append("timer_pathbuilding")
        current_result_header.append("current_distance")
        current_result_header.append("length_current_path")
        current_result_header.append("detour")
        # current_result_header.append("detour_long")
        # current_result_header.append("detour_lat")
        if argv_fullpath:
            current_result_header.append("full_path")
        # write the header
        self.write_to_file(
            argv_content=",".join(current_result_header),
            argv_filename=argv_filename_results
            )
        
        # count the cases
        count_path_exist = 0
        count_path_exist_not = 0

        # loop dataframes
        for current_key_dfs in self._dict_dataframes.keys():
            # loop layer count
            for current_count_layer in range(0,current_count_layers_max+1,1):
                # loop layer size
                for current_size_layer in current_size_layers:
                    # get the dataframe
                    current_df = self._dict_dataframes[current_key_dfs]
                    # initialize the graph
                    current_graph = Graph_Layered(
                        argv_dataframe=current_df,
                        argv_source=self._source, argv_destination=self._destination, argv_weight=self._weight
                        )
                    # get the edges stat
                    current_weight_stats = current_graph.get_stat_weights()

                    # select the start and end point
                    # reset the seed
                    random.seed(argv_seed)
                    current_selected_starts = argv_selected_starts
                    if len(current_selected_starts) == 0:
                        current_selected_starts = select_random(argv_values=current_df[self._source].tolist(), argv_count=argv_start_count)
                    current_selected_ends = argv_selected_ends
                    if len(current_selected_ends) == 0:
                        current_selected_ends = select_random(argv_values=current_df[self._destination].tolist(), argv_count=argv_end_count)

                    # loop all weight types for intermediate layers
                    for current_weight_stat in current_weight_stats.keys():
                        # get the weight for the intermediate layers
                        current_intermediate_weights = [current_weight_stats[current_weight_stat]] * current_count_layer
                        # get the intermediate layers
                        # reset the seed
                        random.seed(argv_seed)
                        current_intermediate_sizes = [current_size_layer] * current_count_layer
                        current_intermediate_points, current_intermediate_weights = self.generate_intermediates(
                            argv_colname="POINT_nearest",
                            argv_sizes=current_intermediate_sizes,
                            argv_weights=current_intermediate_weights,
                            argv_unique=False
                            )
                        # build the layered graph
                        run_timer()
                        current_graph.generate_layer_graph(
                            argv_intermediates_vertices=current_intermediate_points,
                            argv_intermediates_weights=current_intermediate_weights
                            )
                        timer_layer_graph = run_timer()
                        # have the graph details, might be useful for more analysis on different graph types towards performance
                        # we have our own generated graph maybe?
                        # current_graph.print_graph_details()
                        # build the dictionary route
                        # this is needed to expand the points
                        current_dict_routes = current_graph.build_dict_routes(argv_start=self._source, argv_end=self._destination, argv_geom=self._path)

                        # for each start/end pair
                        for current_point_start in current_selected_starts:
                            for current_point_end in current_selected_ends:
                                # skip if points the same
                                if current_point_start == current_point_end:
                                    continue
                                # try out the pathing with dijkstra using the points
                                # note, due to it being a digraph, path might not exist
                                try:
                                    # run dijkstra
                                    # run_timer()
                                    current_distance, current_path, timer_dijkstra, timer_pathbuilding = current_graph.run_dijsktra(argv_start=current_point_start, argv_end=current_point_end)
                                    timer_dijkstra = run_timer()
                                    # post-process dijkstra output               
                                    current_path_cleaned, current_detour, current_detour_long, current_detour_lat = clean_layered_path(argv_path=current_path, argv_df_path=current_dict_routes)
                                    # write the results
                                    current_result_value = []
                                    current_result_value.append(str(current_key_dfs))
                                    current_result_value.append(str(current_count_layer))
                                    current_result_value.append(str(current_size_layer))
                                    current_result_value.append(str(current_weight_stat))
                                    current_result_value.append(str(timer_layer_graph))
                                    current_result_value.append(str(current_point_start))
                                    current_result_value.append(str(current_point_end))
                                    current_result_value.append(str(timer_dijkstra))
                                    current_result_value.append(str(timer_pathbuilding))
                                    current_result_value.append(str(current_distance))
                                    current_result_value.append(str(len(current_path)))
                                    current_result_value.append(" ".join(current_detour))
                                    # current_result_value.append(" ".join(current_detour_long))
                                    # current_result_value.append(" ".join(current_detour_lat))
                                    #print(current_result_value)
                                    if argv_fullpath:
                                        current_result_value.append(" ".join(current_path_cleaned))
                                    self.write_to_file(
                                        argv_content="\n"
                                        )
                                    self.write_to_file(
                                        argv_content=",".join(current_result_value),
                                        )
                                    count_path_exist += 1
                                    # print("Path exist")
                                    # current_graph.print_graph_details()
                                except Exception as e:
                                    #print(e)
                                    count_path_exist_not += 1

                    # no need to repeat the different layer sizes if there is no layer
                    # this addition allow us to be more efficient
                    if current_count_layer == 0:
                        break

#%% ================================================================
"""
Main driver for experiment
"""
if __name__ == "__main__":
    # create the experiment object
    run_experiment = Experiment()
    # generate the filename for the results
    filename_output = "Results_CPU_" + get_current_timestamp() + ".csv"
    # run the experiment, values are hardcoded in the function already
    run_experiment.run_experiment_HERE(argv_filename_results=filename_output)

# %% ================================================================
# END