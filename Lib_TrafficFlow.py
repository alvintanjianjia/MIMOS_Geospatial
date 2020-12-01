"""
Library file for processing
"""

#%% ================================================================
"""
Initialization
"""

#%% Import libraries
"""
Alias the external libraries, so that versioning is better.
This is vital as well when we use cuDF and cuGraph for accelerated computing.
"""
# simple python libraries
import os
import re
import operator
import time
# pandas
import pandas as pandas
import geopandas as geopandas
# graph
import networkx as networkx
# for map drawing
import folium as folium
# shapes
import shapely as shapely
# visualization
# we do not have it here in the library, it will be done in ipynb itself
# random
import random
random.seed(1234)
def random_set_seed(argv_seed):
    random.seed(argv_seed)

#%% Flags control
"""
Control flags that are useful 
"""
FLAG_printDetail = False

#%% Dictionary for folders and filenames
"""
Using dictionary to store the values for quick access
"""
# folders
dict_folders = {}
dict_folders["dataset"] = "Dataset"
# traffic flows
dict_trafficflow = {}
dict_trafficflow["KL_Selangor"] = "KL_Selangor_TrafficFlow_20200906_0326.csv"

dict_trafficflow_paper = {}
# dict_trafficflow_paper["KL_Selangor_wednesday_0800"] = "kl_selangor_traffic_flow_traffic_flow_20200826_0800_0900.csv"
# dict_trafficflow_paper["KL_Selangor_wednesday_1200"] = "kl_selangor_traffic_flow_traffic_flow_20200826_1200_1300.csv"
# dict_trafficflow_paper["KL_Selangor_wednesday_2100"] = "kl_selangor_traffic_flow_traffic_flow_20200826_2100_2200.csv"
dict_trafficflow_paper["KL_Selangor_thursday_0800"] = "kl_selangor_traffic_flow_traffic_flow_20200910_0800_0900.csv"
dict_trafficflow_paper["KL_Selangor_thursday_1200"] = "kl_selangor_traffic_flow_traffic_flow_20200910_1200_1300.csv"
dict_trafficflow_paper["KL_Selangor_thursday_2100"] = "kl_selangor_traffic_flow_traffic_flow_20200910_2100_2200.csv"

dict_trafficflow_paper["KL_Selangor_sunday_0800"] = "kl_selangor_traffic_flow_traffic_flow_20200906_0800_0900.csv"
dict_trafficflow_paper["KL_Selangor_sunday_1200"] = "kl_selangor_traffic_flow_traffic_flow_20200906_1200_1300.csv"
dict_trafficflow_paper["KL_Selangor_sunday_2100"] = "kl_selangor_traffic_flow_traffic_flow_20200906_2100_2200.csv"

dict_trafficflow_paper["KL_Selangor_monday_0800"] = "kl_selangor_traffic_flow_traffic_flow_20200907_0800_0900.csv"
dict_trafficflow_paper["KL_Selangor_monday_1200"] = "kl_selangor_traffic_flow_traffic_flow_20200907_1200_1300.csv"
dict_trafficflow_paper["KL_Selangor_monday_2100"] = "kl_selangor_traffic_flow_traffic_flow_20200907_2100_2200.csv"

# point of interest
dict_poi = {}
dict_poi["Selangor"] = "Selangor_POI.csv"
dict_poi["Selangor_Petrol"] = "Selangor_POI_Petrol.csv"
dict_poi["Selangor_Shell_VPower"] = "Locations_Shell_VPower.txt"

# get them into the correct path


# function to print the dictionary out
def print_dictionary(argv_dict):
    for current_key in argv_dict.keys():
        print(str(current_key) + " : " + str(argv_dict[current_key]))

# add in the path
def complete_path(argv_dict, argv_folder):
    for current_key in argv_dict:
        argv_dict[current_key] = os.path.join(argv_folder, argv_dict[current_key])
    return argv_dict

dict_trafficflow = complete_path(argv_dict=dict_trafficflow, argv_folder=dict_folders["dataset"])
dict_trafficflow_paper = complete_path(argv_dict=dict_trafficflow_paper, argv_folder=dict_folders["dataset"])
dict_poi = complete_path(argv_dict=dict_poi, argv_folder=dict_folders["dataset"])

#%% ================================================================
"""
Setup the environment to run if needed
"""

#%% Set CWD
def set_CWD():
    """
    Change to CWD to where the file is located
    """
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    os.chdir(dname)
    if FLAG_printDetail:
        print(os.getcwd())

#%% ================================================================
"""
File IO
"""

def load_dataset(argv_filename, argv_separator=",", argv_type="dictionary", argv_name=False):
    """
    Function to load dataset into a different datatypes
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
Data frames
with pandas/ geopandas
"""

def load_into_dataframe(argv_filename, argv_pandas_type=None, argv_pandas_geomname="geometry"):
    """
    Load into pandas for now
        Trying to avoid GeoPandas till am confident can containerize it for docker
    """
    if argv_pandas_type is None:
        return_dataframe = pandas.read_csv(filepath_or_buffer=argv_filename, skip_blank_lines=True).dropna()
    elif argv_pandas_type == "geopandas":
        return_dataframe = geopandas.read_file(filename=argv_filename, GEOM_POSSIBLE_NAMES=argv_pandas_geomname, skip_blank_lines=True)
    return return_dataframe

def print_details(argv_dataframe):
    """
    Simple function to print out the details of a dataframe
    """
    print(argv_dataframe.info())
    print(argv_dataframe.loc[:, argv_dataframe.columns != "geometry"].describe(include="all"))
    print(argv_dataframe.head(5))

def preprocess_dataframe_trafficflow(argv_df, argv_source="HERE"):
    """
    Preprocess the dataframe
        Based what what we are aware with for HERE

    More unique FI code (described via TMC_PC) than description for the route. Some FI are not described.
    Route length are generally short except with some really long routes. We can visualize this in comparison with FC (route types) to gain a better understanding.
    Both speed (SP as capped and SU as uncapped) seemed very similar. We would however want to map it against the FF (free flow speed, ie speed when all conditions are clear) to see if they how close they are.
    From the jam factor, there are none which were not calculated (-1) or closed (10) but we can again map this against the other factor such as speed and length.
    Since the confidence of the mesures are all above 0.7, we are confident with all of the values and thus this column can be dropped.
    Timestamps (PBT) is alright.
    The route descriptor for flow items (FIS) has less values than the descriptor of the flow item (FI). If we are to scope down, we can limit to only the ones with descriptors.
    We filtered out missing endpoints (due to how HERE maps data are with subsequences). We can however see repetition in points, thus this let us know that we can somewhat build a well-connected graph.
    Note: TMC stands for traffic message channel
    """
    if argv_source == "HERE":
        # format datatype
        argv_df["geometry"] = argv_df["geometry"].astype("category") # have the geometry as categories for now
        argv_df["TMC_PC"] = argv_df["TMC_PC"].astype("category")
        argv_df["TMC_DE"] = argv_df["TMC_DE"].astype("category")
        argv_df["TMC_QD"] = argv_df["TMC_QD"].astype("category")
        argv_df["TMC_LE"] = argv_df["TMC_LE"].astype("float64")
        argv_df["FC"] = argv_df["FC"].astype("category")
        argv_df["CF_SP"] = argv_df["CF_SP"].astype("float64")
        argv_df["CF_SU"] = argv_df["CF_SU"].astype("float64")
        argv_df["CF_FF"] = argv_df["CF_FF"].astype("float64")
        argv_df["CF_JF"] = argv_df["CF_JF"].astype("float64")
        argv_df["CF_CN"] = argv_df["CF_CN"].astype("float64")
        argv_df["PBT"] = argv_df["PBT"].astype("datetime64")
        argv_df["DE"] = argv_df["DE"].astype("category")
        argv_df["endpoints"] = argv_df["endpoints"].astype("category")  # might consider to drop this
        argv_df["start_point"] = argv_df["start_point"].astype("category")
        argv_df["end_point"] = argv_df["end_point"].astype("category")
        # clean the NAs
        # # we can selectively drop in the future maybe as the data is dirty, but for now let us just do this
        # dataframe_trafficflow.dropna(inplace=True)    # this doesn't work due to empty values are not treated as null
        argv_df = argv_df[argv_df["start_point"] != ""]
        argv_df = argv_df[argv_df["end_point"] != ""]

        # format start/ end values
        argv_df["start_point"] = argv_df["start_point"].astype(str)
        argv_df["end_point"] = argv_df["end_point"].astype(str)
        current_start_points_new = get_vertices_from_dataframe(argv_df=argv_df, argv_col="start_point", argv_before="POINT (", argv_after=")", argv_type=None, argv_swap_latlong=True, argv_asPoint=False)
        current_end_points_new = get_vertices_from_dataframe(argv_df=argv_df, argv_col="end_point", argv_before="POINT (", argv_after=")", argv_type=None, argv_swap_latlong=True, argv_asPoint=False)
        
        for i in range(len(current_start_points_new)):
            current_start_point_new = current_start_points_new[i]
            current_end_point_new = current_end_points_new[i]
            current_start_point_new = " ".join(current_start_point_new)
            current_end_point_new = " ".join(current_end_point_new)
            current_start_points_new[i] = current_start_point_new
            current_end_points_new[i] = current_end_point_new

        argv_df["start_point"] = current_start_points_new
        argv_df["end_point"] = current_end_points_new
        
        argv_df["start_point"] = argv_df["start_point"].astype(str)
        argv_df["end_point"] = argv_df["end_point"].astype(str)
        argv_df["start_point"] = argv_df["start_point"].astype("category")
        argv_df["end_point"] = argv_df["end_point"].astype("category")

    return argv_df


def preprocess_dataframe_poi_petrol(argv_df, argv_source="MIMOS"):
    if argv_source == "MIMOS":
        argv_df["FEATURE CODE"] = argv_df["FEATURE CODE"].astype("category")
        argv_df["FEATURE NAME"] = argv_df["FEATURE NAME"].astype("category")
        argv_df["NAME"] = argv_df["NAME"].astype("category")
        argv_df["TYPE"] = argv_df["TYPE"].astype("category")
        argv_df["STATE"] = argv_df["STATE"].astype("category")
        argv_df["LAT"] = argv_df["LAT"].astype(str)
        argv_df["LONG"] = argv_df["LONG"].astype(str)
        argv_df["POINT"] = argv_df["LAT"] + " " + argv_df["LONG"]
        argv_df["POINT"] = argv_df["POINT"].astype("category")
    return argv_df

def remove_duplicate_by_timestamp(argv_df):
    """
    Remove duplicates
        Using timestamps as within the hour we could have multiple entry in the route.
        Would use earliest date
        Would use first entry, based on points as ID
    """
    # subset by earliest date
    argv_df = argv_df.loc[argv_df["PBT"]==argv_df["PBT"].min()]
    # based on points
    current_dict_points = {}
    current_start_points = argv_df["start_point"].tolist()
    current_end_points = argv_df["end_point"].tolist()
    current_max = [0, None]
    current_indices = []
    for i in range(len(current_start_points)):
        current_start_point = current_start_points[i]
        current_end_point = current_end_points[i]
        current_point = str(current_start_point) + " -- " + str(current_end_point)
        if current_point in current_dict_points:
            current_dict_points[current_point] = current_dict_points[current_point] + 1
            if current_dict_points[current_point] > current_max[0]:
                current_max = [current_dict_points[current_point], current_point]
        else:
            current_dict_points[current_point] = 1
            current_indices.append(i)
    # subset based on the uniques
    return_df_subset = argv_df.iloc[current_indices, :]
    # return it
    return return_df_subset


def remove_duplicate_from_dataframes(argv_dfs, argv_colnames=[]):
    """
    Remove duplicates from a collection of datapoints
    """
    # track the frequency of each points
    dict_points = {}
    for current_df_key in argv_dfs.keys():
        current_df = argv_dfs[current_df_key]
        current_values_raw = []
        for current_colname in argv_colnames:
            # print(current_colname)
            current_values_raw.append(current_df[current_colname].tolist())
        current_values = zip(*current_values_raw)
        for current_value in current_values:
            try:
                # print(current_value)
                current_value = " ".join(current_value)
                if current_value in dict_points:
                    dict_points[current_value] = dict_points[current_value] + 1
                else:
                    dict_points[current_value] = 1
            except Exception as e:
                pass
    current_points = []
    for current_key in dict_points.keys():
        if dict_points[current_key] == len(argv_dfs):
            current_points.append(current_key)
    # so now we have the matching points
    # print(len(current_points))
    # let us subset each of the DF now
    for current_df_key in argv_dfs.keys():
        current_indices = []
        current_df = argv_dfs[current_df_key]
        current_values_raw = []
        for current_colname in argv_colnames:
            current_values_raw.append(current_df[current_colname].tolist())
        current_values = zip(*current_values_raw)
        i = 0
        for current_value in current_values:
            try:
                # print(current_value)
                current_value = " ".join(current_value)
                if current_value in current_points:
                    current_indices.append(i)
            except Exception as e:
                pass
            i = i + 1
        current_df = current_df.iloc[current_indices, :]
        argv_dfs[current_df_key] = current_df
    # return the dfs dictionary
    return argv_dfs


#%% ================================================================
"""
Visualization
    Won't be done here
"""

#%% ================================================================
"""
Coordinate formatting
- LatLong (NE)
    - also known as the y
    - Traditional usage
    - Easy to remember as they are in order
    - Used in raw input and visualization
    - used in Folium as well
    - used in nearest routing
- LongLat (EN)
    - also known as the x
    - Used in MIMOS dash for visualization
        - Routes are formatted like [[101.59911,3.06528],[101.59898,3.06528],[101.69611,3.04941]]
    - HERE route dataset in this format as well
    - Often used during processing, baed on WSG84
- Malaysia LatLong is 3.1390° N, 101.6869° E
- https://en.wikipedia.org/wiki/World_Geodetic_System
"""

def extract_coordinate(argv_coordinate, argv_before="", argv_after="", argv_type=None):
    """
    return [long,lat] or [lat,long]
    """
    if argv_type is None:
        argv_coordinate = argv_coordinate.replace(argv_before, "")
        argv_coordinate = argv_coordinate.replace(argv_after, "")
        current_tokens = argv_coordinate.split()
        return current_tokens
    return None

def swap_coordinate(argv_coordinate, argv_separator=",", argv_type=None):
    """
    return the values 
    """
    if argv_type is None:
        current_tokens = argv_coordinate.split(argv_separator)
        return str(current_tokens[1]) + str(argv_separator) + str(current_tokens[0])
    elif argv_type == "list":
        return_list = []
        for current_item in argv_coordinate:
            current_tokens = current_item.split(argv_separator)
            return_list.append(str(current_tokens[1]) + str(argv_separator) + str(current_tokens[0]))
        return return_list
    return None


def get_vertices_from_dataframe(argv_df, argv_col, argv_before="", argv_after="", argv_type=None, argv_swap_latlong=False, argv_asPoint=False):
    """
    Extract all vertices data into a list
        We want them to be latlong ideally
    """
    return_vertices = []
    current_vertices = argv_df[argv_col].to_list()
    for current_vertex in current_vertices:
        current_vertex = extract_coordinate(
            argv_coordinate=current_vertex,
            argv_before=argv_before,
            argv_after=argv_after,
            argv_type=argv_type
        )
        if argv_swap_latlong:
            current_vertex[0], current_vertex[1] = current_vertex[1], current_vertex[0]
        if argv_asPoint:
            current_vertex = shapely.geometry.Point(
                float(current_vertex[0]),
                float(current_vertex[1])
            )
        return_vertices.append(current_vertex)
    return return_vertices


def get_nearest_vertex(argv_point, argv_vertices=[], argv_point_type="string"):
    """
    Get the nearest point on the map based on the given point
        argv_point is a shapely.geometry.Point object at latlong
        argv_vertices is a list of shapely.geometry.Point objects at latlong
    """
    if argv_point_type == "string":
        current_vertex = argv_point.split()
        current_vertex = shapely.geometry.Point(float(current_vertex[0]), float(current_vertex[1]))
        return [o.wkt for o in shapely.ops.nearest_points(current_vertex, shapely.geometry.MultiPoint(argv_vertices))][1]
    elif argv_point_type == "Point":
        return [o.wkt for o in shapely.ops.nearest_points(argv_point, shapely.geometry.MultiPoint(argv_vertices))][1]










#%% ================================================================
"""
Mapping with Folium
"""

def folium_build(argv_point, argv_zoom=10):
    """
    argv_point is the starting point in latlong format, needs to be floats
        example: [3.12779,101.73297]
    """
    return folium.Map(location=argv_point, zoom_start=argv_zoom)

def folium_add_marker(argv_map, argv_point, argv_popup=None, argv_color="blue"):
    """
    Add a marker to the map
    argv_point is in latlong format
        example [3.12779,101.73297]
    """
    folium.Marker(
        location=argv_point,
        popup=argv_popup,
        icon=folium.Icon(color=argv_color)
    ).add_to(argv_map)

def folium_add_path(argv_map, argv_path, argv_popup=None, argv_color="blue", argv_weight=5, argv_opacity=1):
    """
    argv_path is in latlong format
        example: [[3.12779,101.73297],[3.12779,102.73297]]
    """
    folium.PolyLine(
        locations=argv_path,
        popup=argv_popup,
        color=argv_color,
        weight=argv_weight,
        opacity=argv_opacity
    ).add_to(argv_map)

def folium_add_area(argv_map, argv_locations, argv_tooltip=None):
    """
    Add polygon to map
        [[3.0443327,101.5166065],[3.0444475,101.5169714],[3.0440544,101.517095],[3.0439396,101.5167301],[3.0443327,101.5166065]]
    """
    folium.Polygon(
        locations=argv_locations,
        tooltip=argv_tooltip
    ).add_to(argv_map)












#%% ================================================================
"""
Graph generation related
"""

def generate_graph(argv_df, argv_source, argv_destination, argv_edge=[]):
    """
    Generate a graph with the edges weighted according to a list of values
        from a dataframe
    """
    return_graph = networkx.from_pandas_edgelist(
        df=argv_df,
        source=argv_source,
        target=argv_destination,
        edge_attr=argv_edge,
    )
    return return_graph

def generate_graph_layers(argv_df, argv_u, argv_v, argv_w, argv_intermediate, argv_intermediate_weight, argv_layers_count=2, argv_type="dataframe"):
    """
    Generated layered graph
        u v w are the edge details
        intermediate are the nodes and the weight between nodes
        there should be at least 2 layers
        type is how we do the layered, as of now it is either via graph of dataframe
            dataframe is easier
    """
    if argv_type == "graph":
        pass
    elif argv_type == "dataframe":
        current_u = argv_df[argv_u].tolist()
        current_v = argv_df[argv_v].tolist()
        current_w = argv_df[argv_w].tolist()
        current_u_new = []
        current_v_new = []
        current_w_new = []
        # duplicate it for multiple layers first
        for count_layer in range(argv_layers_count):
            for count_row in range(len(current_u)):
                current_u_new.append(str(current_u[count_row]) + "_" + str(count_layer))
                current_v_new.append(str(current_v[count_row]) + "_" + str(count_layer))
                current_w_new.append(float(current_w[count_row]))
        # add in the connectors
        for count_layer in range(argv_layers_count-1):
            for count_intermediate in range(len(argv_intermediate)):
                current_u_new.append(str(argv_intermediate[count_intermediate]) + "_" + str(count_layer))
                current_v_new.append(str(argv_intermediate[count_intermediate]) + "_" + str(count_layer+1))
                current_w_new.append(float(argv_intermediate_weight))
        # create the df
        return_df = pandas.DataFrame(
            data=list(zip(current_u_new, current_v_new, current_w_new)),
            columns=[argv_u, argv_v, argv_w]
        )
        return return_df



#%% ================================================================
"""
Graph algorithms
"""

def run_dijkstra(argv_graph, argv_source, argv_target, argv_weight_edge):
    """
    Run dijkstra
        source and target are vertex IDs
        graph could be multi edge, thus we would want to specific what is the weight we are being minimum
    """
    current_distance, current_path = networkx.single_source_dijkstra(
        G=argv_graph,
        source=argv_source,
        target=argv_target,
        weight=argv_weight_edge
    )
    return current_distance, current_path








#%% ================================================================
"""
Random functions
Mainly used for evaluations
"""

def select_random(argv_values, argv_count=10):
    """
    Randomly select from a list of values
    """
    return_selected = []
    while len(return_selected) < argv_count:
        current_value = random.choice(argv_values)
        if current_value in return_selected:
            continue
        return_selected.append(current_value)
    return return_selected
