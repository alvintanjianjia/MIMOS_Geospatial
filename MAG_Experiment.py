#%% ================================================================
"""
Imports
"""

import MAG_Util as MAG_Util
import MAG_Manager_Dataframe as MAG_Manager_Dataframe
import MAG_Graph_Layered as MAG_Graph_Layered

import random as random

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
            argv_df_intermediate is the dataframe with a column for points to be used as intermediate vertices in the layerd graph
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
        manager_dataframe = MAG_Manager_Dataframe.Manager_Dataframe(argv_filename_dataframes)
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
        manager_intermediate = MAG_Manager_Dataframe.Manager_Dataframe(argv_filename_dataframes)
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
        current_points = self._df_intermediate[argv_colname].to_arrow().to_pylist()
        # print(current_points)
        # for each size, generate a random list of the size
        return_intermediates = MAG_Util.generate_random_lists(
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
                    current_graph = MAG_Graph_Layered.Graph_Layered(
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
                        current_selected_starts = MAG_Util.select_random(argv_values=current_df[self._source].tolist(), argv_count=argv_start_count)
                    current_selected_ends = argv_selected_ends
                    if len(current_selected_ends) == 0:
                        current_selected_ends = MAG_Util.select_random(argv_values=current_df[self._destination].tolist(), argv_count=argv_end_count)

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
                        MAG_Util.run_timer()
                        current_graph.generate_layer_graph(
                            argv_intermediates_vertices=current_intermediate_points,
                            argv_intermediates_weights=current_intermediate_weights
                            )
                        timer_layer_graph = MAG_Util.run_timer()
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
                                    #print(current_distance, current_path)
                                    # timer_dijkstra = run_timer()
                                    # post-process dijkstra output
                                    #print(current_distance)
                                    if current_distance < sys.float_info.max:
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

                                        #print(current_result_value, 'current_result_value')

                                        if argv_fullpath:
                                            current_result_value.append(" ".join(current_path_cleaned))
                                        self.write_to_file(
                                            argv_content="\n"
                                            )
                                        self.write_to_file(
                                            argv_content=",".join(current_result_value),
                                            )
                                        count_path_exist += 1
                                        #print("Path exist")
                                        #current_graph.print_graph_details()
                                except Exception as e:
                                    print(e)
                                    #print('Something wrong')
                                    count_path_exist_not += 1

                    # no need to repeat the different layer sizes if there is no layer
                    # this addition allow us to be more efficient
                    if current_count_layer == 0:
                        break