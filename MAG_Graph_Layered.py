#%% ================================================================
"""
Imports
"""

# import util tools
import MAG_Util as MAG_Util

# import other libraries needed
import statistics
# import networkx as networkx
import cugraph as cugraph


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
        # have the source and destination column as string
        self._dataframe[self._source] = self._dataframe[self._source].astype(str)
        self._dataframe[self._destination] = self._dataframe[self._destination].astype(str)

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
            current_values = self._dataframe[self._weight[0]].to_arrow().to_pylist()
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
            current_values = self._dataframe[argv_weight].to_arrow().to_pylist()
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
        self._graph = MAG_Util.generate_graph(
            argv_df=self._dataframe,
            argv_source=self._source, argv_destination=self._destination, argv_edge=self._weight, 
            argv_directed=argv_directed)

    def generate_layer_graph(self, argv_intermediates_vertices=[[]], argv_intermediates_weights=[]):
        """
        Generate a layered graph from the dataframe, using the information stored within the attributes
        The layered graph is generated through a dataframe approach (pandas) before being fed directly into networkx/ cudf for the graph
        Note graph is always directed for the layered graph approach
            argv_intermediates_vertices is the list of vertices (vertex ID) to be used as intermediate vertices
            argv_intermediates_weights is the list of weights (numeric) to be used for the edges connecting these intermediate vertices
        """
        # update the attributes first
        self._intermediates_vertices = argv_intermediates_vertices
        self._intermediates_weights = argv_intermediates_weights
        # generate the layers, store within dataframe
        current_df_layered = MAG_Util.generate_graph_layers_different(
            argv_df=self._dataframe,
            argv_source=self._source, argv_destination=self._destination, argv_weight=self._weight[0], 
            argv_intermediates_vertices=argv_intermediates_vertices, argv_intermediates_weights=argv_intermediates_weights,
            argv_type="dataframe"
            )
        # generate the graph
        self._graph = MAG_Util.generate_graph(
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
            print("Density: " + str(cugraph.density(self._graph)))
        except:
            pass

    def run_dijsktra(self, argv_start="", argv_end="", argv_path=True, argv_layer=True):
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
        if argv_layer:
            current_source = str(argv_start) + "_0"
            current_target = str(argv_end) + "_" + str(len(self._intermediates_vertices))
        else:
            current_source = str(argv_start)
            current_target = str(argv_end)
        if argv_path:
            return_distance, return_path_nodes, timer_dijkstra, timer_pathbuilding = MAG_Util.run_dijkstra(
                argv_graph=self._graph,
                argv_source=current_source,
                argv_target=current_target,
                argv_weight_edge=self._weight[0],
                argv_path=argv_path
            )
            return return_distance, return_path_nodes, timer_dijkstra, timer_pathbuilding
        else:
            return_table_distance, timer_dijsktra = MAG_Util.run_dijkstra(
                argv_graph=self._graph,
                argv_source=current_source,
                argv_target=current_target,
                argv_weight_edge=self._weight[0],
                argv_path=argv_path
            )
            return return_table_distance, timer_dijsktra

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
        current_geometries = self._dataframe[argv_geom].to_arrow().to_pylist()
        current_starts = self._dataframe[argv_start].to_arrow().to_pylist()
        current_ends = self._dataframe[argv_end].to_arrow().to_pylist()
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