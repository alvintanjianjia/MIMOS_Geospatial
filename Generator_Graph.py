# import random
import random as random

def generated_graph_simple(
    argv_count_vertices=10,
    argv_count_edges=10,
    argv_flag_directed=True,
    argv_weight_type="random",
    argv_weight_range=[0,10],
    argv_random_seed=1234
    ):
    """
    Function to generate random graph
    Return a list of edges
        u and v are int
        w are 
    """
    # return list of edges
    return_edges = []
    # set the seed
    random.seed(argv_random_seed)
    # if weight type
    if argv_weight_type == "destination" or argv_weight_type == "source":
        dict_weight = {}
        for current_vetex in range(argv_count_vertices):
            dict_weight[str(current_vetex)] = random.randint(argv_weight_range[0],argv_weight_range[1])
    # build the edges first before adding the weights
    # key: u,v
    # value: w
    dict_edges = {}
    counter_edges = 0
    while counter_edges < argv_count_edges:
        # randrange to exclude the V itself
        current_u = random.randrange(0,argv_count_vertices)
        current_v = random.randrange(0,argv_count_vertices)
        # weight the edges
        if argv_weight_type == "random":
            current_w = random.randint(argv_weight_range[0],argv_weight_range[1])
        elif argv_weight_type == "destination":
            current_w = dict_weight[str(current_v)]
        elif argv_weight_type == "source":
            current_w = dict_weight[str(current_u)]
        # check if the edge exist
        current_edge = str(current_u) + "," + str(current_v)
        if current_edge in dict_edges:
            continue
        else:
            dict_edges[current_edge] = current_w
            counter_edges += 1
            # for undirect add another one as well
            if argv_flag_directed == False:
                current_edge = str(current_v) + "," + str(current_u)
                dict_edges[current_edge] = current_w
                counter_edges += 1
    # then rebuild back
    for current_key in dict_edges.keys():
        return_edges.append(current_key + "," + str(dict_edges[current_key]))
    # return the edges
    return return_edges