#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import json
import geopandas as gpd
import os
get_ipython().run_line_magic('matplotlib', 'inline')
import geohash
import geopandas as gp
import pandas as pd
import math
import geojson
from geojson import MultiLineString
from shapely.geometry import MultiLineString
from shapely.geometry import MultiPoint
import numpy as np
from shapely import geometry
from shapely.geometry import Point, Polygon, box,LineString
from geopandas import datasets, GeoDataFrame, read_file
from geopandas.tools import overlay
from matplotlib import pyplot
import matplotlib.pyplot as plt
from shapely.geometry.polygon import LinearRing, Polygon
from numpy import cos,sin,arccos
import numpy as np
from pylab import *
from shapely.ops import cascaded_union
from shapely.geometry import Point, LineString, mapping
import scipy.interpolate
import geopy
import geopy.distance
import time
import folium
import gmaps
import networkx as nx
from shapely.ops import nearest_points


# In[50]:


traffic_flow = gpd.read_file("/data/kl_selangor_traffic_flow_26082020_0000_0100")
traffic_flow.head()


# In[51]:


traffic_flow.dtypes


# In[52]:


traffic_flow['endpoints'] = traffic_flow['geometry'].apply(lambda x: x.boundary)


# In[53]:


def return_start_point(x):
    #print(x)
    try:
        return x.boundary[0]
    except:
        return None

def return_end_point(x):
    try:
        return x.boundary[1]
    except:
        return None
    

traffic_flow['start_point'] = traffic_flow['geometry'].apply(lambda x: return_start_point(x))
traffic_flow['end_point'] = traffic_flow['geometry'].apply(lambda x: return_end_point(x))


# In[54]:


traffic_flow.head()


# In[55]:


traffic_flow = traffic_flow.dropna()


# In[56]:


#traffic_flow['road_condition'] = 10 - traffic_flow['CF_JF']
traffic_flow['road_condition'] = traffic_flow['CF_JF']


# In[57]:


print(len(traffic_flow))


# In[58]:


traffic_flow.plot()


# ## Network Creation

# In[59]:


kl_selangor_graph = nx.Graph()
#geoms =[shape(feature['geometry']) for feature in fiona.open("msia_routes_kl/msia_routes_kl.shp")]
geoms = traffic_flow['geometry']
weights = traffic_flow['road_condition']
import itertools
# create a Graph
import networkx as nx

kl_selangor_graph_distance = nx.Graph()
kl_selangor_graph_roadCondition = nx.Graph()
graph_nodes = []


for row in traffic_flow.iterrows():
    #print(list(zip(row[1]['start_point'].coords.xy)))
    if row[1]['geometry'].geom_type == 'LineString':
        for seg_start, seg_end in zip(list(row[1]['geometry'].coords),list(row[1]['geometry'].coords)[1:]):
            seg_start_gps = (seg_start[1], seg_start[0])
            seg_end_gps = (seg_end[1], seg_end[0])
            #print(seg_start_gps)
            kl_selangor_graph_distance.add_nodes_from([seg_start_gps, seg_end_gps])
            kl_selangor_graph_distance.add_edge(seg_start_gps, seg_end_gps, weight=float(row[1]['TMC_LE']))
            kl_selangor_graph_roadCondition.add_nodes_from([seg_start_gps, seg_end_gps])
            kl_selangor_graph_roadCondition.add_edge(seg_start_gps, seg_end_gps, weight=float(row[1]['road_condition']))
            #graph_nodes.append(Point(seg_start_gps))
            #graph_nodes.append(Point(seg_end_gps))
    elif row[1]['geometry'].geom_type == 'MultiLineString':
        for line_part in row[1]['geometry']:
            for seg_start, seg_end in zip(list(line_part.coords),list(line_part.coords)[1:]):
                seg_start_gps = (seg_start[1], seg_start[0])
                seg_end_gps = (seg_end[1], seg_end[0])
                #print(seg_start_gps)
                kl_selangor_graph_distance.add_nodes_from([seg_start_gps, seg_end_gps])
                kl_selangor_graph_distance.add_edge(seg_start_gps, seg_end_gps, weight=float(row[1]['TMC_LE']))
                kl_selangor_graph_roadCondition.add_nodes_from([seg_start_gps, seg_end_gps])
                kl_selangor_graph_roadCondition.add_edge(seg_start_gps, seg_end_gps, weight=float(row[1]['road_condition']))
               
    


# In[60]:


print(len(kl_selangor_graph_distance))
print(len(kl_selangor_graph_roadCondition))


# In[61]:


list(kl_selangor_graph_distance.nodes)


# # Saving the network

# In[62]:


network_name = "kl_selangor_traffic_flow_26082020_0000_0100"
nx.write_gpickle(kl_selangor_graph_distance, '/data/kl_selangor_traffic_flow_26082020_0000_0100_distance.gpickle')
nx.write_gpickle(kl_selangor_graph_roadCondition, '/data/kl_selangor_traffic_flow_26082020_0000_0100_roadCondition.gpickle')


# In[63]:


origin = (3.01689, 101.37124)
destination = (2.97395, 101.68976)
destination = (3.02693, 101.57018)

route1 = nx.dijkstra_path(kl_selangor_graph_distance, origin, destination)
route2 = nx.dijkstra_path(kl_selangor_graph_roadCondition, origin, destination)

route1_arr_lat = []
route1_arr_lon = []

for element in route1:
    route1_arr_lat.append(element[1])
    route1_arr_lon.append(element[0])
    
route2_arr_lat = []
route2_arr_lon = []

for element in route2:
    route2_arr_lat.append(element[1])
    route2_arr_lon.append(element[0])
    
    





