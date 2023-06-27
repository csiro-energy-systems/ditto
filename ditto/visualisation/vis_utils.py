import collections
import json
import logging
import os
import webbrowser
from collections import defaultdict
from json import JSONDecodeError
from pathlib import Path

import matplotlib as mpl
import networkx as nx
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from pyvis.network import Network

import ditto
import ditto.network.network as dn
from ditto import Store
from ditto.models import Line
from ditto.models.position import Position
from ditto.models.power_source import PowerSource
from ditto.readers.abstract_lv_reader import get_impedance_from_matrix

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

default_pyvis_options = """
var options = {
  "configure": {
        "enabled": true,
        "filter": [
            "physics"
        ]
    },
  "nodes": {
    "borderWidth": 2,
    "borderWidthSelected": 4,
    "font": {
      "size": 7,
      "face": "tahoma"
    }
  },
  "edges": {
    "color": {
      "inherit": true
    },
    "font": {
      "size": 4,
      "face": "tahoma"
    },
    "hoverWidth": 2,
    "shadow": {
      "enabled": true
    },
    "smooth": {
      "type": "continuous",
      "forceDirection": "none"
    },
    "width": 3
  },
 "physics": {
    "forceAtlas2Based": {
      "gravitationalConstant": -52,
      "centralGravity": 0.005,
      "springLength": 20,
      "springConstant": 0.095,
      "damping": 1,
      "avoidOverlap": 0.35
    },
    "maxVelocity": 150,
    "minVelocity": 0.75,
    "solver": "forceAtlas2Based"
  }
}
"""


def plot_network(
        model: Store,
        source: str,
        title: str,
        out_dir: Path = None,
        feeder_subgraphs=None,
        feeder_head_node=None,
        engines: list = ['pyvis'],
        line_unique_features=['R1', 'X1', 'line_type', 'nominal_voltage', 'nameclass'],
        show_plot=False):
    """
    Plots a ditto model using networkx and pyvis to an HTML visualisation with colourised edges according to line characteristics, and nodes according to ditto model type.
    Useful for checking parsing correctness. There are actually 3 different rendering engines that do slightly different things, see 'engine' param for details.

    Note: This was developed for the Low Voltage Feeder Taxonomy project, and is not (yet) a general purpose visualisation tool. It is not guaranteed to work for all models.

    @param model: the ditto network model
    @param source: name of the powersource for this network
    @param title: title for the plot, and filename
    @param out_dir: directory to save the rendered file in. Won't save if None.
    @param engines: 'pyvis' (uses a force graph simulation for layout, and coloursnodes/edges by ditto classes) 'plotly' (uses lat/long coordinates in Position objects in model) or 'networkx' (quick, basic layout viz)
    @param line_unique_features: a list of features which together determine uniqueness of a Line (graph edge). Used for colouring edges.
    @return: (filename of the saved file, and the built ditto.Network)
    """
    ditto_network: dn.Network = dn.Network()
    ditto_network.build(model, source=source)

    if not out_dir.exists():
        out_dir.mkdir(parents=True, exist_ok=True)

    # Set the attributes in the graph
    ditto_network.set_attributes(model)

    # Equipment types and names on the edges
    nx.get_edge_attributes(ditto_network.graph, "equipment")
    nx.get_edge_attributes(ditto_network.graph, "equipment_name")

    # Convert to a networkx graph
    nx_graph = nx.Graph(ditto_network.graph)
    ditto_network.is_directed = False

    if 'pyvis' in engines:
        out_file = out_dir / make_filename_safe(f'{title}.pyvis.html')
        render_pyvis(model, ditto_network, nx_graph, title, out_file, show_plot, feeder_subgraphs, feeder_head_node, line_unique_features, use_line_lengths=True)
    if 'plotly' in engines:
        out_file = out_dir / make_filename_safe(f'{title}.plotly.html')
        render_plotly(nx_graph, title, out_file, show_plot)
    if 'networkx' in engines:
        out_file = out_dir / make_filename_safe(f'{title}.networkx.html')
        render_networkx(nx_graph, title, str(out_file).replace('.html', '.png'), show_plot)

    return out_file, ditto_network





def render_plotly(G, title, out_file, show_plot, relativise_coords=True):
    """
    A fairly basic absolute coordinate plot using Node lat/longs or x/y from model. Nodes without coordinates aren't plotted.

    @param G: a NetworkX undirected graph object
    @param title: title (and filename) for the plot
    @param out_file: directory to save in. None to not save.
    @param show_plot: True to open the resulting html file in a browser.
    @param relativise_coords: if True, all absolute lat/long coordinates will have the minimum lat/long in this model subtracted from them,
        so actual coordinates are difficult to identify. Useful if releasing data and coordinates are sensitive.
    @return: [plotly_fig, htrml_file_path
    """
    try:
        import plotly
        import plotly.graph_objects as go
    except:
        raise RuntimeError("Plotly not installed. Please install plotly to use this function.")

    plotly.io.templates.default = "plotly_dark"

    min_lat = 999999
    min_lon = 999999

    if relativise_coords:
        """ First pass: find minimum lat/long coords"""
        for node in G.nodes():
            pos = get_pos(G.nodes[node])
            if pos is not None:
                min_lat = min(min_lat, pos.lat if pos.lat is not None else pos.x)
                min_lon = min(min_lon, pos.long if pos.long is not None else pos.y)
    else:
        min_lat = 0
        min_lon = 0

    edge_x = []
    edge_y = []
    for edge in G.edges():
        from_node = G.nodes[edge[0]]
        to_node = G.nodes[edge[1]]
        from_pos = get_pos(from_node)
        to_pos = get_pos(to_node)
        if from_pos is not None and to_pos is not None:
            x0, y0 = get_xy(from_pos)
            x1, y1 = get_xy(to_pos)
            edge_x.append(x0-min_lat)
            edge_x.append(x1-min_lat)
            edge_x.append(None)
            edge_y.append(y0-min_lon)
            edge_y.append(y1-min_lon)
            edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=5, color='white'),
        hoverinfo='none',
        mode='lines',
    )

    node_x = []
    node_y = []
    for node in G.nodes():
        pos = get_pos(G.nodes[node])
        if pos is not None:
            x,y = get_xy(pos)
            node_x.append(x-min_lat)
            node_y.append(y-min_lon)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='jet',
            reversescale=True,
            color=[],
            line_color='lightgrey',
            size=20,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
            line_width=2))

    node_adjacencies = []
    node_text = []
    for node, adjacencies in enumerate(G.adjacency()):
        node_adjacencies.append(len(adjacencies[1]))
        node_text.append('# of connections: ' + str(len(adjacencies[1])))

    node_trace.marker.color = node_adjacencies
    node_trace.text = node_text

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title=title,
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=20, l=5, r=5, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    if out_file is not None:
        plotly.offline.plot(fig, filename=str(out_file.resolve()), include_plotlyjs='cdn', auto_open=show_plot)
        return fig, out_file
    else:
        return fig, None

def get_xy(pos: Position):
    x = pos.lat if pos.lat is not None and pos.lat !=0 else pos.x
    y = pos.long if pos.long is not None and pos.long !=0 else pos.y
    return x,y

def plot_lat_long(model: Store, out_dir=None, show_plot=False):
    import plotly
    import plotly.express as px

    lats, longs, types = get_lat_longs(model)
    df = pd.DataFrame({'lat': lats, 'long': longs, 'type': types})

    if len(lats) > 0 and len(lats) == len(longs):
        fig = px.scatter_mapbox(df, lat='lat', lon='long', color='type', hover_data=['lat','long','type'])
        fig.update_layout(mapbox_style="open-street-map")
        if out_dir is not None:
            plotly.offline.plot(fig, filename=str(out_dir) + f'/{model.dnsp}-{model.name}-coords.html', include_plotlyjs='cdn', auto_open=show_plot)
    else:
        logger.warning(f'Cant plot lat/long points, no data available in model: {model.name}')


def render_pyvis(
        model: Store,
        ditto_network: dn.Network,
        networkx_graph: nx.Graph,
        title: str,
        out_file: Path,
        show_plot: bool,
        feeder_subgraphs=None,
        feeder_head_node=None,
        line_unique_features=None,
        use_line_lengths: bool = False):
    """
    Render a pyvis plot of the ditto model without/ignoring known locations for the nodes, using force directed graph physics to do the node layouts.

    :param model:
    :param ditto_network:
    :param networkx_graph:
    :param title:
    :param out_file:
    :param show_plot:
    :param feeder_subgraphs:
    :param feeder_head_node:
    :param line_unique_features:
    :param use_line_lengths: set edge weights from their length.  This is better than setting the Node masses, but it's trickier to get the physics settings right to show the result
    """

    for e in networkx_graph.nodes():
        networkx_graph.nodes[e]['mass'] = 1

    nt = Network("95%", "95%", heading=title)
    nt.from_nx(networkx_graph)

    # nt.show_buttons(filter_=['physics'])  # enable this only if the set_options call is disabled (useful for tweaking the default physics settings etc), or you'll get a blank plot
    try:
        nt.set_options(default_pyvis_options)
    except JSONDecodeError as e:
        print(e, e.doc)
        raise e

    """ Get colors for unique classes of nodes based on their type """
    # Assign a colour to each edge based on its unique feature combination
    {m.name: type(m).__name__ for m in model.models if hasattr(m, 'name')}
    node_types = [type(m).__name__ for m in model.models]
    unique_node_types = np.unique(node_types)
    cmap = dict(zip(unique_node_types, get_discrete_colourmap(len(unique_node_types), base_cmap=plt.cm.tab20)))
    cmap['PowerSource'] = (1., 0., 0., 1.)  # Always make the powersource red
    cmap['PowerTransformer'] = (0., 1., 0., 1.)  # Always make the powersource blue
    cmap['NoneType'] = (0., 0., 0., 1.)  # Always make the missing types black
    [cmap[node_types[i]] for i in range(len(node_types))]

    """ Do stuff to nodes """
    for idx, e in enumerate(nt.nodes):

        """ Remove the occasional missing or non-JSON-serialisable objects from the model so it can render"""
        del_keys = []
        for key, val in e.items():
            if val is None or not valid_json(val):
                del_keys.append(key)
        for k in del_keys:
            del e[k]

        """ Set labels on visualisation """
        hovers = ''.join([f'{k} = {v}<br>' for k, v in dict(sorted(e.items())).items()])
        model_type = type(model.model_names.get(e.get("name"))).__name__
        e['label'] = f'{model_type}: {e["label"]}'
        e['title'] = f'<b>Type={model_type}<br> Name={e.get("name")}</b><br>' + hovers
        e['color'] = mpl.colors.to_hex(cmap[model_type])

    """ Determine unique classes of edges based on a subset of their attributes """
    type_to_edge, _ = get_line_types(ditto_network.graph, line_unique_features)
    line_types = list(type_to_edge.keys())

    # Assign a colour to each edge based on its unique feature combination
    sets = np.unique(line_types)
    try:
        feat_col_map = None
        cmap = dict(zip(sets, get_discrete_colourmap(len(sets) + 1)))
        feat_col_map = {line_types[i]: cmap[line_types[i]] for i in range(len(line_types))}
    except ZeroDivisionError:
        logger.warning(f'Error getting colourmap for Lines with n={len(sets)}', exc_info=True)

    """ Do stuff to edges """
    for idx, e in enumerate(nt.get_edges()):

        """ Set edge weights from their length.  This is better than setting the Node masses, but it's trickier to get the physics settings right to show the result """
        if use_line_lengths:
            if e.get('length') is not None and e.get('length') > 1:
                e['weight'] = e.get('length')
                # e['physics'] = False
            else:
                e['weight'] = 1
                # e['physics'] = True

        """ Remove the occasional non-JSON-serialisable objects from the model so it can render"""
        del_keys = []
        for key, val in e.items():
            # print(f'{key}:  {val} - {type(val)}')
            if val is None or not valid_json(val):
                del_keys.append(key)
        for k in del_keys:
            del e[k]

        """ Set labels on visualisation """
        hovers = ''.join([f'{k} = {v}<br>' for k, v in dict(sorted(e.items())).items()])
        e['title'] = f'<b>Name={e.get("name")}</b><br>' + hovers
        # n['title'] = 'Test Hover Label<br>other line'
        edge_type = type(model.model_names.get(e.get("name"))).__name__
        if feat_col_map is not None:
            line_type = edge_to_feat_str(e, line_unique_features)

            col = feat_col_map.get(line_type)
            if col is not None:
                e['color'] = mpl.colors.to_hex(col)
        line_features = [f'{k} = {prettyify(v)}\n' for k, v in dict(sorted(e.items())).items() if k in line_unique_features]  # string with all unique line features
        e['label'] = f"{'' if e.get('equipment_name') is None else e.get('equipment_name')}\n" + ''.join(line_features)
        e['label'] = f'{edge_type}: {e["label"]}'

    if feeder_subgraphs is not None:
        """ Draw feeder Lines thicker """
        for feeder in feeder_subgraphs:
            for idx, e in enumerate(nt.get_edges()):
                feeder_edges = feeder.edges
                if feeder_edges is not None:
                    if (e['from'], e['to']) in feeder_edges or (e['to'], e['from']) in feeder_edges:
                        # n['color'] = mpl.colors.to_hex((0., 0., 1., 1.))
                        e['width'] = 8

    if feeder_head_node is not None:
        """ Make the feeder_head node yellow"""
        for e in nt.nodes:
            if e['name'] == feeder_head_node:
                e['color'] = mpl.colors.to_hex((1., 1., 0., 1.))
                e['size'] = 15
                break

    if out_file is not None:
        # logger.info(f'Saved network plot to {f}')
        # nt.show(str(f))
        {n['x']: n['y'] for n in nt.nodes if 'x' in n.keys()}
        nt.write_html((str(out_file.resolve())))
        # nx.readwrite.gml.write_gml(H, str(f.resolve())+'.gml')
        if show_plot:
            webbrowser.open(str(out_file.resolve()))


def render_networkx(H, title, out_file, show_plot):
    """ Visualise Graph """
    # try:
    #     pos = nx.nx_agraph.graphviz_layout(H) # needs pygraphvix installed.  Difficult in Windows.
    # except ImportError:

    pos = nx.spring_layout(H, iterations=40)

    plt.rcParams["text.usetex"] = False
    plt.figure(figsize=(20, 20))
    nx.draw_networkx_edges(H, pos, alpha=0.3, edge_color="m")
    nx.draw_networkx_nodes(H, pos, alpha=0.4, node_color="r")
    nx.draw_networkx_edges(H, pos, alpha=0.4, node_size=1, width=1, edge_color="k")
    nx.draw_networkx_labels(H, pos, font_size=9)
    edge_labels = {(u, v): '' if d.get('equipment_name') is None else d.get('equipment_name') for u, v, d in H.edges(data=True)}
    nx.draw_networkx_edge_labels(H, pos, edge_labels=edge_labels, font_size=9)
    plt.title(f"{title}")

    if out_file is not None:
        plt.savefig(str(out_file))
        logger.info(f'Saved network plot to {out_file}')

    if show_plot:
        url = "file://" + os.path.abspath(str(out_file))
        webbrowser.open(url)



def get_node_edge_properties(edges, graph, line_props):
    """
    Builds a dataframe with various characteristics of a set of edges in a ditto network
    :param edges:
    :param net:
    :param line_props:
    :return:
    """
    nodes = set()
    for e in edges:
        nodes.add(e[0])
        nodes.add(e[1])

    data = collections.OrderedDict()
    data.update((f, graph.edges[edges[0]].get(f)) for f in line_props)  # edges should all have the same properties, so just get their unique_feautres from the first one
    data['n_edges'] = len(edges)
    data['min_degree'] = np.min(list(dict(nx.degree(graph, nodes)).values()))
    data['avg_degree'] = np.mean(list(dict(nx.degree(graph, nodes)).values()))
    data['max_degree'] = np.max(list(dict(nx.degree(graph, nodes)).values()))
    data['sum_metres'] = sum(filter(None, [graph.edges[e].get('length') for e in edges]))
    data['n_fuses']    = sum(filter(None, [graph.edges[e].get('is_fuse') for e in edges]))
    data['n_switches'] = sum(filter(None, [graph.edges[e].get('is_switch') for e in edges]))
    data['n_recloser'] = sum(filter(None, [graph.edges[e].get('is_recloser') for e in edges]))
    return data


def line_to_feat_str(line: Line, line_unique_features: list):
    """
    Encodes a Line to a fixed string representation containing values for all provided features. Basically, this gives us a unique key for a Line for comparing its type to other Lines.
    :param line: the ditto Line model to encode
    :param line_unique_features: list of feature (Line properties) to include
    :return:
    """
    return str([round(line.__dict__['_trait_values'].get(f), 5) if isinstance(line.__dict__['_trait_values'].get(f), float) else line.__dict__['_trait_values'].get(f) for f in line_unique_features])


def edge_to_feat_str(edge_dict: dict, line_unique_features: list):
    """
    Encodes a networkx edge-dict to a fixed string representation containing values for all provided features. Basically, this gives us a unique key for a Line for comparing its type to other Lines.
    :param line: the ditto Line model to encode
    :param line_unique_features: list of feature (Line properties) to include
    :return:
    """
    return str([round(edge_dict.get(f), 5) if isinstance(edge_dict.get(f), float) else edge_dict.get(f)  for f in line_unique_features])


def weighted_diameter(graph, weight_prop: str):
    """
    Weighted graph diameter.
    See https://groups.google.com/g/networkx-discuss/c/ibP89C97BLI?pli=1
    :param graph:
    :param weight_prop:
    :return: weighted diameter
    """
    sp = dict(nx.shortest_path_length(graph, weight=weight_prop))
    e = nx.eccentricity(graph, sp=sp)
    diameter = nx.diameter(graph, e=e)
    return diameter


def get_line_types(graph, line_unique_features):
    """ Find the set of distinct Line types (based on a given set of attributes like impedance, lineclass etc) """
    line_types = []
    type_to_edge = defaultdict(list)

    """ Make sure the R/X values have been set from the matrix (I'm looking at you Ausgrid)"""
    for e in graph.edges:
        if graph.edges[e].get('impedance_matrix') is not None and len(graph.edges[e]['impedance_matrix']) > 0:
            try:
                impedances = get_impedance_from_matrix(graph.edges[e]['impedance_matrix'])
                graph.edges[e].update(dict(zip(['R0', 'X0', 'R1', 'X1'], impedances)))
            except:
                pass

    for edge in graph.edges:
        feats = edge_to_feat_str(graph.edges[edge], line_unique_features)
        type_to_edge[feats].append(tuple(sorted(edge)))  # Have to sort the edge-to/from order because apparently this isnt' fixed in networkx.  Sigh.
        line_types.append(str(feats))

    """ Build a table of line types and their properties, mostly for debugging purposes """
    ltypes = []
    for lt in type_to_edge.keys():
        edges = type_to_edge[lt]
        subgraph_props = get_node_edge_properties(edges, graph, line_unique_features)
        ltypes.append(pd.DataFrame(index=[lt], data=subgraph_props))
    ltypes = pd.concat(ltypes)
    ltypes = ltypes.sort_values('R1', ascending=True)
    return type_to_edge, ltypes


def get_trivial_lines(graph, line_unique_features, short_line_threshold=1.0, trivial_line_R1_threshold=0.01, trivial_line_substrs=['removable', 'fuse', 'switch', 'connector']):
    """
    Finds all 'trivial' Line types in the model, eg short lines, lines with very low or missing R1, 'openable' lines (switches, breakers, fuses etc).

    :param model:
    :param graph:
    :param line_unique_features: a list of line name or lineclass substrings that flag a line as trivial (which means it's considered part of every type-subgraph)
    :param short_line_threshold:
    :param trivial_line_R1_threshold:
    :param trivial_line_substrs:
    :return: trivial_edges - a list of networkx edge tuples determined to be trivial. trivial_types - line-types determined to be trivial. type_to_edge - a dict mapping line tuypes to list of edge tuples, ltypes - a pandas dataframe report on the types found
    """

    type_to_edge, ltypes = get_line_types(graph, line_unique_features)

    # trivial_edges = [l.name for l in model.models if isinstance(l, Line) and any(sub in l.name.lower() for sub in trivial_line_substrs)]
    # trivial_edges.extend([l.name for l in model.models if isinstance(l, Line) and (l.is_switch or l.is_fuse or l.is_breaker or l.is_recloser or l.is_network_protector or l.is_sectionalizer)])

    """ Get edge-tuple to edge-data-dict mapping (for cleaner code) """
    ed = {e: graph.edges[e] for e in graph.edges} #ed = edge-data
    """ Find edges with missing or low R1 """
    trivial_edges = [tuple(sorted(e)) for e in graph.edges if 'R1' not in ed[e].keys() or np.isnan(ed[e]['R1']) or (ed[e]['R1'] < trivial_line_R1_threshold)]
    """ Find edges that are switches, fuses or breakers """
    trivial_edges.extend([tuple(sorted(e)) for e in graph.edges if ed[e].get('is_switch') or ed[e].get('is_fuse') or ed[e].get('is_breaker')])
    """ Find short edges """
    trivial_edges.extend([tuple(sorted(e)) for e in graph.edges if graph.edges[e].get('length') is None or graph.edges[e].get('length') <= short_line_threshold])  # short lines
    """ Find substrings in edge names """
    trivial_edges.extend([tuple(sorted(e)) for e in graph.edges if any(['name' in ed[e].keys() and s.lower() in ed[e].get('name') for s in trivial_line_substrs])])


    """ Find edge types with missing ot low R1"""
    trivial_types = ltypes[(ltypes['R1'] < trivial_line_R1_threshold) | (ltypes['R1'].isna())].index.values if 'R1' in ltypes.columns else []
    """ Make sure all edges of trivial edge types are added"""
    if trivial_types is not None:
        trivial_edges.extend([tuple(sorted(e)) for e in graph.edges if edge_to_feat_str(graph.edges[e], line_unique_features) in trivial_types])  # add names of lines with trivial types

    return trivial_edges, trivial_types, type_to_edge, ltypes

def is_there_a_path(_from, _to):
    visited = set() # remember what you visited
    while _from:
        from_node = _from.pop(0) # get a new unvisited node
        if from_node in _to:
            # went the path
            return True
        # you need to implement get_nodes_referenced_by(node)
        for neighbor_node in nx.get_nodes_referenced_by(from_node):
            # iterate over all the nodes the from_node points to
            if neighbor_node not in visited:
                # expand only unvisited nodes to avoid circles
                visited.add(neighbor_node)
                _from.append(neighbor_node)
    return False

def get_power_sources(store):
    """
    Gets a list of power source names from a ditto Store object
    @param store: the store to process
    @return: list of names
    """
    power_source_names = []
    for obj in store.models:
        if isinstance(obj, PowerSource) and obj.is_sourcebus == 1:
            power_source_names.append(obj.name)

    power_source_names = np.unique(power_source_names)
    return power_source_names

def make_filename_safe(value, allow_unicode=False):
    """
    Taken from https://github.com/django/django/blob/master/django/utils/text.py
    Convert to ASCII if 'allow_unicode' is False. Convert spaces or repeated
    dashes to single dashes. Only allows characters: '-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    """
    import unicodedata
    value = str(value)
    if allow_unicode:
        value = unicodedata.normalize('NFKC', value)
    else:
        value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode('ascii')
    import string
    valid_chars = "-_.() %s%s" % (string.ascii_letters, string.digits) #Allows: '-_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(c for c in value if c in valid_chars)



def get_lat_longs(model):
    from collections import defaultdict
    positions = defaultdict(list)
    positions = {type(p).__name__ : p.positions for p in model.models if hasattr(p, 'positions') and p.positions is not None and p.positions != []}  # get Positions from model attribute
    positions.update({'Position': [p] for p in model.models if isinstance(p, ditto.models.position.Position)})  # get Positions that were put directly into from store.model
    lats = []
    longs = []
    types = []
    for p in positions.items():
        typ = p[0]
        pos_list = p[1]
        for pos in pos_list:
            if pos.lat is not None and pos.lat != 0 and pos.long is not None and pos.long != 0:
                lats.append(pos.lat)
                longs.append(pos.long)
                types.append(typ)

    return lats, longs, types


def open_switches_in_cycles(model, source, line_unique_features):
    """
    For any cycles in the network graph, see if there are any open-able edges (eg switches, fuses, etc), and iteratively open/remove them to see if the cycle can be removed.
    We try to do this in a deterministic manner, so that repeated runs result in the same loop-free network.
    Note that this MODIFIES THE DITTO MODEL - removing 'trivial' Lines if cycles are found containing them! Pass model.copy() if this is an issue.
    Note that this process may disconnect parts of the network entirely, so use with caution.
    :param model: ditto Store
    :param source: the source node name
    :param line_unique_features: list of features to use to uniquely identify lines
    :return: ditto Store with open switches
    """

    """ Build ditto network and networkx graphs """
    net: dn.Network = dn.Network()
    net.build(model, source=source)
    net.set_attributes(model)  # Set the attributes in the graph
    graph = nx.Graph(net.graph)

    """ Remove all nodes are are not ditto Nodes, Transformers or PowerSources """
    ditto_types = {node.name: type(model.model_names.get(node.name)).__name__ for node in model.models if hasattr(node, 'name')}
    drop_nodes = [name for name, t in ditto_types.items() if t not in ['Node', 'PowerTransformer', 'PowerSource']]
    graph.remove_nodes_from(drop_nodes)

    trivial_edges, trivial_types, type_to_edge, type_df = get_trivial_lines(graph, line_unique_features)

    """ Number of graph cycles/loops """
    cycle_list = nx.algorithms.cycle_basis(graph)
    n_cycles_orig = len(cycle_list)
    new_cycle_list = []
    pass_count = 1
    while len(cycle_list) > 0:

        removable_edges = defaultdict(list)
        if len(cycle_list) > 0:
            for cycle in cycle_list:

                for idx, from_node in enumerate(cycle):
                    to_node = cycle[(idx + 1) % len(cycle)]
                    edge_key = tuple(sorted((from_node, to_node)))
                    edge_dict = graph.edges[edge_key]
                    edge_type = edge_to_feat_str(edge_dict, line_unique_features)

                    # Add the edges with trivial types first, as these (might be) more likely to be switches.  Maybe?
                    if edge_type in trivial_types:
                        removable_edges[tuple(cycle)].append(edge_key)

                    # Then add the individual trival edges. These are less likely to be switches, though it's still possible.  Maybe.
                    if (from_node, to_node) in trivial_edges:
                        removable_edges[tuple(cycle)].append(edge_key)

        if len(removable_edges) == 0:
            logger.warning(f'No more removable edges found, but still {len(cycle_list)} cycles remaining!  Giving up.')
            logger.debug(f'Remaining cycles: {cycle_list}')
            break

        for cycle in removable_edges.keys():
            edges = removable_edges[cycle]
            e = edges[0] # Just aribtrarily pick the first removable edge to delete.  In lieu of better information about the lines, I'm not sure there's a better approach.

            """ Remove the line from the ditto model """
            if e in graph.edges: #need to check because previous cycle traversals may have already removed this edge from another direction
                model_name = graph.edges[e].get('equipment_name')
                m = model.model_names[model_name]
                model.model_store.remove(m)

                """ Remove the edge from the networkx graph, so we can check the result"""
                graph.remove_edge(*e)

                logger.debug(f'Removed first edge "{e}" and model ({model_name}) from cycle with {len(cycle)} edges and {len(edges)} removable candidates, cycle={cycle}')

        new_cycle_list = nx.algorithms.cycle_basis(graph)
        if len(new_cycle_list) > 0:
            logger.warning(f'{len(new_cycle_list)} of {len(cycle_list)} original cycles remaining in graph after {pass_count} passes removing trivial cycle edges')
        else:
            logger.info(f'All cycles removed from graph after {pass_count} passes. Huzzah!')
        cycle_list = new_cycle_list
        pass_count += 1

    cycles_removed = n_cycles_orig - len(new_cycle_list)
    conn = list(nx.connected_components(graph))
    if len(conn) > 1:
        # TODO Modify the associated model to match the disconnected subgraphs? Does this ever happen?
        logger.warning(f'After removing cycle-edges, graph has {len(conn)} disconnected subgraphs, returning all graphs.')
        graphs = []
        for idx, c in enumerate(conn):
            g = graph.subgraph(c)
            graphs.append(g)
        return graphs, type_df, cycles_removed
    else:
        return [graph], type_df, cycles_removed


def valid_json(o):
    try:
        json.dumps(o)
        return True
    except:
        return False

def get_discrete_colourmap(n: int, base_cmap=plt.cm.jet):
    """
    Gets a list of n RGBA colour tuples, uniformly sampled from a matplotlib colormap
    @param n: number of colors to return.
    @param base_cmap: the base colormap to sample from.
    @return: a list of n RGBA tuples
    """

    # extract all colors from the base map
    [base_cmap(i) for i in range(base_cmap.N)]
    # force the first color entry to be grey
    # cmaplist[0] = (.5, .5, .5, 1.0)
    # define the bins and normalize
    bounds = np.linspace(0, n, n+1)
    norm = mpl.colors.BoundaryNorm(bounds, base_cmap.N)
    cols = [base_cmap(norm(i)) for i in bounds]
    return cols


def valid_json(o):
    try:
        json.dumps(o)
        return True
    except:
        return False

def prettyify(v: object):
    """ Converts arbitrary objects to strings, with some prettification (eg. rounding floats to 3 decimal places etc) """
    if isinstance(v, float):
        return f'{v:.3f}'
    else:
        return str(v)

def get_pos(m):
    return m['positions'][0] if 'positions' in m.keys() and m['positions'] is not None and m['positions'] != [] else None
