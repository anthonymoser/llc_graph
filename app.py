import io 
import networkx as nx
from ipysigma import Sigma
from util import *
import pandas as pd 
import asyncio 

from shiny import reactive
from shiny.express import input, render, ui, session
from shinywidgets import output_widget, render_widget
from shiny.types import FileInfo
from htmltools import TagList, div

warmup = requests.get(endpoint)
def download_handler():
    return file_buffer()

### SHINY APP ###
ui.page_opts(title="Dese Guys", fillable=True)

with ui.sidebar():
    "Use % as a wildcard.\nCase insensitive"
    ui.input_text("name_search", "Search by name", "Michael Tadin")
    ui.input_text("addr_search", "Search by address")
    ui.input_text("file_number_search", "Search by file number")
    ui.input_task_button("search_btn", "Search")
    ui.input_task_button("expand_btn", "Expand")
        
        
    ui.input_selectize("selected_nodes", "Select nodes", choices=[], multiple=True)  
    ui.input_checkbox("and_neighbors", "and connected nodes")
    
    with ui.tooltip(placement="left"):
        ui.input_action_button("tidy", "Tidy up")
        "Combine variations on name and address that are probably the same"
        
    ui.input_action_button("remove", "Remove nodes")
    ui.input_action_button("clear", "Reset graph")
    ui.input_action_button("export_graph_btn", "Export graph")
    # @render.download("download_handler", filename="graph_export.html")


### CONTROL LOGIC     
entities = reactive.value([])
all_entities = reactive.value([])   
G = reactive.value(nx.MultiGraph())
first_build = reactive.value(True)
action_log = reactive.value("")
nodes = reactive.value({})

### Clicked Search
@reactive.effect
@reactive.event(input.search_btn)
def search_click():
    search(input.name_search().upper(), input.addr_search().upper())

@ui.bind_task_button(button_id = 'search_btn')
@reactive.extended_task
async def search(name_search:str, addr_search:str) -> list:

    print("retrieving ids")
    name_ids = get_name_ids(name_search)
    address_ids = get_address_ids(addr_search)
    print(name_ids, address_ids)    
    print("retrieving entities")
    name_entities = get_entities('name_id', name_ids)
    addr_entities = get_entities('address_id', address_ids)
    combined = combine_entitity_list(name_entities, addr_entities)
    print("entities:", len(combined))
    return combined 

@reactive.effect
@reactive.event(search.result)
def set_entities():
    print("setting entities")
    result = search.result()
    entities.set(result)
    all_entities.set(all_entities() + result)    


def get_selected_nodes():
    try:
        selected = [ nodes().get(n) for n in input.selected_nodes() ]
        neighbors = []
        if input.and_neighbors():
            for s in selected:
                neighbors += list(G().neighbors(s))
            neighbors = list(set(neighbors))
            selected += neighbors
        return selected  
    except Exception as e:
        return []
    
    
### Build graph from entities
@reactive.effect
@reactive.event(entities, search.result)
def build_graph():
    new_graph = graph_entities(G(), entities())
    G.set(new_graph)
    if first_build() and len(new_graph) > 0:
        first_build.set(False)
        expand(new_graph)


def remove_nodes(G: nx.MultiGraph, node_list:list = []) ->nx.MultiGraph:
    return G.remove_nodes_from(node_list)


### Remove selected nodes
@reactive.effect
@reactive.event(input.remove)
def _():
    G().remove_nodes_from(get_selected_nodes())
        
        
### Clicked Expand Graph
@reactive.effect
@reactive.event(input.expand_btn)
def expand_click():
    expand(G(), get_selected_nodes())

@ui.bind_task_button(button_id='expand_btn')
@reactive.extended_task
async def expand(graph: nx.MultiGraph, selected_nodes:list = []):
    return expand_graph(graph, selected_nodes) 
    
@reactive.effect 
def set_expanded_graph():
    G.set(expand.result()) 


# Clear graph
@reactive.effect 
@reactive.event(input.clear)
def _():
    G().clear()
    all_entities.set([])

# Tidy up  
@reactive.effect
@reactive.event(input.tidy)
def _():
    print("Tidying up")
    tidy_graph = tidy_up(G(), ignore_middle_initial=True)
    G.set(tidy_graph)

# Update node list 
@reactive.effect
@reactive.event(entities, search.result, expand.result, input.tidy, input.remove, input.clear) 
def update_node_choices():
    node_names = {} 
    for n in G().nodes:
        name = G().nodes[n].get("label", n)
        node_names[name] = n
    nodes.set(node_names)            
    ui.update_selectize("selected_nodes", choices= list(nodes().keys()) )

def file_buffer():
    with io.BytesIO() as bytes_buf:
                with io.TextIOWrapper(bytes_buf) as text_buf:
                    Sigma.write_html(
                        G(),
                        path=text_buf,  
                        height=1000,
                        layout_settings=large_layout if len(G()) > 1000 else small_layout, 
                        edge_color = 'type', 
                        node_size = G().degree, 
                        node_size_range = (3, 30),
                        clickable_edges = False,
                        node_color = 'type',
                        node_color_palette = {
                            "company": "black", 
                            "address": "#f9cf13",
                            "person": "#dd0f04", 
                        },
                        edge_color_palette={
                            "manager": "#e515ed",
                            "agent": "#00c3dd",  
                            "address": "#adadad",
                            "company": "black", 
                            "president": "#7a15ed", 
                            "secretary": "#2937f4"
                        },
                        show_all_labels=True if len(G()) < 100 else False,
                        start_layout= len(G()) / 10
                    )
                    yield bytes_buf.getvalue()

# Render graph 
@render_widget(height="1000px")
@reactive.event(entities, G, expand.result, input.remove, input.clear)
def sigma_graph():
    return Sigma(
            G(), 
            height=1000,
            layout_settings=large_layout if len(G()) > 1000 else small_layout, 
            edge_color = 'type', 
            node_size = G().degree, 
            node_size_range = (3, 30),
            clickable_edges = False,
            node_color = 'type',
            node_color_palette = {
                "company": "black", 
                "address": "#f9cf13",
                "person": "#dd0f04", 
            },
             edge_color_palette={
                "manager": "#e515ed",
                "agent": "#00c3dd",  
                "address": "#adadad",
                "company": "black", 
                "president": "#7a15ed", 
                "secretary": "#2937f4"
            },
            show_all_labels=True if len(G()) < 100 else False,
            start_layout= len(G()) / 10
    )
        
        
# @render.data_frame
# @reactive.event(entities)
# def entities_df():
#     df = pd.DataFrame([ e.simplify() for e in entities() ])
#     return render.DataGrid(df, row_selection_mode='multiple')