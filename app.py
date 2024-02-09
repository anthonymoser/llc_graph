import io 
import networkx as nx
from ipysigma import Sigma
from util import *
import pandas as pd 
import asyncio 

from shiny import App, Inputs, Outputs, Session, reactive, render, ui
from shinywidgets import output_widget, render_widget
from shiny.types import FileInfo
from htmltools import TagList, div
from qng_classes import QNG 

def download_handler():
    return file_buffer()

### SHINY APP ###

app_ui = ui.page_fillable(
        ui.tags.style(
            "#sidebar_buttons .btn  {width:40%; margin: 10px 10px 10px 10px; padding:5px 5px 5px 5px;}",
        ),
        ui.h2("Dese Guys"),
        ui.layout_sidebar(
            ui.sidebar(
                ui.input_text("name_search", "Search for names", "Michael Tadin%"),
                ui.input_text("addr_search", "Search for addresses"),
                ui.input_text("file_number_search", "Search for file numbers"),
                ui.input_task_button("search_btn", "Search"),
                ui.input_task_button("expand_btn", "Expand"),
                ui.input_selectize("selected_nodes", "Select nodes", choices=[], multiple=True),  
                ui.input_checkbox("and_neighbors", "and connected nodes", value=False),
                ui.input_checkbox("tidy", "Combine likely duplicates", value=False),
                ui.row(
                    ui.input_action_button("remove", "Remove"),
                    ui.input_action_button("combine", "Combine"),
                    id="sidebar_buttons"    
                ),
                ui.download_button("export_graph", "Export HTML"),
                ui.download_button("export_entities", "Export business data"),
                ui.download_button("save_graph_data", "Save QNG Graph File"),
                ui.input_file("file1", "Upload QNG Graph File", accept=[".qng"], multiple=False, placeholder='*.qng'),

                width=300,
                open="always"
            ),
            ui.navset_card_tab(
                ui.nav_panel("Graph",
                    output_widget("sigma_graph")           
                ),
                ui.nav_panel("More controls",

                )
            ),  
        ),
        height=1000,
        title="Dese Guys"
    )



def server(input, output, session):
        
    ### CONTROL LOGIC     
    entities = reactive.value([])
    all_entities = reactive.value([])   
    G = reactive.value(nx.MultiGraph())
    nodes = reactive.value({})
    build_count = reactive.value(0)
    manually_combined = reactive.value([])


    @reactive.Effect
    @reactive.event(input.file1)
    def _():
        f: list[FileInfo] = input.file1()
        datapath = f[0]['datapath']
        if f[0]['type'] == "application/octet-stream":
            with open(datapath, 'r') as f:
                graph_data = msgspec.json.decode(f.read(), type=QNG)
                mg = graph_data.multigraph()
                for node in mg.nodes():
                    mg.nodes[node]['label'] = mg.nodes[node]['label'].upper()
                    
                print(len(mg))
                if len(G()) > 0:
                    G.set(nx.compose(G(), mg))
                else:
                    G.set(mg)

                build_count.set(build_count() + 1)


    ### Clicked Search
    @reactive.effect
    @reactive.event(input.search_btn)
    def search_click():
        search(input.name_search().upper(), input.addr_search().upper(), input.file_number_search())

    @ui.bind_task_button(button_id = 'search_btn')
    @reactive.extended_task
    async def search(name_search:str, addr_search:str, file_number_search:str) -> list:

        print("retrieving ids")
        name_ids = get_name_ids(name_search) if len(name_search) > 0 else []
        address_ids = get_address_ids(addr_search) if len(addr_search) > 0 else []
        print(name_ids, address_ids)    
        
        print("retrieving entities")
        name_entities = get_entities('name_id', name_ids)
        addr_entities = get_entities('address_id', address_ids)
        fn_entities = get_entities_by_file_number(file_number_search) if len(file_number_search) > 0 else []
        
        combined = combine_entitity_list([name_entities, addr_entities, fn_entities])
        print("entities:", len(combined))
        return combined 

    @reactive.effect
    @reactive.event(search.result)
    def set_entities():
        print("setting entities")
        result = search.result()
        all_now = all_entities() + result
        all_entities.set(all_now)    
        entities.set(result)



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
        print("building graph")
        new_graph = graph_entities(G(), entities())
        new_graph = fix_unlabeled_nodes(new_graph)
        for c in manually_combined():
            new_graph = combine_nodes(new_graph, c)
        G.set(new_graph)
        builds = build_count() + 1
        build_count.set(builds)


    ### Remove selected nodes
    @reactive.effect
    @reactive.event(input.remove)
    def _():
        G().remove_nodes_from(get_selected_nodes())
    
    
    ### Combine selected nodes
    @reactive.effect
    @reactive.event(input.combine)
    def _():
        selected = get_selected_nodes()
        print("Combining", selected)
        new_graph = combine_nodes(G(), selected)
        new_combined = manually_combined().copy()
        new_combined.append(selected)
        print("new combined", new_combined)
        manually_combined.set(new_combined)
        print(manually_combined())
        G.set(new_graph)
            
            
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
    @reactive.event(expand.result) 
    def set_expanded_graph():
        new_entities = expand.result()
        new_graph = graph_entities(G(), new_entities)
        print("Combined nodes", manually_combined())
        for c in manually_combined():
            new_graph = combine_nodes(new_graph, c)
        G.set(new_graph)
        entities.set(entities() + new_entities)
        builds = build_count() + 1
        build_count.set(builds) 


    # # Clear graph
    # @reactive.effect 
    # @reactive.event(input.clear)
    # def _():
    #     G().clear()
    #     all_entities.set([])

    # Update node list 
    @reactive.effect
    @reactive.event(build_count, input.tidy, input.remove, G) 
    def update_node_choices():
        node_names = {} 
        for n in G().nodes:
            name = G().nodes[n].get("label", n)
            node_names[name] = n
        nodes.set(node_names)            
        ui.update_selectize("selected_nodes", choices= list(nodes().keys()) )


    # Render graph 
    @render_widget(height="1000px")
    # @reactive.event(entities, G, expand.result, search.result, input.remove, input.tidy, all_entities)
    @reactive.event(input.remove, input.combine, build_count, input.tidy)
    def sigma_graph():
        if input.tidy() is True and len(G()) > 0:
            new_graph = tidy_up(G(), ignore_middle_initial=True)
            G.set(new_graph)

            
        print("rendering graph")
        return Sigma(
                G(), 
                height=1500,
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
                show_all_labels=True if len(G()) < 50 else False,
                start_layout= len(G()) / 10
        )
    
    @render.download(filename="graph_export.html")
    def export_graph():
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

    @render.download(filename="entities_export.csv" )
    def export_entities():        
        with io.BytesIO() as buf:
            df = (
                pd.DataFrame([ e.simplify() for e in entities() ])
                .assign(company = lambda df: df.file_number.apply(lambda n: G().nodes[n].get('label', n)))  
                .sort_values(['company', 'type'])
                [['id', 'file_number', 'company','type', 'name', 'address']]
            ).drop_duplicates().to_csv(buf, index=False)
            yield buf.getvalue()
            
    # @render.data_frame
    # @reactive.event(entities)
    # def entities_df():
    #     df = pd.DataFrame([ e.simplify() for e in entities() ])
    #     return render.DataGrid(df, row_selection_mode='multiple')
    
    
    @render.download(filename="quick_network_graph.qng")
    def save_graph_data():
        adj = nx.to_dict_of_dicts(G())
        attrs = { n: G().nodes[n] for n in G().nodes()}
        qng = QNG(adjacency=adj, node_attrs=attrs)
        yield msgspec.json.encode(qng)

app = App(app_ui, server)