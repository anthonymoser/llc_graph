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
from qng import GraphSchema, NodeFactory, LinkFactory, GraphFactory, SigmaFactory, Element, QNG

def download_handler():
    return file_buffer()

def load_schema(filename:str):
    with open(filename, 'r') as f:
        return msgspec.json.decode(f.read(), type=GraphSchema)

gs = {
    "address":  load_schema('graph_schemas/address.qngs'),
    "name":     load_schema('graph_schemas/name.qngs'),
    "company":  load_schema('graph_schemas/company.qngs'),
    "links":    load_schema('graph_schemas/links.qngs'),
    "rsxa-ify5": load_schema('graph_schemas/rsxa-ify5 city_contracts.qngs')
}

gfs = {}
for key in gs.keys():
    gf_nf = list(dict(gs[key].node_factories).values())
    gf_lf = gs[key].link_factories
    gfs[key] = GraphFactory(node_factories=gf_nf, link_factories=gf_lf)

def help_link(id:str):
     return ui.input_action_link(id, ui.HTML('<i class="fa fa-question-circle" aria-hidden="true"></i>') )

question_circle =  ui.HTML('<i class="fa fa-question-circle" aria-hidden="true"></i>')

help_text = {
    "subgraph": ui.TagList(
                    ui.tags.p("Click a node on the graph or select some from the dropdown. The subgraph is anything connected to your selection."),
                    ui.tags.p("Preview to confirm it's what you want. Then you can delete it, or keep it and delete everything else.")
                ),
    "simple_paths": "Choose a starting point and and ending point and click 'Show' to see only nodes and edges that connect them. Click 'Clear' to return to the full graph.",
    "select": ui.TagList(ui.tags.p("Select node(s) by clicking on the graph and/or choosing from the dropdown."), ui.tags.p("Once selected, you can merge them together, remove them, or use them to search for more connections.")),
    "and_directly_connected": "in addition to what you selected, include any nodes directly linked to those nodes.",
    "merge_likely_duplicates": 'automatically merge nodes that are probably the same person/address - ("LASTNAME, FIRSTNAME JR" and "FIRSTNAME LASTNAME JR")',
    "name": "the name of a person or company. Use '%' as a wildcard", 
    "street": "the street address - exclude city/state/zip",
    "file_number": "corporate/llc file number, prefixed by CORP or LLC",
    "save/load_qng_graph_file": "Save a copy of this graph data in the Quick Network Graph (QNG) format, upload a graph you saved earlier, or upload one from Quick Network Graph at bit.ly/qng. Uploads are added to the current graph"
}

def tooltip(title:str):
    return ui.tooltip(
        ui.span(title, question_circle),
        help_text[title.lower().replace(" ", "_")],
        placement="right"
    )

### SHINY APP ###

app_ui = ui.page_fillable(
    ui.head_content(ui.include_css("font-awesome-4.7.0 2/css/font-awesome.min.css")),
    
    ui.tags.style("""
        .card-header {background-color: #efefef !important;}
        .fa-question-circle {margin: 0 0 0 5px; color:darkgrey;}
        .card li {margin-top: 5px !important;}
        .graph-control-button { width:100%; margin: 0px 0px 0px 0px;}
        .modal-content {width: 75%;}                     
        .modal-content li {margin-top 10px !important;}
        .modal-body .btn {margin: 5px 5px 5px 5px; align=center;}
        .modal-body #modal_buttons {margin: 10px 10px 10px 10px;}
        .modal-header {background-color: #ececec;}
        #card_tooltip {align-text:left;}
        
        """
    ),
    
    ui.div(
        ui.h2("Dese Guys", style="{margin: 0 0 0 0;}"),
        ui.TagList(ui.help_text("A "), ui.a("Public Data Tools", href="http://publicdatatools.com"), ui.help_text(" project")),
    ),
    
    ui.div(
       
        ui.accordion(
            ui.accordion_panel("Controls",
                ui.layout_columns(
                    ui.navset_card_tab(
                        ui.nav_panel("Search",
                            ui.card(
                                ui.layout_columns(
                                    ui.input_text("name_search",tooltip("Name"), "Michael Tadin%"),
                                    ui.input_text("addr_search",tooltip("Street")),
                                    ui.input_text("file_number_search", tooltip("File number")),
                                    col_widths=(4,4,4)
                                ),
                                ui.input_task_button("search_btn", "Search"),
                            ),
                        ),
                        
                        ui.nav_panel("Save/Load",
                            ui.layout_columns(
                                ui.card(
                                    ui.card_header("Export"),
                                    ui.tooltip(
                                        ui.download_button("export_graph", "Export HTML"),
                                        "Save a standalone HTML file of the interactive graph",
                                        placement="right"
                                    ),
                                    ui.download_button("export_entities", "Export business data"),
                                ),
                                ui.card(
                                    ui.card_header(tooltip("Save/Load QNG Graph File")),
                                    ui.input_file("file1", "", accept=[".qng"], multiple=True, placeholder='*.qng', width="100%"),
                                    ui.card_footer(
                                        ui.download_button("save_graph_data", "Save QNG Graph File"),
                                    ),
                                ),
                                col_widths=(4,8),
                                heights_equal = False,
                                fill=False
                            ),
                        ),
                        ui.nav_panel("Add Context", 
                        ui.layout_columns(
                            ui.card(
                                ui.card_header("Chicago Data Portal"),
                                ui.input_task_button("get_contracts", "Search for contracts")
                            ),
                        col_widths=(12)
                        ),
                        ),
                        ui.nav_panel("Filter", 
                        ui.layout_columns(
                                ui.card(
                                    ui.card_header(tooltip("Subgraph")),
                                    ui.layout_column_wrap(
                                                ui.input_action_button("preview_subgraph", "Preview", class_="graph-control-button"),
                                                ui.input_action_button("keep_subgraph", "Keep", class_="graph-control-button"),
                                                ui.input_action_button("remove_subgraph", "Remove", class_="graph-control-button"),
                                                ui.input_action_button("cancel_subgraph", "Cancel", class_="graph-control-button"),    
                                    width=(1/2),
                                    fill=False,
                                    ),
                                ),
                                ui.card(
                                    ui.card_header(tooltip("Simple paths")), 
                                    ui.layout_columns(
                                        ui.input_select("path_start", "Start", choices = []),
                                        ui.input_select("path_end", "End", choices = []),
                                    col_widths=(6,6)
                                    ),
                                    ui.card_footer(
                                        # ui.row(
                                            ui.input_action_button("clear_paths", "Clear"),
                                            ui.input_action_button("show_paths", "Show"),
                                        # )
                                    )
                    
                                ),
                        col_widths=(6,6),
                            ),
                        ),
                    ),
                    ui.card(
                        ui.card_header(tooltip("Select")),
                        ui.input_selectize("selected_nodes", "", choices=[], multiple=True, width="100%"),  
                        ui.input_checkbox("and_neighbors", tooltip("and directly connected"), value=False),
                        ui.input_checkbox("tidy", tooltip("merge likely duplicates"), value=False),
                    ),
                    ui.card(
                        ui.input_action_button("remove", "Remove"),
                        ui.input_action_button("combine", "Merge"),
                        ui.tooltip(
                            ui.input_task_button("expand_btn", "What else connects?"),
                            "Search for more names and addresses connected to the ones on the graph",
                            placement="bottom"
                        ),
                    ),
                    col_widths = (7,3,2),
                ),
            ),
            id="accordion_controls"
        ),
        # id="mainbox",
    id="frustrating"
    ),
    output_widget("sigma_graph"),
    height="auto",
    title="Dese Guys"
)

def server(input, output, session):
    ### System State         
    entities = reactive.value([])
    all_entities = reactive.value([])  
    G = reactive.value(nx.MultiGraph())
    nodes = reactive.value({})
    build_count = reactive.value(0)
    manually_combined = reactive.value([])
    connected_nodes = reactive.value({})
    
    ### Factories
    SF = reactive.value(SigmaFactory(clickable_edges=True))
    viz = reactive.value()
    
    
    def get_connected_to_selected():
        selected = get_selected_nodes()
        connected = {}
        for s in selected:
            connected.update(get_connected_nodes(G(), s, connected))
        # connected_nodes.set(connected)
        return connected 
            
    
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
                    try:
                        mg.nodes[node]['label'] = mg.nodes[node]['label'].upper()
                    except KeyError:
                        mg.nodes[node]['label'] = node.upper()
                    
                print(len(mg))
                if len(G()) > 0:
                    G.set(nx.compose(G(), mg))
                else:
                    G.set(mg)

                build_count.set(build_count() + 1)

    def get_modal(title:str|None = None, prompt:str|ui.TagList|None = None, buttons:list = [], size = "m", easy_close=False):
        ui.modal_remove()
        return ui.modal(
            prompt, 
            title=title,
            size=size,
            footer=ui.TagList([b for b in buttons]) if len(buttons) > 0 else None,
            easy_close = False if len(buttons) > 0 else True
        )
        


    ### ADVANCED CONTROLS
    
    @reactive.effect
    @reactive.event(input.preview_subgraph)
    def _():
        if len(G()) > 0:
            connected = get_connected_to_selected()
            connected_nodes.set(connected)
            
            if len(connected) > 0:  
                selected_SF = SigmaFactory(
                    layout_settings = {"StrongGravityMode": False}, 
                    node_color_palette = None, 
                    node_color = lambda n: "selected" if n in connected else "not selected"
                )
                layout = viz().get_layout()
                camera_state = viz().get_camera_state()
                viz.set(selected_SF.make_sigma(G(), node_colors="Dark2", layout=layout, camera_state=camera_state))
            else:
                m = get_modal(
                    title="You didn't select anything",
                    prompt="Select a node by clicking it, or choose some from the selection dropdown. Then you can preview a subgraph of everything able to connect to your selection, and either remove it all, or keep it and remove everything else.",
                    buttons = [ui.modal_button("OK")]
                    )
                ui.modal_show(m)
                
    
    @reactive.effect
    @reactive.event(input.keep_subgraph)
    def _():
        connected = get_connected_to_selected()
        if len(connected) == 0 and len(connected_nodes()) > 0:
            connected = connected_nodes()
        graph = nx.induced_subgraph(G(), connected)
        G.set(graph)
        
    
    @reactive.effect
    @reactive.event(input.remove_subgraph)
    def _():
        connected = get_connected_to_selected()
        if len(connected) == 0 and len(connected_nodes()) > 0:
            connected = connected_nodes()
        graph = G().copy()
        graph.remove_nodes_from(connected)
        G.set(graph)
        
    @reactive.effect
    @reactive.event(input.cancel_subgraph, input.clear_paths)
    def _():
        graph = G().copy()
        G.set(graph)
        
    
    # Show Simple Paths
    @reactive.Effect
    @reactive.event(input.show_paths)        
    def _():
        print("generating path graph")
        PG = path_graph = get_path_graph(G(), input.path_start(), input.path_end())
        viz.set(SF().make_sigma(PG))

            
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
        file_numbers = [e.file_number for e in name_entities] + [e.file_number for e in addr_entities]
        
        fn_entities = get_entities_by_file_number(file_number_search) if len(file_number_search) > 0 else []
        fn_entities += get_entities("file_number", file_numbers)
        
        combined = combine_entitity_list([name_entities, addr_entities, fn_entities])
        print("entities:", len(combined))
        return combined 

    @reactive.effect
    @reactive.event(search.result)
    def set_entities():
        print("setting entities")
        result = search.result()
        if len(result) > 0:
            all_now = all_entities() + result
            all_entities.set(all_now)    
            entities.set(result)
        else: 
            m = get_modal(
                title=None,
                prompt="No results",
                buttons = [ui.modal_button("OK")]
            )
            ui.modal_show(m)



    ### SELECTED NODES 
    def get_selected_nodes():
        try:
            selected = []
            print(viz().get_selected_node())
            if viz().get_selected_node() is not None:
                selected += [ viz().get_selected_node() ]
                
            selected += [ nodes().get(n) for n in input.selected_nodes() ]
                
            neighbors = []
            if input.and_neighbors():
                for s in selected:
                    neighbors += list(G().neighbors(s))
                neighbors = list(set(neighbors))
                selected += neighbors
            print("SELECTED NODES: ", selected)
            return selected  
        except Exception as e:
            return []
    
    # Update node list 
    def update_node_choices(graph):
        node_names = get_node_names(graph)
        nodes.set(node_names)
        choices = {node_names[n]: n for n in sorted(list(node_names.keys()))}
        # choices = sorted(list(nodes().keys()))
        ui.update_selectize("selected_nodes", choices= choices)
        ui.update_select("path_start", choices=choices)
        ui.update_select("path_end", choices=choices)   


    ### Remove selected nodes
    @reactive.effect
    @reactive.event(input.remove)
    def _():
        graph = G().copy()
        graph.remove_nodes_from(get_selected_nodes())
        G.set(graph)
    
    
    ### Combine selected nodes
    @reactive.effect
    @reactive.event(input.combine)
    def _():
        
        selected = get_selected_nodes()
        new_graph = combine_nodes(G(), selected)
        
        new_combined = manually_combined().copy()
        new_combined.append(selected)
        manually_combined.set(new_combined)
        
        G.set(new_graph)
        
    ### Build graph from entities
    @reactive.effect
    @reactive.event(entities, search.result)
    def build_graph():
        print("building graph")
        graph = G().copy()   
        graph = nx.compose(graph, gfs['company'].make_graphs([ e.company_dict() for e in entities() ], "il_sos"))
        graph = nx.compose(graph, gfs['name'].make_graphs([ e.name_dict() for e in entities() ],"il_sos"))
        graph = nx.compose(graph, gfs['address'].make_graphs([e.address_dict() for e in entities() ], "il_sos"))   
        graph = nx.compose(graph, gfs['links'].make_graphs([e.link_dict() for e in entities()], "il_sos"))
        
        unlabeled = get_unlabeled_companies(graph)
        companies = get_companies(unlabeled)
        graph = nx.compose(graph, gfs['company'].make_graphs([ c.company_dict() for c in companies ], "il_sos"))
        
        for c in manually_combined():
            graph = combine_nodes(graph, c)
        
        excluded_nodes = get_excluded_nodes(graph)
        for en in excluded_nodes:
            if " DISSOLUTION" in graph.nodes[en]['label'] or "REVOKED " in graph.nodes[en]['label']:
                for nbr in graph.neighbors(en):
                    if graph.nodes[nbr]['type'] == "company":
                        graph.nodes[nbr]['type'] = "company (inactive)"
            
            graph.remove_node(en)
        
        if len(graph) > 0:
            graph = deduplicate_edges(graph)
            G.set(graph)
            builds = build_count() + 1
            build_count.set(builds)
        

    
    
    ### Data Portal (Contracts)
    @reactive.effect
    @reactive.event(input.get_contracts)
    def contracts_click():
        streets = extract_street_parts(G())
        street_searches = get_street_searches(streets)
        search_contracts(street_searches)

    @ui.bind_task_button(button_id = 'get_contracts')
    @reactive.extended_task
    async def search_contracts(street_searches):
        rf = search_data_portal(resource_id='rsxa-ify5', keywords=street_searches)
        return rf 
        
    @reactive.effect
    @reactive.event(search_contracts.result)
    def _():
        rf = search_contracts.result()
        
        if len(rf) > 0:
            print(rf.result_id.nunique())
            
            m = get_modal(
                title = None, 
                prompt = ui.HTML(f"Add {rf.result_id.nunique()} results to the graph?"), 
                buttons = [ui.modal_button("Cancel"), ui.input_action_button("add_results", "Add")]
            )
            ui.modal_show(m)
        else:
            m = get_modal(
                title=None,
                prompt="No results",
                buttons = [ui.modal_button("OK")]
            )
            ui.modal_show(m)
    
    @reactive.effect
    @reactive.event(input.add_results)
    def _():
        ui.modal_remove()
        resource_id = 'rsxa-ify5'
        records = search_contracts.result().to_dict('records')
        graph = nx.compose(G(), gfs[resource_id].make_graphs([ row for row in records], "data.cityofchicago.org"))
        G.set(graph)
                
        
        
            
            
    ### Clicked Expand Graph
    @reactive.effect
    @reactive.event(input.expand_btn)
    def expand_click():
        selected = get_selected_nodes()
        if len(selected) == 0:
            m = get_modal(
                title = None,
                prompt = f"Search for connections to all {len(G())} nodes in the graph?",
                buttons = [ui.modal_button("Cancel"), ui.input_action_button("expand_all", "Confirm")]
            )
            ui.modal_show(m)
        else:
            expand(G(), selected)
    
    @reactive.effect
    @reactive.event(input.expand_all)
    def _():
        ui.modal_remove()
        expand(G(), [])        

    @ui.bind_task_button(button_id='expand_btn')
    @reactive.extended_task
    async def expand(graph: nx.MultiGraph, selected_nodes:list = []):
        return expand_graph(graph, selected_nodes) 
        
    @reactive.effect
    @reactive.event(expand.result) 
    def set_expanded_graph():        
        new_entities = expand.result()
        entities.set(entities() + new_entities)

    @reactive.effect
    def _():
        update_node_choices(G())
    


        
    # Tidy up
    @reactive.effect
    @reactive.event(G, build_count, input.tidy)
    def _():
        print(input.tidy(), len(G()))

        if input.tidy() is True and len(G()) > 0:
            graph = tidy_up(G())
        elif len(G()) > 0: 
            graph = tidy_up_companies(G())
        else: 
            graph = G()
        G.set(graph)
        

    # Make graph widget
    @reactive.effect
    @reactive.event(G) 
    def _():
        node_colors, edge_colors = get_colors(G())
        try:
            layout = viz().get_layout()
            camera_state = viz().get_camera_state()
            viz.set(SF().make_sigma(G(), node_colors, edge_colors, layout = layout, camera_state = camera_state))
        except Exception as e:
            print(e)
            viz.set(SF().make_sigma(G(), node_colors, edge_colors))
        
        
    # Update visualization
    @render_widget(height="1000px")
    def sigma_graph():
        return viz()
    
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
            

    @render.download(filename="quick_network_graph.qng")
    def save_graph_data():
        # no_data_source = get_nodes_by_attribute(G(), "data_source", None)
        # for node in no_data_source:
        #     G().nodes[node]['data_source'] = "il_sos"
            
        adj = nx.to_dict_of_dicts(G())
        attrs = { n: G().nodes[n] for n in G().nodes()}
        qng = QNG(adjacency=adj, node_attrs=attrs, sigma_factory=SF())
        yield msgspec.json.encode(qng)


app = App(app_ui, server)