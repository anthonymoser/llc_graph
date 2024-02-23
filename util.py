import networkx as nx
import msgspec
import pandas as pd
import usaddress 
import probablepeople as pp 
import requests 
import msgspec 
import json
from business_class import Entity
from sodapy import Socrata
from address_util import get_label


endpoint = "https://companies-mvwuoztvlq-uc.a.run.app"
data_portal_url = "data.cityofchicago.org"
client = Socrata(data_portal_url, None)

small_layout = {
        "adjustSizes": True,
        "StrongGravityMode": False,
        "edgeWeightInfluence":.3
    } 

large_layout = {
        "adjustSizes": False,
        "barnesHutOptimize": True,
        "barnesHutTheta":1,
        "StrongGravityMode": False,
        "edgeWeightInfluence":1
}

def get_excluded_nodes(G):
    return [ n for n in G.nodes() if has_excluded_name(G.nodes[n]['label']) ]
    

def has_excluded_name(node_label:str):
    excluded = ["INVOLUNTARY", "VACANT", "VACATED", "SOLE OFFICER", "None", "SAME ", "REVOKED ", " DISSOLUTION", "UNACCEPTABLE ", "MERGED "]
    for e in excluded:
        if e in node_label:
            return True 
    
    # if no excluded terms are in the node label, return false 
    return False 


def get_alias_ids(G, nodes:list):
    full_list = []
    for n in nodes:
        if has_excluded_name(G.nodes[n]['label']) is False:
            try:
                full_list += G.nodes[n]['alias_ids']
            except Exception as e:
                full_list.append(n)
    return full_list 


def paginate(url):
    items = []
    while url:
        response = requests.get(url)
        try:
            url = response.links.get("next").get("url")
        except AttributeError:
            url = None
        items.extend(msgspec.json.decode(response.content))
    return items


def get_entities(field, search_values):
    url = f"{endpoint}/companies/entities.json?_labels=on&_shape=array&{field}__in={json.dumps(search_values)}"
    data = paginate(url)
    return [ Entity(**d) for d in data ]


def get_companies(file_numbers:list):
    url = f"{endpoint}/companies/entities.json?_labels=on&_shape=array&file_number__in={json.dumps(file_numbers)}&type__exact=company"
    data = paginate(url)
    return [ Entity(**d) for d in data ]


def get_unlabeled_companies(G):
    return [n for n in G.nodes if 'label' not in G.nodes[n].keys()]


def get_name_ids(search_value):
    url = f"{endpoint}/companies/names.json?_shape=array&name__like={search_value}"
    data = paginate(url)
    return [d['id'] for d in data]


def get_address_ids(search_value): 
    url = f"{endpoint}/companies/addresses.json?_shape=array&street__like={search_value}"
    data = paginate(url)
    return [d['id'] for d in data]


def get_entities_by_file_number(search_value):
    url = f"{endpoint}/companies/entities.json?_labels=on&_shape=array&file_number__like={search_value}"
    data = paginate(url)
    return [ Entity(**d) for d in data ]


def expand_nodes(nodes:list):
    name_ids = []
    file_numbers = []
    address_ids = []
    
    for n in nodes:
        
        
        match n:
            case str(x) if x[:3] in ['LLC', 'COR']:
                file_numbers.append(n) 
            case str(x) if x[:1] == "A":
                address_ids.append(n[1:])
            case str(x) if x[:1] == "N":
                name_ids.append(n[1:])
    
    entities = get_entities('name_id', name_ids)
    entities += get_entities('address_id', address_ids)
    file_numbers += [e.file_number for e in entities]
    entities += get_entities('file_number', file_numbers)
    
    return entities

     
def expand_graph(G:nx.MultiGraph, node_list = []) ->list:
    entities = []
    node_ids = []
    
    if len(node_list) == 0:
        # print("expanding all nodes")
        # gids = get_graph_ids(G)
        # for g in gids:
        #     gids[g] = get_alias_ids(G, gids[g])
        #     node_ids += gids[g]
        node_list = list(G.nodes())
    # else: 
    
    print(f"expanding {len(node_list)} nodes")
    node_ids = get_alias_ids(G, node_list)
        
    node_ids = list(set(node_ids))
    segments = list(divide_list(node_ids, 100))
    for s in segments:
        entities += expand_nodes(list(set(node_ids)))        
    return entities

def divide_list(l, n):   
    for i in range(0, len(l), n):  
        yield l[i:i + n] 

def extract_name_parts(G:nx.MultiGraph):
    name_nodes = get_nodes_by_attribute(G, "tidy", "name")
    names_parts = {}
    for n in name_nodes:
        try:
            name = G.nodes[n]['label'].replace('.', '').strip().upper()
            parts = pp.parse(name)
            names_parts[n] = parts
        except Exception as e:
            print(e, n)
            continue
    
    name_records = []
    company_name_records = []
    for name in names_parts:
        parts = names_parts[name]
        if "CorporationName" in dict(names_parts[name]).values():
            parts = [p[0].replace(',', '').replace('.', '').upper() for p in parts]
            company_name_records.append({"node_id": name, "company_name": " ".join(parts)})
        else:
            record = {part[1]: part[0].replace(',', '').replace('.', '') for part in parts}
            record["node_id"] = name
            name_records.append(record)  
            
    return pd.DataFrame(name_records).fillna(''), pd.DataFrame(company_name_records).fillna('')


def clean_streets(G: nx.MultiGraph):
    street_nodes = get_nodes_by_attribute(G, "tidy", "address")
    for sn in street_nodes:
        raw = G.nodes[sn].get("label", sn)
        label = get_label(raw)
        G.nodes[sn]["label"] = label
    return G 


def extract_street_parts(G:nx.MultiGraph):
    street_nodes = get_nodes_by_attribute(G, "tidy", "address")
    records = []
    for n in street_nodes:
        try:         
            street = G.nodes[n]['label']
            tags = usaddress.tag(street.upper())
            records.append({"node_id": n, **tags[0]})
        except Exception as e:
            print(G.nodes[n])
            continue
    return pd.DataFrame(records).fillna('')


def get_graph_ids(G):
    return {
        "file_numbers": [ n for n in G.nodes if n[:1] in ['C', 'L']],
        "address_ids":  [ n for n in G.nodes if n[:1] == "A" ],
        "name_ids":     [ n for n in G.nodes if n[:1] == "N" ]
    }

def get_ilsos_node(G, nodes:list):
    for n in nodes:
        if n in G:
            if G.nodes[n]['data_source'] == "il_sos":
                return n 

def combine_nodes(G, nodes:list):
    ilsos_node = get_ilsos_node(G, nodes)
    keep_node = nodes[0] if ilsos_node is None else ilsos_node
    merge_data = {}
    
    for n in nodes:
        if n in G.nodes and n != keep_node:
            merge_data[n] = G.nodes[n]
            G = nx.identified_nodes(G, keep_node, n)
            
    if keep_node in G:
        G.nodes[keep_node]['alias_ids'] = nodes
        md = G.nodes[keep_node]['merge_data'] if "merge_data" in G.nodes[keep_node].keys() else {}
        G.nodes[keep_node]['merge_data'] = { **md, **merge_data}
        
    return G 


def get_node_names(G)->dict:
    node_names = {} 
    for n in G.nodes:
        name = G.nodes[n].get("label", n)
        node_names[name] = n
    
    return node_names


def tidy_up(G):
    G = clean_streets(G)
    nf, cnf = extract_name_parts(G)
    sr = extract_street_parts(G)
    
    nd = get_probable_duplicates(nf, ['GivenName', 'Surname', 'SuffixGenerational']) 
    cnd = get_probable_duplicates(cnf, ['company_name'])
    sd = get_probable_duplicates(sr, ['AddressNumber', 'StreetName', 'OccupancyIdentifier'])
    duplicates = nd + cnd + sd 
    
    for d in duplicates:
        G = combine_nodes(G, d)
    return G    


def tidy_up_companies(G):
    nf, cnf = extract_name_parts(G)
    cnd = get_probable_duplicates(cnf, ['company_name'])
    for d in cnd:
        G = combine_nodes(G, d)
    return G    


def get_probable_duplicates(df, grouping):
    if len(df) == 0:
        return []
    else:
        grouping = [g for g in grouping if g in df.columns]
        probable_duplicates = (
            df
            .reset_index()
            .groupby(grouping)
            .agg({"node_id": ";".join, "index":"count"})
            .pipe(lambda df: df[df['index'] > 1])
        )
        return [pd.split(';') for pd in list(probable_duplicates.node_id)]


def combine_entitity_list(entity_lists:list):
    
    combined = entity_lists.pop()
    for e_list in entity_lists:
        ids = list(set([c.id for c in combined]))
        combined = combined + [e for e in e_list if e.id not in ids]
    
    return combined


def clean_columns(df:pd.DataFrame)->pd.DataFrame:
    lowercase = { 
        c: c.lower().strip().replace(' ', '_') 
        for c in df.columns }
    df = df.rename(columns=lowercase)
    return df


def get_nodes_by_attribute(G: nx.MultiGraph, key:str, filter_value:str) -> list:
    node_attributes = G.nodes(data=key, default = None)
    return [ n[0] for n in node_attributes if n[1] == filter_value ]


def get_colors(G):
    node_reserved = {
        "company": "black", 
        "address": "#f9cf13",
        "name": "#dd0f04",
    }
    edge_reserved = {
        "manager": "#e515ed",
        "agent": "#00c3dd",  
        "address": "#adadad",
        "company": "black", 
        "president": "#7a15ed", 
        "secretary": "#2937f4" 
    }
    colors = [
        '#1b9e77',
        '#d95f02',
        '#7570b3',
        '#e7298a',
        '#66a61e',
        '#e6ab02',
        '#a6761d',
        '#666666',
        '#666666',
        '#666666'
        ]
    
    node_types = [t for t in set(dict(G.nodes(data="type", default=None)).values()) if t is not None]
    edge_types = set()
    for (u, v, k, c) in G.edges(data='type', keys=True, default=None):
        if c is not None:
            edge_types.add(c)
    edge_types = list(edge_types)
    node_colors = get_colormap(node_types, colors, node_reserved)
    edge_colors = get_colormap(edge_types, colors, edge_reserved)
    return node_colors, edge_colors 


def get_colormap(types, colors, reserved):
    colormap = {}
    for count, t in enumerate(types):
        colormap[t] = reserved.get(t, colors[count])
    return colormap 


def deduplicate_edges(G):
    records = [ {"source": edge[0], "target": edge[1], **edge[2]} for edge in G.edges(data=True) ]
    df = pd.DataFrame(records).drop_duplicates()
    source = df.source
    target = df.target
    attr = df.drop(columns=["source", "target"]).to_dict('records')
    G.clear_edges()
    G.add_edges_from(zip(source, target, attr))
    return G    





### DATA PORTAL FUNCTIONS

def search_data_set(resource, keyword):
    results = client.get(resource, q=keyword)
    return results

def format_address_search(row:dict):
    parts = ['AddressNumber', 'StreetNamePreDirectional', 'StreetName', 'OccupancyType', 'OccupancyIdentifier']
    search_parts = []
    for p in parts:
        if p in row and row[p] != "":
            search_parts.append(row[p].replace('#', "").strip())
            
    search = " ".join(search_parts)
    return search 


def get_street_searches(streets:pd.DataFrame):
    return (streets
                .assign(search = lambda df: df.apply(lambda row: format_address_search(row), axis=1))
                .pipe(lambda df: df[df['search'] != ""])
                [['node_id', 'search']]
                .groupby('search')['node_id'].apply(list)
                .reset_index()
            ).to_dict('records')
    
    
def search_data_portal(keywords:list, resource_id:str = 'rsxa-ify5', prefix:str = "CONTRACT"):
    results = []
    for k in keywords:
        result = search_data_set(resource_id, k['search'])
        for row in result:
            row['node_id'] = k['node_id']
            row['result_id'] = f"{prefix}-{row['purchase_order_contract_number']}-{row['revision_number']}"
            results.append(row)
    if len(results) > 0:
        return pd.DataFrame(results).explode('node_id')
    else:
        return pd.DataFrame()

    
def get_connected_nodes(G, node, nbrhood:dict = {}) -> dict:
    if node in G:
        nbrs = nx.neighbors(G, node)
        nbrhood[node] = nbrs
        for n in nbrs:
            if n not in nbrhood:
                nbrhood.update(get_connected_nodes(G, n, nbrhood))
        return nbrhood
    else:
        return nbrhood 


### Path graph    
def get_path_graph(G, node_1, node_2):
    path_nodes = set()
    shortest_paths = list(nx.all_shortest_paths(G, node_1, node_2))
    for path in shortest_paths:
        path_nodes.update(path)
    return nx.induced_subgraph(G, list(path_nodes))