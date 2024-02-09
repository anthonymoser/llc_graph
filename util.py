import networkx as nx
import msgspec
import pandas as pd
import usaddress 
import probablepeople as pp 
import requests 
import msgspec 
import json
from business_class import Entity

endpoint = "https://companies-mvwuoztvlq-uc.a.run.app"

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

def get_alias_ids(G, nodes:list):
    full_list = []
    for n in nodes:
        try:
            full_list += G.nodes[n]['alias_ids']
        except Exception as e:
            full_list.append(n)
            
    return full_list 


def get_entities(field, search_values):
    url = f"{endpoint}/companies/entities.json?_labels=on&_shape=array&{field}__in={json.dumps(search_values)}"
    response = requests.get(url)
    data = msgspec.json.decode(response.content)
    return [ Entity(**d) for d in data ]


def get_name_ids(search_value):
    url = f"{endpoint}/companies/names.json?_shape=array&name__like={search_value}"
    response = requests.get(url)
    data = msgspec.json.decode(response.content)
    return [d['id'] for d in data]



def get_address_ids(search_value): 
    url = f"{endpoint}/companies/addresses.json?_shape=array&street__like={search_value}"
    response = requests.get(url)
    data = msgspec.json.decode(response.content)
    return [d['id'] for d in data]


def get_entities_by_file_number(search_value):
    search_value = "%" + search_value + "%"
    url = f"{endpoint}/companies/entities.json?_labels=on&_shape=array&file_number__like={search_value}"
    response = requests.get(url)
    data = msgspec.json.decode(response.content)
    return [ Entity(**d) for d in data ]


def add_address(G, entity, node_id):   
    address_node_id = f"A{entity.address_id['value']}"
    address_node_label = entity.address_id['label']
    if address_node_id not in G:
        G.add_node(address_node_id, **{"label": address_node_label, "type": "address"})
    
    # Don't create duplicate edges between nodes                
    try:
        edge_types = [G[node_id][address_node_id][edge]['type'] for edge in G[node_id][address_node_id]]
        if entity.type not in edge_types:    
            G.add_edge(node_id, address_node_id, **{"type": entity.type})
    except KeyError:
            G.add_edge(node_id, address_node_id, **{"type": entity.type})
    return G 


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
    entities += get_entities('file_number', file_numbers)
    entities += get_entities('address_id', address_ids)
    return entities


def graph_entity(G, entity):
    
    match entity.type:
        
        case "president" | "secretary" | "agent" | "manager":
            node_id = f"N{entity.name_id['value']}"
            node_label = entity.name_id['label']
            if node_id in G.nodes and 'label' not in G.nodes[node_id].keys():
                G.nodes[node_id]['label'] = node_label 
            else:
                G.add_node(node_id, **{"label": node_label, "type": "person"})
                
            # Don't create duplicate edges between nodes                
            try:
                edge_types = [G[node_id][entity.file_number][edge]['type'] for edge in G[node_id][entity.file_number]]
                if entity.type not in edge_types:    
                    G.add_edge(node_id, entity.file_number, **{"type": entity.type})
            except KeyError:
                 G.add_edge(node_id, entity.file_number, **{"type": entity.type})
            
            if entity.address_id:
                add_address(G, entity, node_id)
                
        case "company": 
            node_id = entity.file_number
            node_label = entity.name_id['label']
            G.add_node(node_id, **{"label": node_label, "type": "company"})
            
            if entity.address_id:
                add_address(G, entity, node_id)
            
        case str(x) if "company" in x and x != "company":
            node_id = f"C{entity.id}"
            node_label = entity.name_id['label']
            G.add_node(node_id, **{"label": node_label, "type": "company"})
            G.add_edge(node_id, entity.file_number, **{"type": entity.type})
            
            if entity.address_id:
                add_address(G, entity, node_id)
        case _: 
            print("no entity match")    
            print(entity)
            
    return G 
    

def graph_entities(G:nx.MultiGraph, entities:list) ->nx.MultiGraph: 
    for e in entities:
        if e.label() not in ["SAME", "NONE"]:
            G = graph_entity(G, e)
    return G 


def fix_unlabeled_nodes(G):
    unlabeled =  [n for n in G.nodes if 'label' not in G.nodes[n].keys()]
    # print(len(unlabeled))
    for u in unlabeled:
        graph_entities(G, get_entities('file_number', [u]))
    return G 

     
def expand_graph(G:nx.MultiGraph, node_list = []) ->nx.MultiGraph:
    entities = []
    node_ids = []
    if len(node_list) == 0:
        print("expanding all nodes")
        gids = get_graph_ids(G)
        for g in gids:
            gids[g] = get_alias_ids(G, gids[g])
            node_ids += gids[g]
            # entities += expand_nodes(gids[g])
    else: 
        print("expanding nodes", node_list)
        node_ids = get_alias_ids(G, node_list)
        
    entities = expand_nodes(list(set(node_ids)))        
    # G = graph_entities(G, entities)
    # G = fix_unlabeled_nodes(G)
    return entities


def extract_name_parts(G:nx.MultiGraph):
    name_parts = {}
    for n in G.nodes:
        try:
            node = G.nodes[n]
            if node['type'] != "address":
                name = G.nodes[n]['label'].replace('.', '').strip()
                if name[-5:] == " SAME":
                    name = name.replace(" SAME", "")
                parts = pp.parse(name)
                name_parts[n] = parts
        except Exception as e:
            print(e, n)
            continue
    
    records = []
    for n in name_parts:
        parts = name_parts[n]
        record = {part[1]: part[0].replace(',', '').replace('.', '') for part in parts}
        record["node_id"] = n
        if "CorporationName" not in record.keys():
            records.append(record)
    
    return pd.DataFrame(records).fillna('')


def extract_street_parts(G:nx.MultiGraph):
    records = []
    for n in G.nodes:
        try:
            if G.nodes[n]['type'] == "address":         
                street = G.nodes[n]['label']
                tags = usaddress.tag(street)
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
     

def combine_nodes(G, nodes:list):
    keep_node = nodes[0]
    for n in nodes:
        if n in G.nodes and n != keep_node:
            G = nx.identified_nodes(G, keep_node, n)
    G.nodes[keep_node]['alias_ids'] = nodes
    return G 


def tidy_up(G, ignore_middle_initial = True):
    G = fix_unlabeled_nodes(G)
    nf = extract_name_parts(G)
    name_grouping = ['GivenName', 'Surname', 'SuffixGenerational'] if ignore_middle_initial else ['GivenName', 'MiddleInitial', 'Surname', 'SuffixGenerational']
    # nf.to_csv('nf.csv', index=False)
    
    sr = extract_street_parts(G)
    street_grouping = ['AddressNumber', 'StreetName']
    # sr.to_csv('sr.to_csv', index=False)
    
    
    nd = get_probable_duplicates(nf, name_grouping) if len(nf) > 0 else []
    sd = get_probable_duplicates(sr, street_grouping) if len(sr) > 0 else []
    duplicates = nd + sd
    for d in duplicates:
        # print(d)
        G = combine_nodes(G, d)
    return G     


def get_probable_duplicates(df, grouping):
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
    # df = df.convert_dtypes()
    lowercase = { 
        c: c.lower().strip().replace(' ', '_') 
        for c in df.columns }
    df = df.rename(columns=lowercase)
    return df

