import msgspec
import networkx as nx 
from typing import Optional

class QNG(msgspec.Struct):
    adjacency: dict
    node_attrs: dict
    
    def multigraph(self):
        MG = nx.from_dict_of_dicts(self.adjacency, multigraph_input=True, create_using=nx.MultiGraph)
        nx.set_node_attributes(MG, self.node_attrs)
        return MG 
    
class Element(msgspec.Struct):
    type : str 
    value : str 


class Node(msgspec.Struct):
    id : str 
    label : str 
    type : str 
    source : str = "il_sos"
    attr : dict = {}
    
    def nx_format(self):
        return (self.id, {"label": self.label, "type": self.type, "source": self.source, **self.attr})

        
class NodeFactory(msgspec.Struct, kw_only=True):
    id_field : str
    label_field : Optional[str | None] = None
    type : Optional[Element]
    source : str = "" 
    attr : list[str] = []
    
    def make_node(self, data:dict):
        return Node(**{
            "id": str(data.get(self.id_field)),
            "label": str(data.get(self.label_field)) if self.label_field else str(data.get(self.id_field)),
            "type": data.get(self.type.value) if self.type.type == "field" else self.type.value,
            "source": self.source,
            "attr": {a: data.get(a, None) for a in self.attr}
        })


class Link(msgspec.Struct):
    source : str 
    target : str 
    type : str 
    attr : dict = {}
    
    def nx_format(self) -> tuple:
        return (str(self.source), str(self.target), {"type": self.type, **self.attr})

    
class LinkFactory(msgspec.Struct):
    source_field : str 
    target_field: str 
    type : Optional[Element]
    attr : list[str] = []
    
    def make_link(self, data:dict):
        return(Link(**{
            "source": data.get(self.source_field), 
            "target": data.get(self.target_field), 
            "type":  data.get(self.type.value) if self.type.type == "field" else self.type.value,
            "attr": {a: data.get(a, None) for a in self.attr}
        }))


class GraphFactory(msgspec.Struct):
    node_factories : list[NodeFactory]
    link_factories : list[LinkFactory]
    
    def make_nodes(self, data:dict):
        return [ nf.make_node(data) for nf in self.node_factories ]
    
    def nx_nodes(self, data:dict):
        nodes = self.make_nodes(data)
        return [ node.nx_format() for node in nodes]
    
    def make_links(self, data:dict):
        return [ lf.make_link(data) for lf in self.link_factories ]
    
    def nx_edges(self, data:dict):
        links = self.make_links(data)
        return [ link.nx_format() for link in links]
    
    def make_graph(self, data:dict):
        G = nx.MultiGraph()
        G.add_nodes_from(self.nx_nodes(data))
        G.add_edges_from(self.nx_edges(data))
        return G 
