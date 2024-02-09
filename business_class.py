import msgspec 

class ForeignKey(msgspec.Struct):
    label: str 
    value: int         

class Entity(msgspec.Struct):
    """A person or company with some relationship to a specific company"""
    id: int 
    file_number : str 
    type : str 
    name_id : ForeignKey
    address_id : ForeignKey
    
    def node(self):
        params = {}
        match self.type:
            case "president" | "secretary" | "agent" | "manager":
                params["id"] = f"N{self.name_id['value']}"
                params["label"] = self.name_id['label']
                
    # def links(self):
    #     return [(self.)]
    def label(self): 
        if self.name_id:
            return getattr(self, 'name_id')['label']
    
    def simplify(self):
        name = self.name_id['label'] if self.name_id else ""
        address = self.address_id['label'] if self.address_id else ""
        td = {
            "id": self.id, 
            "file_number": self.file_number, 
            "type": self.type, 
            "name": name, 
            "address": address
        }
        return td 



class Node(msgspec.Struct):
    id : str 
    label : str 
    type : str 
    source : str = "il_sos"
    attr : dict = {}
    
class Address(msgspec.Struct):
    """An address tied to a person or company with some relationship to a specific company"""
    file_number : str 
    name : str
    type: str 
    node_id : str
    linked_node_id : str 
    label: str 
    street_id : int 
    city : str 
    state : str 
