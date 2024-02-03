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
    
    def simplify(self):
        td = {}
        for f in self.__struct_fields__:
            if f in ['name_id', 'address_id']:
                td[f.split('_')[0]] = getattr(self,f)['label']
            else: 
                td[f] = getattr(self, f)
        return td 
    
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
