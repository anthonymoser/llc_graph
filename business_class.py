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
                
    def link_dict(self):
        prohibited_links = ["SAME"]
        return {
            "id": self.id, 
            "file_number": self.file_number, 
            "type": self.type, 
            "dg_type": self.type,
            "data_source": "il_sos",
            "name_id": None if self.name_id['label'] in prohibited_links else f"N{self.name_id['value']}",
            "name": None if self.name_id['label'] in prohibited_links else self.name_id['label'],
            "address_id": f"A{self.address_id['value']}" if self.address_id else None,
            "address": self.address_id['label'] if self.address_id else None
        }
    
    def address_dict(self):
        if self.address_id is not None:
            return {
                "id": self.id,  
                "type": "address", 
                "dg_type": "address",
                "data_source": "il_sos",
                "address_id": f"A{self.address_id['value']}",
                "address": self.address_id['label']
            }
        else:
            return {}
        
    def name_dict(self):
        prohibited_names = ["SAME"]
        if self.name_id['label'] not in prohibited_names:
            return {
                    "id": self.id,  
                    "type": "person", 
                    "dg_type": "person",
                    "data_source": "il_sos",
                    "name_id": f"N{self.name_id['value']}",
                    "name": self.name_id['label']
                }
        else:
            return {}
    
    def company_dict(self):
        if self.type == "company":
            return {
                    "id": self.id, 
                    "file_number": self.file_number, 
                    "type": "company", 
                    "dg_type": "company",
                    "data_source": "il_sos",
                    "name_id": f"N{self.name_id['value']}",
                    "name": self.name_id['label']
                }
        else:
            return {}
        
        
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