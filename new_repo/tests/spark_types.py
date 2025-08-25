from dataclasses import dataclass
from typing import Any

@dataclass
class Field:
    name: str
    dataType: Any

class DataType:
    """
    Mock para tipos de dados do Spark, suportando tipos primitivos e complexos
    """
    def __init__(self, type_str: str = "string", element_type: 'DataType' = None):
        self._type = type_str.lower()
        self._element_type = element_type
        
        # Mapeia tipos do YAML para tipos do Spark
        self._type_mapping = {
            # Tipos primitivos
            "string": "string",
            "str": "string",
            "varchar": "string",
            "char": "string",
            "text": "string",
            
            "int": "integer",
            "integer": "integer",
            "long": "long",
            "bigint": "long",
            
            "float": "float",
            "double": "double",
            "decimal": "decimal",
            
            "bool": "boolean",
            "boolean": "boolean",
            
            "date": "date",
            "timestamp": "timestamp",
            "datetime": "timestamp",
            
            # Tipos complexos
            "array": "array",
            "map": "map",
            "struct": "struct",
            "binary": "binary",

            # Comparações especiais
            "map<string,string>": "map",
            "array<string>": "array",
            "struct<fields:array<struct<name:string,value:string>>>": "struct"
        }
        
    def simpleString(self) -> str:
        """Retorna o tipo normalizado do Spark"""
        base_type = self._type_mapping.get(self._type, self._type)
        if self._element_type and base_type in ["array", "map"]:
            return f"{base_type}<{self._element_type.simpleString()}>"
        return base_type
        
    def typeName(self) -> str:
        """Retorna o nome do tipo para compatibilidade com Spark"""
        return self.simpleString()
        
    def __eq__(self, other):
        """Compara dois tipos do Spark"""
        if not isinstance(other, DataType):
            return False
            
        if self._type == "struct" and other._type == "struct":
            return True  # Consideramos structs como compatíveis
        if self._type == "array" and other._type == "array":
            return True  # Consideramos arrays como compatíveis
        if self._type == "map" and other._type == "map":
            return True  # Consideramos maps como compatíveis
            
        return self.simpleString() == other.simpleString()

    @staticmethod
    def create_mock_fields() -> list['Field']:
        """
        Cria uma lista de campos com diferentes tipos para testes
        """
        return [
            # Strings
            Field(name="col_string", dataType=DataType("string")),
            Field(name="col_varchar", dataType=DataType("varchar")),
            
            # Números
            Field(name="col_int", dataType=DataType("integer")),
            Field(name="col_long", dataType=DataType("bigint")),
            Field(name="col_float", dataType=DataType("float")),
            Field(name="col_double", dataType=DataType("double")),
            Field(name="col_decimal", dataType=DataType("decimal")),
            
            # Boolean
            Field(name="col_boolean", dataType=DataType("boolean")),
            
            # Data/Hora
            Field(name="col_date", dataType=DataType("date")),
            Field(name="col_timestamp", dataType=DataType("timestamp")),
            
            # Tipos complexos
            Field(name="col_array", dataType=DataType("array")),
            Field(name="col_map", dataType=DataType("map")),
            Field(name="col_struct", dataType=DataType("struct")),
            Field(name="col_binary", dataType=DataType("binary"))
        ]
