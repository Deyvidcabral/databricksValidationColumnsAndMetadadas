from dataclasses import dataclass
from typing import List, Dict, Any

@dataclass
class Field:
    name: str
    dataType: type

class DataType:
    """
    Mock para tipos de dados do Spark, suportando tipos primitivos e complexos
    """
    def __init__(self, type_str: str = "string"):
        self._type = type_str.lower()
        
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
            "binary": "binary"
        }
        
    def simpleString(self) -> str:
        """Retorna o tipo normalizado do Spark"""
        return self._type_mapping.get(self._type, self._type)
        
    def typeName(self) -> str:
        """Retorna o nome do tipo para compatibilidade com Spark"""
        return self.simpleString()

    @staticmethod
    def create_mock_fields() -> List[Field]:
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

class MockSchema:
    def __init__(self, fields: List[Field]):
        self.fields = fields

class MockDataFrame:
    def __init__(self, schema: MockSchema):
        self.schema = schema

class MockSpark:
    def __init__(self, mock_tables: Dict[str, List[Dict[str, Any]]] = None):
        """
        Inicializa o MockSpark com tabelas simuladas
        
        Args:
            mock_tables: Dicionário com nome da tabela -> lista de dicionários com dados
        """
        self.mock_tables = mock_tables or {}
        
    def table(self, name: str) -> MockDataFrame:
        """
        Simula a leitura de uma tabela do Spark
        
        Args:
            name: Nome da tabela
            
        Returns:
            MockDataFrame com estrutura de colunas simulada
            
        Raises:
            Exception: Se a tabela não existir no mock
        """
        # Se a tabela existe no mock_tables, usa sua estrutura
        if name in self.mock_tables:
            table_data = self.mock_tables[name]
            if table_data:
                # Pega as colunas do primeiro registro
                columns = list(table_data[0].keys())
                fields = []
                
                # Tenta inferir tipos das colunas baseado nos valores
                for col in columns:
                    value = table_data[0][col]
                    if isinstance(value, bool):
                        fields.append(Field(name=col, dataType=DataType("boolean")))
                    elif isinstance(value, int):
                        fields.append(Field(name=col, dataType=DataType("integer")))
                    elif isinstance(value, float):
                        fields.append(Field(name=col, dataType=DataType("double")))
                    elif isinstance(value, dict):
                        fields.append(Field(name=col, dataType=DataType("struct")))
                    elif isinstance(value, list):
                        fields.append(Field(name=col, dataType=DataType("array")))
                    else:
                        fields.append(Field(name=col, dataType=DataType("string")))
                        
                return MockDataFrame(MockSchema(fields))
        
        # Se a tabela não existe, lança uma exceção
        available_tables = list(self.mock_tables.keys())
        raise Exception(
            f"Tabela '{name}' não encontrada no mock. "
            f"Tabelas disponíveis: {', '.join(available_tables)}"
        )
    
    def sql(self, query: str) -> None:
        """Simula execução de queries SQL"""
        print(f"[MOCK] Executando query: {query}")
