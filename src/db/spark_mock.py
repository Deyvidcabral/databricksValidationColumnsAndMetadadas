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
    def __init__(self, fields: List[Field]):
        self.schema = MockSchema(fields)

class MockSpark:
    """Mock para simular uma sessão Spark"""
    
    def __init__(self, mock_tables: Dict[str, List[Dict[str, Any]]] = None):
        """
        Inicializa o MockSpark com tabelas simuladas
        
        Args:
            mock_tables: Dicionário com nome da tabela -> lista de dicionários com dados
        """
        self.mock_tables = mock_tables or {}
        self._table_comments = {}
        self._column_comments = {}
        self._table_properties = {}
    
    def table(self, name: str) -> "MockDataFrame":
        """
        Simula a leitura de uma tabela do Spark
        
        Args:
            name: Nome da tabela
            
        Returns:
            MockDataFrame com estrutura de colunas simulada
        """
        # Se não há dados mockados ou a tabela não existe
        if not self.mock_tables or name not in self.mock_tables:
            raise ValueError(f"Tabela não encontrada: {name}")
            
        # Se a tabela existe mas está vazia
        table_data = self.mock_tables[name]
        if not table_data:
            raise ValueError(f"Tabela não encontrada: {name}")
            
        # Caso contrário, retorna DataFrame com schema inferido dos dados
        sample_record = table_data[0]
        fields = []
        
        for col_name, value in sample_record.items():
            field_type = self._infer_type(value)
            fields.append(Field(name=col_name, dataType=field_type))
            
        return MockDataFrame(fields)
        
    def sql(self, query: str):
        """Simula execução de queries SQL"""
        query = query.strip().lower()
        
        if "comment on column" in query:
            # Extrai nome da tabela e coluna
            parts = query.split('`')
            if len(parts) >= 2:
                column_path = parts[1]
                table_name = '.'.join(column_path.split('.')[:-1])
                column_name = column_path.split('.')[-1]
                comment = parts[-1].strip().strip("'")
                
                if table_name not in self._column_comments:
                    self._column_comments[table_name] = {}
                self._column_comments[table_name][column_name] = comment
                
                print(f"[MOCK] Executando query: {query}")
                print(f"[OK] 🟢 Comentário adicionado à coluna '{column_name}'")
                
        elif "alter table" in query and "set tblproperties" in query:
            # Extrai nome da tabela e propriedades
            table_name = query.split('alter table')[1].split('set')[0].strip()
            properties_str = query.split('tblproperties')[1].strip('() ')
            
            # Parse das propriedades
            self._table_properties[table_name] = {}
            props = properties_str.split(',')
            for prop in props:
                if '=' in prop:
                    key, value = prop.split('=')
                    key = key.strip().strip("'")
                    value = value.strip().strip("'")
                    self._table_properties[table_name][key] = value
                    
            print(f"[MOCK] Executando query: {query}")
            print(f"[OK] 🟢 Tags adicionadas à tabela '{table_name}'")
            
        return self
        
    def _infer_type(self, value: Any) -> DataType:
        """
        Infere o tipo Spark a partir de um valor Python
        
        Args:
            value: Valor Python para inferir o tipo
            
        Returns:
            DataType correspondente ao tipo Spark
        """
        if isinstance(value, bool):
            return DataType("boolean")
        elif isinstance(value, int):
            if value > 2**31:
                return DataType("long")
            return DataType("integer")
        elif isinstance(value, float):
            return DataType("double")
        elif isinstance(value, list):
            # Para arrays, retorna um array do tipo do primeiro elemento
            if value:
                element_type = self._infer_type(value[0])
                return DataType("array")
            return DataType("array")
        elif isinstance(value, dict):
            # Para dicts, verifica se é um campo struct com tipo específico
            if "type" in value and value["type"] in ["struct", "array", "map"]:
                return DataType(value["type"])
            return DataType("struct")
        elif isinstance(value, bytes):
            return DataType("binary")
        elif isinstance(value, str):
            # Verifica se é uma string de timestamp em formato ISO
            if 'T' in value and value.endswith('Z'):
                return DataType("timestamp")
            return DataType("string")
        else:
            return DataType("string")
