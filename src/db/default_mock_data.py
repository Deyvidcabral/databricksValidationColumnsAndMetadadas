from typing import Dict, Any
from .spark_types import DataType, Field

def get_default_mock_tables() -> Dict[str, Dict[str, Any]]:
    """
    Retorna um conjunto de tabelas mock padrão para testes
    """
    return {
        "catalogo.schema.valid_table": {
            "name": "valid_table",
            "catalog": "catalogo",
            "schema": "schema",
            "fields": [
                Field(name="id", dataType=DataType("integer")),
                Field(name="name", dataType=DataType("string")),
                Field(name="active", dataType=DataType("boolean")),
                Field(name="value", dataType=DataType("integer")),
                Field(name="created_at", dataType=DataType("timestamp")),
                Field(name="metadata", dataType=DataType("struct"))
            ],
            "description": "Tabela de exemplo para testes",
            "properties": {
                "product": "TACIA",
                "data_owner": "teste@exemplo.com",
                "nivel_compartilhamento": "publico"
            }
        },
        "catalogo.schema.types_table": {
            "name": "types_table",
            "catalog": "catalogo",
            "schema": "schema",
            "fields": [
                # Tipos primitivos
                Field(name="string_col", dataType=DataType("string")),
                Field(name="int_col", dataType=DataType("integer")),
                Field(name="long_col", dataType=DataType("long")),
                Field(name="float_col", dataType=DataType("float")),
                Field(name="double_col", dataType=DataType("double")),
                Field(name="decimal_col", dataType=DataType("decimal")),
                Field(name="bool_col", dataType=DataType("boolean")),
                Field(name="date_col", dataType=DataType("date")),
                Field(name="timestamp_col", dataType=DataType("timestamp")),
                # Tipos complexos
                Field(name="array_col", dataType=DataType("array", DataType("string"))),
                Field(name="map_col", dataType=DataType("map", DataType("string"))),
                Field(name="struct_col", dataType=DataType("struct"))
            ],
            "description": "Tabela com diversos tipos de dados para teste",
            "properties": {
                "product": "TACIA",
                "data_owner": "teste@exemplo.com",
                "nivel_compartilhamento": "publico"
            }
        }
    }
