from string import Template
import yaml
from typing import Dict, Any, Tuple

def process_yaml_file(filepath: str, variables: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Processa um arquivo YAML, substituindo variáveis
    
    Args:
        filepath: Caminho do arquivo YAML
        variables: Dicionário com variáveis para substituição
        
    Returns:
        Dados do YAML processado
    """
    with open(filepath, "r", encoding="utf-8") as f:
        yaml_text = f.read()
    
    # Substitui variáveis se fornecidas
    if variables:
        yaml_text = Template(yaml_text).safe_substitute(variables)
    
    return yaml.safe_load(yaml_text)

from dataclasses import dataclass
from typing import List

@dataclass
class ColumnInfo:
    name: str
    type: str
    required: bool
    metadata: Dict[str, Any]

@dataclass
class TableValidationResult:
    missing_required: List[str]  # Colunas required que faltam
    type_mismatches: List[tuple]  # Tuplas (coluna, tipo_esperado, tipo_real)
    missing_optional: List[str]  # Colunas opcionais que faltam
    table_schema: Dict[str, str] = None  # Schema atual da tabela

def get_table_metadata(table_info: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Dict], Dict[str, str], Dict[str, ColumnInfo]]:
    """
    Extrai os metadados de uma tabela do YAML
    
    Args:
        table_info: Informações da tabela do YAML
        
    Returns:
        Tupla com:
        - table_path: Caminho da tabela
        - tag_dict: Dicionário com tags
        - fields_description: Dicionário com descrição dos campos
        - fields_types: Dicionário com tipos dos campos
        - columns_info: Dicionário com informações das colunas, indexado por nome
    """
    table_path = table_info.get("table_path")
    
    # Extrai metadados da tabela (serão usados como tags)
    tabela_metadata = table_info.get("metadata", {})
    
    # Monta dicionários de metadados e tipos das colunas
    colunas = table_info.get("columns", [])
    
    # Processa informações das colunas
    columns_info = {}
    fields_description = {}
    fields_types = {}
    
    for col in colunas:
        name = col.get("name", "sem_nome")
        column_info = ColumnInfo(
            name=name,
            type=col.get("type", "").lower(),
            required=col.get("required", False),
            metadata=col.get("metadata", {})
        )
        columns_info[name] = column_info
        
        # Extrair apenas a descrição do metadata para o fields_description
        fields_description[name] = col.get("metadata", {}).get("descricao", "")
        fields_types[name] = column_info.type
    
    return table_path, tabela_metadata, fields_description, fields_types, columns_info
