from typing import Dict, Optional, List, Set, Any
from dataclasses import dataclass
from src.utils.yaml_processor import ColumnInfo, TableValidationResult

@dataclass
class ProcessResult:
    """Resultado do processamento de uma tabela"""
    table_name: str
    validation: Optional[TableValidationResult]
    success: bool = True
    error_message: str = ""

class MetadataValidationError(Exception):
    """Exceção personalizada para erros de validação de metadados"""
    def __init__(self, error_results: List[ProcessResult]):
        self.error_results = error_results
        error_messages = []
        
        for result in error_results:
            msg_parts = []
            msg_parts.append(f"Tabela '{result.table_name}':")
            
            # Adiciona mensagem de erro específica se houver
            if result.error_message:
                msg_parts.append(f"  - {result.error_message}")
            
            # Adiciona informações de validação se disponíveis
            if result.validation:
                if result.validation.missing_required:
                    msg_parts.append("  - Colunas obrigatórias faltando: " + 
                                   ", ".join(result.validation.missing_required))
                if result.validation.type_mismatches:
                    mismatches = [f"{col} (esperado: {exp}, encontrado: {real})" 
                                for col, exp, real in result.validation.type_mismatches]
                    msg_parts.append("  - Incompatibilidade de tipos: " + 
                                   ", ".join(mismatches))
                if result.validation.missing_optional:
                    msg_parts.append("  - Colunas opcionais faltando: " + 
                                   ", ".join(result.validation.missing_optional))
            
            error_messages.append("\n".join(msg_parts))
        
        super().__init__("\n\n".join(error_messages))

class MetadataManager:
    """Gerencia metadados das tabelas usando Spark e YAML"""
    
    def __init__(self, spark):
        self.spark = spark
        self._metadata_cache: Dict[str, Dict[str, ColumnInfo]] = {}

    def process_table(self, table_name: str, template_metadata: Dict[str, ColumnInfo]) -> ProcessResult:
        """
        Processa uma tabela verificando se ela corresponde ao template.
        
        Args:
            table_name: Nome da tabela para processar
            template_metadata: Dicionário com metadados do template
            
        Returns:
            ProcessResult com o resultado do processamento
        """
        try:
            # Verifica se a tabela existe
            exists, message = self._table_exists(table_name)
            if not exists:
                return ProcessResult(
                    table_name=table_name,
                    validation=None,
                    success=False,
                    error_message=f"Tabela não encontrada: {message}"
                )

            # Obtém o schema da tabela
            table_schema = self._get_table_schema(table_name)
            if not table_schema:
                return ProcessResult(
                    table_name=table_name,
                    validation=None,
                    success=False,
                    error_message=f"Não foi possível obter o schema da tabela '{table_name}'"
                )

            # Valida o schema contra o template
            validation_result = self._validate_schema(table_schema, template_metadata)
            
            success = (not validation_result.missing_required and 
                      not validation_result.type_mismatches)
            
            return ProcessResult(
                table_name=table_name,
                validation=validation_result,
                success=success
            )

        except Exception as e:
            return ProcessResult(
                table_name=table_name,
                validation=None,
                success=False,
                error_message=f"Erro ao processar tabela: {str(e)}"
            )

    def process_tables(self, tables_metadata: Dict[str, Dict[str, ColumnInfo]]) -> List[ProcessResult]:
        """
        Processa múltiplas tabelas verificando se correspondem aos templates.
        
        Args:
            tables_metadata: Dicionário com metadados das tabelas
            
        Returns:
            Lista de ProcessResult com os resultados
            
        Raises:
            MetadataValidationError: Se houver erros de validação
        """
        results = []
        for table_name, template_metadata in tables_metadata.items():
            result = self.process_table(table_name, template_metadata)
            results.append(result)
            
        # Filtra resultados com erro
        error_results = [r for r in results if not r.success]
        if error_results:
            raise MetadataValidationError(error_results)
            
        return results

    def _table_exists(self, table_name: str) -> tuple[bool, str]:
        """
        Verifica se uma tabela existe
        
        Returns:
            Tupla (existe: bool, mensagem: str)
        """
        try:
            self.spark.table(table_name)
            return True, f"Tabela '{table_name}' encontrada"
        except Exception as e:
            return False, str(e)

    def _get_table_schema(self, table_name: str) -> Dict[str, str]:
        """
        Obtém o schema de uma tabela.
        
        Returns:
            Dicionário com nome da coluna -> tipo
        """
        try:
            df = self.spark.table(table_name)
            if not df or not hasattr(df, 'schema') or not df.schema or not df.schema.fields:
                return {}
            
            schema = {}
            for field in df.schema.fields:
                if hasattr(field, 'name') and hasattr(field.dataType, 'typeName'):
                    schema[field.name] = field.dataType.typeName()
            return schema
            
        except Exception as e:
            print(f"Erro ao obter schema: {str(e)}")
            return {}

    def _validate_schema(self, 
                        table_schema: Dict[str, str],
                        template_metadata: Dict[str, ColumnInfo]) -> TableValidationResult:
        """
        Valida o schema de uma tabela contra o template.
        
        Args:
            table_schema: Schema atual da tabela
            template_metadata: Metadados do template
            
        Returns:
            TableValidationResult com o resultado da validação
        """
        if not template_metadata:
            return TableValidationResult(set(), set(), [])

        missing_required: Set[str] = set()
        missing_optional: Set[str] = set()
        type_mismatches: List[tuple] = []

        # Verifica colunas faltando e tipos incompatíveis
        for col_name, col_info in template_metadata.items():
            if col_name not in table_schema:
                if col_info.required:
                    missing_required.add(col_name)
                else:
                    missing_optional.add(col_name)
            else:
                actual_type = table_schema[col_name].lower()
                expected_type = col_info.type.lower()
                
                # Mapeamento de tipos Spark para tipos do template
                type_mappings = {
                    'string': ['string', 'str'],
                    'integer': ['int', 'integer', 'long', 'short'],
                    'double': ['float', 'double', 'decimal'],
                    'boolean': ['bool', 'boolean'],
                    'timestamp': ['timestamp', 'datetime'],
                    'date': ['date']
                }
                
                # Verifica se o tipo atual corresponde ao tipo esperado
                is_valid = False
                for base_type, valid_types in type_mappings.items():
                    if expected_type in valid_types and actual_type in valid_types:
                        is_valid = True
                        break
                        
                if not is_valid:
                    type_mismatches.append(
                        (col_name, col_info.type, table_schema[col_name])
                    )

        return TableValidationResult(
            missing_required=missing_required,
            missing_optional=missing_optional,
            type_mismatches=type_mismatches,
            table_schema=table_schema
        )


def add_metadata_to_table(
    spark,
    table_name: str,
    fields_description: Dict[str, str],
    columns_info: Dict[str, ColumnInfo],
    tag_dict: Dict[str, Any] = None,
    fields_types: Dict[str, str] = None
) -> ProcessResult:
    """
    Adiciona metadados a uma tabela específica.

    Args:
        spark: Sessão Spark
        table_name: Nome da tabela para adicionar os metadados
        fields_description: Dicionário com descrições dos campos
        columns_info: Dicionário com informações das colunas (tipo ColumnInfo)
        tag_dict: Dicionário com tags adicionais (opcional)
        fields_types: Dicionário com tipos dos campos (opcional)

    Returns:
        ProcessResult com o resultado do processamento

    Raises:
        MetadataValidationError: Se houver erros de validação
    """
    manager = MetadataManager(spark)
    
    # Enriquece os ColumnInfo com as descrições
    if fields_description:
        for col_name, col_info in columns_info.items():
            if col_name in fields_description:
                col_info.description = fields_description[col_name]
    
    # Atualiza os tipos se fornecidos
    if fields_types:
        for col_name, col_info in columns_info.items():
            if col_name in fields_types:
                col_info.type = fields_types[col_name]
                
    result = manager.process_table(table_name, columns_info)
    
    if not result.success:
        raise MetadataValidationError([result])
        
    return result
