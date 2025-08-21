import os
from typing import Dict, Any, List
from src.db.spark_mock import MockSpark
from src.utils.yaml_processor import process_yaml_file, get_table_metadata
from src.db.metadata_manager import add_metadata_to_table, ProcessResult, MetadataValidationError

# Dados de exemplo para testes com diferentes tipos
MOCK_DATA = {
    "prd_estruturante.silver_postgresql_argilla.responses": [
        {
            "id": 1,
            "nome": "Teste",
            "ativo": True,  # boolean mas YAML declara como string
            "valor": 10.5,  # double mas YAML declara como integer
            "data": "2023-01-01",
            "metadata": {"key": "value"},  # struct
            "tags": ["tag1", "tag2"],  # array
            "binario": b"teste"  # binary
        }
    ],
    "prd_estruturante.silver_postgresql_argilla.questions": [
        {
            "question_id": 1000,  # long
            "text": "Pergunta teste",
            "is_active": True,  # boolean mas YAML declara como int
            "score": 9.5,  # double mas YAML declara como integer
            "created_at": "2023-01-01 10:00:00",
            "properties": {"type": "multiple_choice"},  # map
            "options": ["op1", "op2"],  # array
            "attachment": b"dados"  # binary
        }
    ]
}

def process_table(spark, yaml_data, table_name, use_path_directly=False) -> ProcessResult:
    """
    Processa uma tabela e retorna o resultado
    
    Args:
        spark: Sessão spark
        yaml_data: Dados do YAML
        table_name: Nome da tabela ou caminho completo
        use_path_directly: Se True, usa table_name diretamente como caminho
    """
    try:
        if use_path_directly:
            # Usa o caminho completo diretamente
            return add_metadata_to_table(
                spark=spark,
                table_name=table_name,
                fields_description=fields_description,
                columns_info=columns_info,
                tag_dict=tag_dict,
                fields_types=fields_types
            )
            
        if table_name not in yaml_data.get("tables", {}):
            return ProcessResult(
                table_name=table_name,
                validation=None,
                success=False,
                error_message=f"Tabela '{table_name}' não encontrada no arquivo YAML"
            )
            
        table_info = yaml_data["tables"][table_name]
        table_path, tag_dict, fields_description, fields_types, columns_info = get_table_metadata(table_info)
        
        return add_metadata_to_table(
            spark=spark,
            table_name=table_path,
            fields_description=fields_description,
            columns_info=columns_info,
            tag_dict=tag_dict,
            fields_types=fields_types
        )
    except Exception as e:
        return ProcessResult(
            table_name=table_name,
            validation=None,
            success=False,
            error_message=f"Erro ao processar tabela '{table_name}': {str(e)}"
        )

def test_column_type_mismatch():
    """Testa casos onde o tipo no YAML não corresponde ao tipo real"""
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Incompatibilidade de Tipos ===")
    result = process_table(spark, yaml_data, "responses_table")
    if not result.success and result.error_message and "não encontrada" in result.error_message:
        print("  ⚠️ Tentando com caminho completo...")
        # Se falhar, obter o caminho do YAML e usar diretamente
        table_info = yaml_data["tables"]["responses_table"]
        table_path, _, _, _, _ = get_table_metadata(table_info)
        result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def test_complex_types():
    """Testa casos com tipos complexos (array, map, struct)"""
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Tipos Complexos ===")
    result = process_table(spark, yaml_data, "questions_table")
    if not result.success and result.error_message and "não encontrada" in result.error_message:
        print("  ⚠️ Tentando com caminho completo...")
        # Se falhar, obter o caminho do YAML e usar diretamente
        table_info = yaml_data["tables"]["questions_table"]
        table_path, _, _, _, _ = get_table_metadata(table_info)
        result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def run_all_tests():
    """Executa todos os testes e verifica os resultados no final"""
    print("\nExecutando testes...\n")
    
    # Executa os testes e garante que sempre temos um resultado
    test_results = [
        ("Teste de Tipos e Colunas", test_column_type_mismatch()),
        ("Teste de Tipos Complexos", test_complex_types())
    ]
    
    successful_validations = []
    failed_validations = []
    
    for test_name, result in test_results:
        if result is None:
            result = ProcessResult(
                table_name="Desconhecida",
                validation=None,
                success=False,
                error_message=f"Teste '{test_name}' não retornou resultado"
            )
            failed_validations.append((test_name, result))
        elif result.success:
            successful_validations.append((test_name, result))
        else:
            failed_validations.append((test_name, result))
    
    # Mostra resumo da validação para todos os testes
    for test_name, result in test_results:
        print(f"\n=== {test_name} ===")
        print(f"Tabela: {result.table_name}")
        
        # Verifica se a tabela existe no mock
        try:
            spark = MockSpark(MOCK_DATA)
            spark.table(result.table_name)
            print("  ✅ Tabela encontrada no mock")
        except Exception as e:
            print("  ❌ Tabela não encontrada no mock:")
            print(f"    {str(e)}")
        
        if result.validation and result.validation.table_schema:
            print(f"\n  Schema encontrado: {len(result.validation.table_schema)} colunas")
            # Primeiro lista as colunas corretas
            problem_columns = set()
            if result.validation.missing_required:
                problem_columns.update(result.validation.missing_required)
            if result.validation.missing_optional:
                problem_columns.update(result.validation.missing_optional)
            problem_columns.update(col for col, _, _ in result.validation.type_mismatches)
            
            correct_columns = set(result.validation.table_schema.keys()) - problem_columns
            if correct_columns:
                print("\n  ✅ Colunas Corretas:")
                for col in sorted(correct_columns):
                    print(f"    - {col} ({result.validation.table_schema[col]})")
            
            # Mostra incompatibilidades de tipo
            if result.validation.type_mismatches:
                print("\n  ❌ Incompatibilidade de tipos:")
                for col, exp, real in sorted(result.validation.type_mismatches):
                    print(f"    - {col}: esperado {exp}, encontrado {real}")
            
            # Mostra colunas obrigatórias faltantes
            if result.validation.missing_required:
                print("\n  ❌ Colunas obrigatórias faltando:")
                for col in sorted(result.validation.missing_required):
                    print(f"    - {col}")
            
            # Mostra colunas opcionais faltantes
            if result.validation.missing_optional:
                print("\n  ⚠️ Colunas opcionais faltando:")
                for col in sorted(result.validation.missing_optional):
                    print(f"    - {col}")
        
        # Mostra erro geral se houver
        if result.error_message:
            print("\n  ❌ Erro:")
            error_lines = result.error_message.split('\n')
            for line in error_lines:
                print(f"    {line.strip()}")
            
        # Mostra o status final do teste
        status = "✅ Sucesso" if result.success else "❌ Falha"
        if result.validation and result.validation.table_schema:
            total_columns = len(result.validation.table_schema)
            problem_columns = len(set(col for col, _, _ in result.validation.type_mismatches) | 
                                set(result.validation.missing_required) | 
                                set(result.validation.missing_optional))
            correct_columns = total_columns - problem_columns
            status += f" ({correct_columns}/{total_columns} colunas corretas)"
            
        print(f"\n  {status}\n")
    
    # Resumo final
    print("\n=== Resumo Final ===")
    print(f"Total de testes: {len(test_results)}")
    print(f"✅ Sucesso: {len(successful_validations)}")
    print(f"❌ Falha: {len(failed_validations)}")
    
    # Resumo das tabelas testadas
    print("\nTabelas verificadas:")
    for test_name, result in test_results:
        exists = True
        try:
            spark = MockSpark(MOCK_DATA)
            spark.table(result.table_name)
        except:
            exists = False
        
        status = "✅" if exists else "❌"
        print(f"  {status} {result.table_name}")
    
    # Se houver erros, mostra detalhes e lança exceção
    if failed_validations:
        print("\nDetalhes dos erros:")
        raise MetadataValidationError([r for _, r in failed_validations])

if __name__ == "__main__":
    print("Iniciando testes de metadata...")
    try:
        run_all_tests()
        print("\n✅ Todos os testes foram concluídos com sucesso!")
    except MetadataValidationError as e:
        print("\n❌ Testes concluídos com erros!")
        print(str(e))
        exit(1)
