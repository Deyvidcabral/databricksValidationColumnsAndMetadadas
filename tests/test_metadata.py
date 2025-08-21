import os
from typing import Dict, Any, List
from src.db.spark_mock import MockSpark
from src.utils.yaml_processor import process_yaml_file, get_table_metadata
from src.db.metadata_manager import add_metadata_to_table, ProcessResult, MetadataValidationError

# Prefixos usados para construir os nomes das tabelas
TABLE_PREFIX = "catalogo.schema."

# Dados de exemplo para testes com diferentes tipos
MOCK_DATA = {
    "catalogo.schema.nonexistent": [],  # Tabela que não existe
    "catalogo.schema.valid_table": [
        {
            "id": 1,
            "name": "Exemplo Válido",
            "active": True,
            "value": 42,
            "created_at": "2023-01-01 10:00:00",
            "metadata": {"key": "value", "description": "Registro válido"},
            "tags": ["tag1", "tag2"]
        }
    ],
    "catalogo.schema.missing_columns": [
        {
            "id": 1,  # Falta a coluna 'nome' que é obrigatória
            "valor": 10.5,
            "data": "2023-01-01",
            "metadata": {"description": "Registro com coluna faltando"},
            "tags": ["test", "missing_column"]
        }
    ],
    "catalogo.schema.wrong_types": [
        {
            "id": "1",  # Deveria ser integer
            "nome": "Teste",
            "valor": "10.5",  # Deveria ser número
            "ativo": "true",  # Deveria ser boolean
            "metadata": {"description": "Registro com tipos incorretos"},
            "tags": ["test", "wrong_type"]
        }
    ],
    "catalogo.schema.complex_types": [
        {
            "id": 1,
            "metadata": {"key": "value", "description": "Tipos complexos"},
            "tags": ["tag1", "tag2"],
            "config": {"nested": {"data": 42}},
            "scores": [1.0, 2.5, 3.7]
        }
    ],
    "catalogo.schema.mixed_types": [
        {
            "question_id": 1000,  # long
            "text": "Pergunta teste",
            "is_active": True,  # boolean mas YAML declara como int
            "score": 9.5,  # double mas YAML declara como integer
            "created_at": "2023-01-01 10:00:00",
            "metadata": {"description": "Registro com tipos mistos"},
            "tags": ["test", "mixed_types"],
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
        # Remove o prefixo se presente e adiciona sufixo _table se não for valid_table
        table_base_name = table_name.replace(TABLE_PREFIX, "").split('.')[-1]
        yaml_table_name = table_base_name
        
        if yaml_table_name not in yaml_data.get("tables", {}):
            yaml_table_name = f"{table_base_name}_table"
            if yaml_table_name not in yaml_data.get("tables", {}):
                return ProcessResult(
                    table_name=table_name,
                    validation=None,
                    success=False,
                    error_message=f"Tabela '{yaml_table_name}' não encontrada no arquivo YAML"
                )
            
        # Obtém as informações da tabela do YAML
        table_info = yaml_data["tables"][yaml_table_name]
        table_path, tag_dict, fields_description, fields_types, columns_info = get_table_metadata(table_info)
        
        # Se use_path_directly for True, usa o nome da tabela fornecido
        actual_table_name = table_name if use_path_directly else table_path
        
        return add_metadata_to_table(
            spark=spark,
            table_name=actual_table_name,
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

def test_table_existence():
    """
    Testa se a validação detecta corretamente tabelas que não existem.
    
    Erro esperado:
    - A tabela não deve ser encontrada no mock
    """
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Existência da Tabela ===")
    table_info = yaml_data["tables"]["nonexistent_table"]
    table_path, _, _, _, _ = get_table_metadata(table_info)
    result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def test_missing_columns():
    """
    Testa se a validação detecta colunas obrigatórias faltando.
    
    Erros esperados:
    1. Coluna obrigatória 'nome' faltando
    """
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Colunas Faltando ===")
    table_info = yaml_data["tables"]["missing_columns_table"]
    table_path, _, _, _, _ = get_table_metadata(table_info)
    result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def test_wrong_types():
    """
    Testa se a validação detecta tipos incorretos nas colunas.
    
    Erros esperados:
    1. Coluna 'id': string quando deveria ser integer
    2. Coluna 'valor': string quando deveria ser double
    3. Coluna 'ativo': string quando deveria ser boolean
    """
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Tipos Incorretos ===")
    table_info = yaml_data["tables"]["wrong_types_table"]
    table_path, _, _, _, _ = get_table_metadata(table_info)
    result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def test_complex_types():
    """
    Testa se a validação funciona corretamente com tipos complexos.
    
    Deve validar:
    1. Estruturas (struct)
    2. Arrays
    3. Estruturas aninhadas
    """
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Tipos Complexos ===")
    table_info = yaml_data["tables"]["complex_types_table"]
    table_path, _, _, _, _ = get_table_metadata(table_info)
    result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def test_valid_table():
    """
    Testa uma tabela com todos os tipos corretos e colunas existentes.
    Este teste deve passar com sucesso, pois todos os tipos e colunas
    correspondem exatamente ao esperado.
    """
    spark = MockSpark(MOCK_DATA)
    yaml_data = process_yaml_file("test_tables.yml")
    
    print("\n=== Teste de Tabela Válida ===")
    table_info = yaml_data["tables"]["valid_table"]
    table_path, _, _, _, _ = get_table_metadata(table_info)
    result = process_table(spark, yaml_data, table_path, use_path_directly=True)
    return result

def print_table_validation_result(test_name: str, result: ProcessResult):
    """
    Imprime o resultado detalhado da validação de uma tabela
    
    Args:
        test_name: Nome do teste
        result: Resultado do processamento
    """
    print(f"\n{'='*50}")
    print(f"Executando: {test_name}")
    print(f"Tabela: {result.table_name}")
    print('='*50)
    
    if result.error_message and "não encontrada" in result.error_message:
        print(f"\n❌ {result.error_message}")
        return
        
    if result.validation:
        # 1. Colunas OK
        valid_columns = []
        if result.validation.columns_checked:
            valid_columns = [col for col in result.validation.columns_checked 
                           if col not in result.validation.missing_required
                           and col not in result.validation.missing_optional
                           and col not in [x[0] for x in (result.validation.type_mismatches or [])]]
        if valid_columns:
            print("\n✅ Colunas Validadas com Sucesso:")
            for col in sorted(valid_columns):
                print(f"    ✅ {col}")
        
        # 2. Warnings (colunas opcionais faltando)
        if result.validation.missing_optional:
            print("\n⚠️ Avisos (Colunas Opcionais Faltando):")
            for col in sorted(result.validation.missing_optional):
                print(f"    ⚠️ {col}")
        
        # 3. Erros
        has_errors = False
        
        if result.validation.missing_required:
            has_errors = True
            print("\n❌ Colunas Obrigatórias Faltando:")
            for col in sorted(result.validation.missing_required):
                print(f"    ❌ {col}")
                
        if result.validation.type_mismatches:
            has_errors = True
            print("\n❌ Incompatibilidade de Tipos:")
            for col, expected, found in sorted(result.validation.type_mismatches, key=lambda x: x[0]):
                print(f"    ❌ {col}")
                print(f"       Esperado: {expected}")
                print(f"       Encontrado: {found}")
        
        # 4. Resumo da tabela
        print("\n📊 Resumo desta Tabela:")
        total_columns = len(valid_columns) + len(result.validation.missing_optional or []) + \
                       len(result.validation.missing_required or []) + len(result.validation.type_mismatches or [])
        print(f"    Total de Colunas: {total_columns}")
        print(f"    ✅ Colunas OK: {len(valid_columns)}")
        print(f"    ⚠️ Warnings: {len(result.validation.missing_optional or [])}")
        errors = len(result.validation.missing_required or []) + len(result.validation.type_mismatches or [])
        print(f"    ❌ Erros: {errors}")
        
        if not has_errors:
            print("\n✅ Tabela validada com sucesso!")
        else:
            print("\n❌ Tabela com erros de validação!")
    
    print('\n' + '-'*50)

def run_all_tests():
    """Executa todos os testes e verifica os resultados no final"""
    print("\n🚀 Iniciando Validação de Metadados...\n")
    
    # Lista de testes na ordem lógica de validação
    tests = [
        ("Teste de Existência da Tabela", test_table_existence),
        ("Teste de Colunas Faltando", test_missing_columns),
        ("Teste de Tipos Incorretos", test_wrong_types),
        ("Teste de Tipos Complexos", test_complex_types),
        ("Teste de Tabela Válida", test_valid_table)
    ]
    
    # Executa cada teste e coleta os resultados
    test_results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            print_table_validation_result(test_name, result)
            test_results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ Erro ao executar {test_name}: {str(e)}")
            test_results.append((test_name, None))
    
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
        if result.success:
            successful_validations.append((test_name, result))
        else:
            failed_validations.append((test_name, result))
    
    print("\n=== Resumo Final ===")
    print(f"Total de testes: {len(test_results)}")
    print(f"✅ Sucesso: {len(successful_validations)}")
    print(f"❌ Falha: {len(failed_validations)}\n")
    
    # Calcula as estatísticas finais
    successful_tests = [tr for tr in test_results if tr[1] and tr[1].success]
    failed_tests = [tr for tr in test_results if tr[1] is None or not tr[1].success]
    
    # Imprime o resumo final
    print("\n📋 RESUMO FINAL DA VALIDAÇÃO")
    print("=" * 50)
    print(f"\nTotal de Testes Executados: {len(test_results)}")
    print(f"✅ Testes com Sucesso: {len(successful_tests)}")
    print(f"❌ Testes com Falha: {len(failed_tests)}")
    
    if successful_tests:
        print("\n✅ Testes Bem Sucedidos:")
        for test_name, _ in successful_tests:
            print(f"    ✅ {test_name}")
    
    if failed_tests:
        print("\n❌ Testes com Falha:")
        for test_name, _ in failed_tests:
            print(f"    ❌ {test_name}")
    
    print("\n" + "=" * 50)
    
    if failed_tests:
        print("\n❌ Processo de validação concluído com erros!")
    else:
        print("\n✅ Processo de validação concluído com sucesso!")

if __name__ == "__main__":
    print("Iniciando testes de metadata...")
    try:
        run_all_tests()
    except MetadataValidationError as e:
        print(str(e))
        exit(1)
