import os
from src.utils.yaml_processor import process_yaml_file, get_table_metadata
from src.db.metadata_manager import add_metadata_to_table

def process_yaml_files(spark, yaml_folder: str = ".", bundle_target: str = "prd"):
    """
    Processa todos os arquivos YAML em uma pasta
    
    Args:
        spark: Instância do Spark
        yaml_folder: Pasta com arquivos YAML
        bundle_target: Valor para substituição de variáveis
    """
    for filename in os.listdir(yaml_folder):
        if not (filename.endswith(".yml") or filename.endswith(".yaml")):
            continue
            
        filepath = os.path.join(yaml_folder, filename)
        print(f"\n===== Arquivo: {filename} =====")
        
        # Processa o arquivo YAML
        yaml_data = process_yaml_file(
            filepath,
            variables={"bundle.target": bundle_target}
        )
        
        # Processa cada tabela
        for tabela_nome, tabela_info in yaml_data.get("tables", {}).items():
            # Extrai metadados
            table_path, tag_dict, fields_description, fields_types, columns_info = get_table_metadata(tabela_info)
            
            if not table_path:
                print("[SKIP] Nenhum table_path definido.")
                continue
                
            # Adiciona metadados
            add_metadata_to_table(
                spark=spark,
                table_name=table_path,
                fields_description=fields_description,
                tag_dict=tag_dict,
                fields_types=fields_types,
                columns_info=columns_info
            )

if __name__ == "__main__":
    # Aqui você importaria o spark real
    from src.db.spark_mock import MockSpark
    spark = MockSpark()
    
    print(spark)
    print(spark.mock_tables)

    #process_yaml_files(spark)
