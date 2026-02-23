import argparse
import json
import pandas as pd
from smus_scripts import (
    list_assets_ids,
    get_asset,
    add_metadata_form,
    add_asset_description,
)


def main(domain_id: str, project_id: str, csv_path: str):
    df = pd.read_csv(csv_path, delimiter=';', encoding='utf-8')

    # Lista todos os assets do projeto
    assets_ids = list_assets_ids(domain_id, project_id)

    # Para cada asset
    for asset_id in assets_ids:
        # Obtém os dados do asset
        data = get_asset(domain_id, asset_id)

        # Extrai o conteúdo do formulário
        content_str = data["formsOutput"][0]["content"]
        content = json.loads(content_str)

        table_name = content.get('tableName')

        # Filtra o CSV pela tabela correspondente
        df_filter = df[df['nome_tabela'] == table_name].fillna("")

        if df_filter.empty:
            print(f"⚠️ Nenhuma linha encontrada para a tabela: {table_name}")
            continue

        row = df_filter.iloc[0]

        # Descrição do asset
        description = row['Descrição']

        # Metadados a serem adicionados
        forms = [
            {
                'formName': 'dominio_de_dados',
                'typeIdentifier': 'dominio_de_dados',
                'content': json.dumps({
                    'subdomain': row['Subdomain'],
                    'domain': row['Domain'],
                    'top_domain': row['Top Domain']
                })
            },
            {
                'formName': 'seguranca_privacidade',
                'typeIdentifier': 'seguranca_privacidade',
                'content': json.dumps({
                    'classificacao_privacidade': row['Classificação Privacidade'],
                    'classificacao_seguranca': row['Classificação Segurança']
                })
            },
            {
                'formName': 'ownership',
                'typeIdentifier': 'ownership',
                'content': json.dumps({
                    'owner': row['Owner']
                })
            }
        ]

        # Adiciona os formulários de metadados
        for form in forms:
            add_metadata_form(domain_id, asset_id, form)

        # Adiciona / sobrescreve a descrição do asset
        add_asset_description(domain_id, asset_id, description)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Carga de metadados em assets a partir de CSV')
    parser.add_argument(
        '--domain-id',
        required=True,
        help='ID do domínio'
    )
    parser.add_argument(
        '--project-id',
        required=True,
        help='ID do projeto'
    )
    parser.add_argument(
        '--csv-path',
        required=True,
        help='Caminho para o arquivo CSV de metadados'
    )

    args = parser.parse_args()

    main(
        domain_id=args.domain_id,
        project_id=args.project_id,
        csv_path=args.csv_path
    )
