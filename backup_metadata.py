import argparse
import pandas as pd
from smus_scripts import get_asset, list_assets_ids, create_metadata_df


def main(domain_id: str, project_id: str, output_csv: str):
    # Lista todos os assets do projeto
    assets_ids = list_assets_ids(domain_id, project_id)

    # DataFrame vazio para armazenar os metadados
    df_metadata = pd.DataFrame()

    # Para cada asset, busca os dados e adiciona ao DataFrame
    for asset_id in assets_ids:
        asset_data = get_asset(domain_id, asset_id)
        df_metadata = pd.concat(
            [df_metadata, create_metadata_df(asset_data)],
            ignore_index=True
        )

    # Salva o DataFrame em CSV
    df_metadata.to_csv(output_csv, index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Backup de metadados dos assets')
    parser.add_argument('--domain-id', required=True, help='ID do domínio')
    parser.add_argument('--project-id', required=True, help='ID do projeto')
    parser.add_argument(
        '--output_csv',
        default='backup_metadados.csv',
        help='Arquivo CSV de saída (default: backup_metadados.csv)'
    )

    args = parser.parse_args()

    main(
        domain_id=args.domain_id,
        project_id=args.project_id,
        output_csv=args.output_csv
    )
