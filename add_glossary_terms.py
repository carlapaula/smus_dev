import argparse
from time import sleep
import pandas as pd
from smus_scripts import (
    add_glossary,
    add_glossary_term,
    list_glossary_ids,
)


def main(domain_id: str, project_id: str, csv_path: str):
    df = pd.read_csv(csv_path, delimiter=';')
    df = df.fillna('')

    # Cria os glossários
    for _, row in df.iterrows():
        add_glossary(domain_id, project_id, row['name*'])

    sleep(5)

    # Recupera os IDs dos glossários criados
    glossary_ids = list_glossary_ids(domain_id, project_id)

    # Cria os termos dos glossários
    for name, glossary_id in glossary_ids.items():
        df_filter = df[df['name*'] == name]

        if df_filter.empty:
            continue

        readme = f"""### **displayName:** {df_filter.iloc[0]['displayName']}
---
### **synonyms:** {df_filter.iloc[0]['synonyms']}
---
### **references:** {df_filter.iloc[0]['references']}"""

        add_glossary_term(
            domain_id,
            glossary_id,
            df_filter.iloc[0]['name*'],
            df_filter.iloc[0]['description'],
            readme
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Carga de glossário a partir de CSV')
    parser.add_argument('--domain-id', required=True, help='ID do domínio')
    parser.add_argument('--project-id', required=True, help='ID do projeto')
    parser.add_argument('--csv-path', required=True, help='Caminho para o arquivo CSV')

    args = parser.parse_args()

    main(
        domain_id=args.domain_id,
        project_id=args.project_id,
        csv_path=args.csv_path
    )
