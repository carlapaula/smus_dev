import boto3
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from smus_scripts import (
    list_assets_ids,
)

sts = boto3.client("sts")

assumed_role = sts.assume_role(
    RoleArn="arn:aws:iam::317508306134:role/DataZoneScriptsRole",   
    RoleSessionName="local-script"
)

creds = assumed_role["Credentials"]

assumed_session = boto3.Session(
    aws_access_key_id=creds["AccessKeyId"],
    aws_secret_access_key=creds["SecretAccessKey"],
    aws_session_token=creds["SessionToken"],
    region_name="us-east-1"
)

datazone_client = assumed_session.client("datazone")

def _aguardar_changeset_completion(changeset_id: str, domain_id: str, timeout: int = 300):
    """
    Aguarda a conclusão do changeset
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = datazone_client.get_listing(
                domainIdentifier=domain_id,
                identifier=changeset_id
            )
            status = response.get('status')
            if status == 'ACTIVE':
                return True
            elif status in ['FAILED', 'CANCELLED']:
                raise Exception(f"Changeset falhou com status: {status}")
            time.sleep(1)
        except Exception as e:
            print(f"Erro ao verificar status do changeset: {e}")
            break
    raise Exception("Timeout aguardando conclusão do changeset")

def republicar_asset(asset_id: str, domain_id: str) -> bool:
      """
      Republica um asset no catálogo
      """
      try:
          # Criar changeset para republicação
          response = datazone_client.create_listing_change_set(
              domainIdentifier=domain_id,
              entityIdentifier=asset_id,
              entityType='ASSET',
              action='PUBLISH'
          )
          changeset_id = response['listingId']
          # Aguardar processamento do changeset
          _aguardar_changeset_completion(changeset_id, domain_id)
          return True
      except Exception as e:
          print(f"Erro ao republicar asset {asset_id}: {e}")
          return False

def main():
    parser = argparse.ArgumentParser(description="Republica assets no catálogo DataZone")
    parser.add_argument("--project-id", required=True, help="ID do projeto")
    parser.add_argument("--domain-id", required=True, help="ID do domínio")

    args = parser.parse_args()

    assets_ids = list_assets_ids(project_id=args.project_id, domain_id=args.domain_id)
    print("Republish de SMUs iniciado...\n")

    republicar_asset(assets_ids[0], args.domain_id)
    count = 0
    for asset_id in assets_ids:
        success = republicar_asset(asset_id, args.domain_id)

        if success:
            count += 1
            print(f"Progresso: {count}/{len(assets_ids)}", end="\n")
            print(f"Asset {asset_id} republicado com sucesso.", end="\n")
        else:
            print(f"Falha ao republicar o asset {asset_id}.", end="\n")

    print("\nRepublish de SMUs finalizado.", end="\n")
if __name__ == "__main__":
    main()
