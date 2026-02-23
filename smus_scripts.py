from botocore.exceptions import ClientError
from copy import deepcopy
from typing import List
import boto3
import json
import pandas as pd

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


def get_asset(domain_id: str, asset_id: str) -> dict:
    """
    Gets the data of a SageMaker DataZone asset.

    :param domain_id: Domain identifier in SageMaker DataZone.
    :param asset_id: Asset identifier in SageMaker DataZone.
    :return: A dictionary with the asset data returned by DataZone.
    """
    return datazone_client.get_asset(
        domainIdentifier=domain_id,
        identifier=asset_id,
    )


def list_assets_ids(domain_id: str, project_id: str) -> List[str]:
    """
    Lists the identifiers of all assets within a given SageMaker DataZone project.

    :param domain_id: Identifier of the DataZone domain where the search will be executed.
    :param project_id: Identifier of the DataZone project whose asset IDs will be listed.
    :return: A list of asset identifiers found in the specified project.
    """
    asset_ids: List[str] = []
    next_token: str | None = None

    while True:
        request_params = {
            "domainIdentifier": domain_id,
            "owningProjectIdentifier": project_id,
            "searchScope": "ASSET",
        }

        if next_token:
            request_params["nextToken"] = next_token

        response = datazone_client.search(**request_params)

        asset_ids.extend(
            item["assetItem"]["identifier"]
            for item in response.get("items", [])
        )

        next_token = response.get("nextToken")
        if not next_token:
            break

    return asset_ids


def filter_content_output(asset_data, form_name):
    """
    Extracts and returns the content of a specific form from a DataZone asset.

    :param asset_data: Dictionary representing the asset data returned by ``get_asset``.
    :param form_name: Name of the form whose content should be retrieved.
    :return: A pandas DataFrame containing the parsed content of the matching form.
    """
    content = asset_data["formsOutput"]
    content_filter = [content['content'] for content in content
                if content.get("formName") == form_name]
    if content_filter == []:
      print(f"Formulário '{form_name}' não encontrado no asset.")
      content_filter = {}
    else:
      content_filter = json.loads(content_filter[0])

    df = pd.DataFrame([content_filter])
    return df


def create_metadata_df(asset_data):
    """
    Creates a consolidated pandas DataFrame containing metadata extracted from
    multiple forms of a SageMaker DataZone asset.

    This function retrieves and merges the output of specific forms inside the
    asset's ``formsOutput`` section. The forms used are:
    - ``seguranca_privacidade``
    - ``dominio_de_dados``
    - ``descricao``
    - ``GlueTableForm`` (from which only the ``tableName`` field is used)

    Each form is extracted using ``filter_content_output`` and then merged into a
    single DataFrame indexed by row.

    :param asset_data: Dictionary representing asset data returned by ``get_asset``.
                       Must contain a ``formsOutput`` section with the expected forms.
    :return: A merged pandas DataFrame containing all relevant metadata from the asset.
    """
    df_seguranca_privacidade = filter_content_output(asset_data, "seguranca_privacidade")

    df_dominio_de_dados = filter_content_output(asset_data, "dominio_de_dados")

    df_table_name = filter_content_output(asset_data, "GlueTableForm")

    df = pd.merge(df_seguranca_privacidade, df_dominio_de_dados, left_index=True, right_index=True)
    
    if "tableName" in df_table_name.columns:
        df = pd.merge(df, df_table_name["tableName"], left_index=True, right_index=True)
    
    df["Descrição"] = asset_data.get("description", "")

    return df


def list_glossary_ids(domain_id: str, project_id: str) -> dict:
    """
    Retrieves all glossary identifiers available in a given SageMaker DataZone project.

    :param domain_id: Identifier of the DataZone domain where the glossaries reside.
    :param project_id: Identifier of the DataZone project associated with the glossaries.
    :return: A dictionary where keys are glossary names and values are glossary IDs.
    """
    request_params = {
        'domainIdentifier': domain_id,
        'owningProjectIdentifier': project_id,
        'searchScope': 'GLOSSARY'
    }

    glossary_dict = {}
    next_token = None

    while True:
        if next_token:
            request_params['nextToken'] = next_token

        response = datazone_client.search(**request_params)

        for item in response.get('items', []):
            name = item['glossaryItem']['name']
            gid = item['glossaryItem']['id']
            glossary_dict[name] = gid

        next_token = response.get('nextToken')

        if not next_token:
            break

    return glossary_dict


def add_glossary(domain_id: str, project_id: str, glossary_name: str):
    """
    Creates a new glossary in a specified SageMaker DataZone project.
    :param domain_id: Identifier of the DataZone domain where the glossary will be created.
    :param project_id: Identifier of the DataZone project that will own the glossary.
    :param glossary_name: Name of the glossary to be created.
    """
    request_params = {
        'domainIdentifier': domain_id,
        'owningProjectIdentifier': project_id,
        'name': glossary_name
    }
    try:
        datazone_client.create_glossary(**request_params)
        print(f'Glossário {glossary_name} adicionado')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print(f'Glossário {glossary_name} já existe')
        else:
            raise


def add_glossary_term(domain_id: str, glossary_id: str, term_name: str, description: str, readme: str):
    """
    Creates a new glossary term inside a specific glossary in SageMaker DataZone.

    :param domain_id: Identifier of the DataZone domain where the glossary resides.
    :param glossary_id: Identifier of the glossary in which the term will be created.
    :param term_name: Name of the glossary term to be created.
    :param description: Short description of the term (field: ``shortDescription``).
    :param readme: Long, detailed description of the term (field: ``longDescription``).
    :return: None. Prints a message describing the outcome of the operation.
    """
    request_params = {
        'domainIdentifier': domain_id,
        'glossaryIdentifier': glossary_id,
        'name': term_name,
        'shortDescription': description,
        'longDescription': readme,
    }
    try:
        datazone_client.create_glossary_term(**request_params)
        print(f'Termo de négocio {term_name} adicionado')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConflictException':
            print(f'Termo de negócio {term_name} já existe')
        else:
            raise


def add_metadata_form(domain_id: str, asset_id: str, metadata_form_to_add: dict) -> None:
    """
    Adds a metadata form in an AWS DataZone asset.

    :param domain_id: Identifier of the DataZone domain.
    :param asset_id: Identifier of the asset to update.
    :param metadata_form_to_add: Dict containing formName, typeIdentifier and content.
    """
    actual_asset = get_asset(domain_id, asset_id)
    asset_name = actual_asset["name"]

    # Parse new content
    try:
        new_content = (
            json.loads(metadata_form_to_add["content"])
            if isinstance(metadata_form_to_add["content"], str)
            else deepcopy(metadata_form_to_add["content"])
        )
    except Exception as e:
        raise ValueError(f"Erro ao decodificar 'content': {e}")

    target_type = metadata_form_to_add["typeIdentifier"]

    # Load existing forms
    asset_forms = deepcopy(actual_asset.get("formsOutput", []))
    normalized_forms = []

    for form in asset_forms:
        if "typeName" in form:
            form["typeIdentifier"] = form.pop("typeName")

        if isinstance(form.get("content"), dict):
            form["content"] = json.dumps(form["content"])

        normalized_forms.append(form)

    # Update or insert
    form_found = False
    for form in normalized_forms:
        if form.get("typeIdentifier") == target_type:
            try:
                existing_content = json.loads(form["content"])
            except Exception:
                existing_content = {}

            merged_content = {**existing_content, **new_content}
            form["content"] = json.dumps(merged_content)
            form_found = True
            break

    if not form_found:
        normalized_forms.append({
            "formName": metadata_form_to_add["formName"],
            "typeIdentifier": target_type,
            "content": json.dumps(new_content)
        })

    description = actual_asset.get("description")

    if description is None:
        description = ""

    request = {
        "domainIdentifier": domain_id,
        "identifier": asset_id,
        "name": asset_name,
        "formsInput": normalized_forms,
        "description": description  # keep original description
    }

    datazone_client.create_asset_revision(**request)

    print(f"Formulário '{target_type}' adicionado/atualizado no asset '{asset_name}'.")


def add_asset_description(domain_id: str, asset_id: str, description: str) -> None:
    """
    Updates the description of an AWS DataZone asset.
    - If the asset has no description, it will always be added.
    - If the asset has a description, it will only be overwritten when overwrite=True.

    :param domain_id: Identifier of the DataZone domain.
    :param asset_id: Identifier of the asset.
    :param description: The new description text.
    :param overwrite: If True, overwrite existing description when it exists.
    """

    # Fetch current asset
    actual_asset = get_asset(domain_id, asset_id)
    asset_name = actual_asset["name"]
    existing_description = actual_asset.get("description")

    # # Decide whether update is allowed
    # if existing_description and not overwrite:
    #     print(f"O asset '{asset_name}' já possui descrição e overwrite=False.")
    #     return

    # Preserve existing forms
    forms_output = actual_asset.get("formsOutput", [])

    # Normalize forms
    normalized_forms = []
    for form in deepcopy(forms_output):
        if "typeName" in form:
            form["typeIdentifier"] = form.pop("typeName")

        if isinstance(form.get("content"), dict):
            form["content"] = json.dumps(form["content"])

        normalized_forms.append(form)

    request = {
        "domainIdentifier": domain_id,
        "identifier": asset_id,
        "name": asset_name,
        "formsInput": normalized_forms,
        "description": description,
    }

    datazone_client.create_asset_revision(**request)

    if existing_description:
        print(f"Descrição sobrescrita para o asset '{asset_name}'.")
    else:
        print(f"Descrição adicionada ao asset '{asset_name}'.")


def get_domain_id(domain_name: str) -> str:
    """
    Retrieves the identifier (ID) of an AWS DataZone domain by its name.

    - Searches through available domains.
    - Filters the domain list to match the provided domain name.
    - Returns the ID of the matched domain.

    :param domain_name: The name of the DataZone domain to look up.
    :return: The identifier (ID) of the matching domain.
    """
    domain_data = datazone_client.list_domains(
        maxResults=1,
        status='AVAILABLE'
    )

    domain_data = [domain for domain in domain_data["items"] if domain["name"] == domain_name]

    domain_id = domain_data[0]["id"]

    return domain_id


def list_project_ids(domain_id: str) -> dict:
    """
    Lists all project identifiers within an AWS DataZone domain.

    - Retrieves all projects using pagination.
    - Extracts both the project ID and the associated domain unit ID.
    - Returns a dictionary mapping each project ID to its domain unit ID.

    :param domain_id: Identifier of the DataZone domain.
    :return: A dictionary where keys are project IDs and values are their corresponding domain unit IDs.
    """

    paginator = datazone_client.get_paginator('list_projects')
    project_data = []

    for page in paginator.paginate(domainIdentifier=domain_id):
        project_data.extend(page.get('items', []))

    project_ids = [project["id"] for project in project_data]
    project_domains = [project["domainId"] for project in project_data]

    return dict(zip(project_ids, project_domains))