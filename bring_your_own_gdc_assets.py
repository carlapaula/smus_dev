import argparse
import boto3
import ast
from botocore.exceptions import ClientError

def _parse_args():
    parser = argparse.ArgumentParser(description='Python script to bring your glue tables to a specified project in sagemaker unified studio')

    parser.add_argument('--project-role-arn', type=str, required=True,
                        help='Project role arn of the project in which you want to bring your own glue tables')

    parser.add_argument('--database-name', type=str, required=True,
                        help='Glue database name of the table you want to bring into your project')

    parser.add_argument(
        '--table-name',
        type=str,
        required=False,
        help='List of Glue table names in Python list format, e.g.: ["table1", "table2"]. '
             'If not provided, imports all tables in the database.'
    )

    parser.add_argument('--iam-role-arn-lf-resource-register', type=str, required=False,
                        help='IAM Role arn used to register S3 location in LakeFormation. If not provided, the AWS service-linked role is used.')

    parser.add_argument('--region', type=str, required=False,
                        help='The AWS region. If not specified, the default region from your AWS credentials will be used')

    return parser.parse_args()


def _parse_table_list(table_name_arg):
    """
    Parses the --table-name argument which should be a list in string form.
    Example input: '["tabela1", "tabela2"]'
    """
    if not table_name_arg:
        return None

    try:
        # Convert string representation of list into real list
        table_list = ast.literal_eval(table_name_arg)

        if isinstance(table_list, list):
            return table_list
        else:
            # Fallback: treat it as a single element list
            return [table_name_arg]

    except Exception:
        # If parsing fails, treat the argument as a single table name
        return [table_name_arg]


def _check_database_managed_by_iam_access_and_enable_opt_in(database_name, role_arn, lf_client):
    try:
        db_access = lf_client.list_permissions(
            Resource={'Database': {'Name': database_name}},
            Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}
        ).get('PrincipalResourcePermissions', [])

        if db_access:
            print(f"Glue database: {database_name} is managed via IAM access")

            db_opt_in = lf_client.list_lake_formation_opt_ins(
                Principal={'DataLakePrincipalIdentifier': role_arn},
                Resource={'Database': {'Name': database_name}}
            ).get('LakeFormationOptInsInfoList', [])

            if db_opt_in:
                print(f"Principal: {role_arn} is already opted-in to {database_name}")
            else:
                lf_client.create_lake_formation_opt_in(
                    Principal={'DataLakePrincipalIdentifier': role_arn},
                    Resource={'Database': {'Name': database_name}}
                )
                print(f"Successfully created Lake Formation opt-in for database: {database_name}")
        else:
            print(f"Glue database: {database_name} is already managed via LakeFormation")

    except Exception as e:
        print(f"Error checking IAM access / opt-in: {str(e)}")
        raise e


def _check_table_managed_by_iam_access_and_enable_opt_in(database_name, table_name, role_arn, lf_client):
    try:
        table_access = lf_client.list_permissions(
            Resource={'Table': {'DatabaseName': database_name, 'Name': table_name}},
            Principal={'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}
        ).get('PrincipalResourcePermissions', [])

        if table_access:
            print(f"Glue table {database_name}.{table_name} is managed via IAM access")

            tb_opt_in = lf_client.list_lake_formation_opt_ins(
                Principal={'DataLakePrincipalIdentifier': role_arn},
                Resource={'Table': {'DatabaseName': database_name, 'Name': table_name}}
            ).get('LakeFormationOptInsInfoList', [])

            if tb_opt_in:
                print(f"Principal already opted-in: {database_name}.{table_name}")
            else:
                lf_client.create_lake_formation_opt_in(
                    Principal={'DataLakePrincipalIdentifier': role_arn},
                    Resource={'Table': {'DatabaseName': database_name, 'Name': table_name}}
                )
                print(f"Opt-in created for table {database_name}.{table_name}")
        else:
            print(f"Glue table: {database_name}.{table_name} is already managed by LakeFormation")

    except Exception as e:
        print(f"Error checking IAM access for table {database_name}.{table_name}: {str(e)}")
        raise e


def _register_s3_location(s3_path, role_arn, lf_client):
    try:
        resource_arn = f"arn:aws:s3:::{s3_path.replace('s3://', '')}"

        if role_arn:
            lf_client.register_resource(
                ResourceArn=resource_arn,
                RoleArn=role_arn,
                HybridAccessEnabled=True
            )
        else:
            lf_client.register_resource(
                ResourceArn=resource_arn,
                UseServiceLinkedRole=True,
                HybridAccessEnabled=True
            )

        print(f"Registered S3 location: {resource_arn}")

    except Exception as e:
        print(f"Error registering S3 location {resource_arn}: {str(e)}")
        raise e


def _grant_permissions_to_table(role_arn, database_name, table_name, lf_client):
    try:
        lf_client.grant_permissions(
            Principal={'DataLakePrincipalIdentifier': role_arn},
            Resource={'Table': {'Name': table_name, 'DatabaseName': database_name}},
            Permissions=['ALL'],
            PermissionsWithGrantOption=['ALL']
        )
        print(f"Granted ALL permissions on {database_name}.{table_name}")

    except Exception as e:
        print(f"Error granting permissions: {str(e)}")
        raise e


def s3_arn_to_s3_path(arn):
    s3_path = arn.rstrip('/').split(':::', 1)[1]
    return f"s3://{s3_path}"


def _get_s3_subpaths(s3_path):
    s3_path = s3_path.rstrip('/')
    parts = s3_path.split('/')

    paths = []
    current = parts[0] + '//' + parts[2]  # s3://bucket
    paths.append(current)

    for part in parts[3:]:
        current += '/' + part
        paths.append(current)

    return paths


def _get_S3_registered_locations(lf_client):
    locations = []
    next_token = None

    try:
        while True:
            params = {}
            if next_token:
                params["NextToken"] = next_token

            response = lf_client.list_resources(**params)

            for resource in response.get('ResourceInfoList', []):
                if 's3:::' in resource.get('ResourceArn', ''):
                    locations.append(s3_arn_to_s3_path(resource['ResourceArn']))

            next_token = response.get("NextToken")
            if not next_token:
                break

        return locations

    except Exception as e:
        print(f"Error listing S3 registered locations: {str(e)}")
        raise e


def _check_and_register_location(tables, role_arn, lf_client):
    registered_locations = _get_S3_registered_locations(lf_client)

    for table in tables:
        s3_location = table['StorageDescriptor']['Location'].rstrip('/')
        if not s3_location:
            continue

        subpaths = _get_s3_subpaths(s3_location)

        if any(path in registered_locations for path in subpaths):
            print(f"S3 path {s3_location} already registered.")
            continue

        _register_s3_location(s3_location, role_arn, lf_client)
        registered_locations.append(s3_location)


def _get_table(database_name, table_name, glue_client):
    try:
        return glue_client.get_table(DatabaseName=database_name, Name=table_name)['Table']
    except Exception as e:
        print(f"Error retrieving table {database_name}.{table_name}: {str(e)}")
        raise e


def _get_all_tables_for_a_database(database_name, glue_client):
    try:
        tables = []
        next_token = None

        while True:
            params = {"DatabaseName": database_name}
            if next_token:
                params["NextToken"] = next_token

            response = glue_client.get_tables(**params)
            tables.extend(response['TableList'])

            next_token = response.get("NextToken")
            if not next_token:
                break

        return tables

    except Exception as e:
        print(f"Error retrieving tables for database {database_name}: {str(e)}")
        raise e


def byogdc_main():
    args = _parse_args()

    table_list = _parse_table_list(args.table_name)

    session = boto3.Session(region_name=args.region) if args.region else boto3.Session()
    lf_client = session.client('lakeformation')
    glue_client = session.client('glue')

    try:
        _check_database_managed_by_iam_access_and_enable_opt_in(
            args.database_name, args.project_role_arn, lf_client
        )

        if table_list:
            tables = [
                _get_table(args.database_name, table_name, glue_client)
                for table_name in table_list
            ]
        else:
            tables = _get_all_tables_for_a_database(args.database_name, glue_client)

        _check_and_register_location(tables, args.iam_role_arn_lf_resource_register, lf_client)

        for table in tables:
            name = table['Name']
            _check_table_managed_by_iam_access_and_enable_opt_in(
                args.database_name, name, args.project_role_arn, lf_client
            )
            _grant_permissions_to_table(
                args.project_role_arn, args.database_name, name, lf_client
            )

    except Exception as e:
        print(f"Error during import: {e}")
        raise


if __name__ == "__main__":
    byogdc_main()
    print("Successfully imported resources into provided project")
