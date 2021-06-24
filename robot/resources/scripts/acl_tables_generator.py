#!/usr/bin/python3.8

###################################
# eACL tables generation functions
###################################

import json

VERBS = [
    'GET',
    'HEAD',
    'PUT',
    'DELETE',
    'SEARCH',
    'GETRANGE',
    'GETRANGEHASH'
]

ROLES = [
    'OTHERS',
    'USER',
    'SYSTEM'
]

ACCESS = [
    'DENY',
    'ALLOW'
]

TABLES_DIR = '../files/eacl_tables/'


def deny_allow_tables_per_role():
    for a in ACCESS:
        for r in ROLES:
            table_dict = {
                        "records": []
                    }
            for v in VERBS:
                table_record =  {
                    "operation": v,
                    "action": a,
                    "filters": [],
                    "targets": [
                        {
                            "role": r
                        }
                    ]
                }
                table_dict['records'].append(table_record)
            with open(f"{TABLES_DIR}/gen_eacl_{a.lower()}_all_{r}", "w+") as f:
                json.dump(table_dict, f, indent=4)

def allow_pubkey_deny_others():
    table_dict = {
                "records": []
            }
    for v in VERBS:
        table_record =  {
            "operation": v,
            "action": "ALLOW",
            "filters": [],
            "targets": [
                {
                    # TODO: where do we take this value from?
                    "keys": [ 'A9tDy6Ye+UimXCCzJrlAmRE0FDZHjf3XRyya9rELtgAA' ]
                }
            ]
        }
        table_dict['records'].append(table_record)
    for v in VERBS:
        table_record =  {
            "operation": v,
            "action": "DENY",
            "filters": [],
            "targets": [
                {
                    "role": 'OTHERS'
                }
            ]
        }
        table_dict['records'].append(table_record)
    with open(f"{TABLES_DIR}/gen_eacl_allow_pubkey_deny_OTHERS", "w+") as f:
        json.dump(table_dict, f, indent=4)

def compound_tables():
    compounds = {
        'get': {
            'GET': 'ALLOW',
            'GETRANGE': 'ALLOW',
            'GETRANGEHASH': 'ALLOW',
            'HEAD': 'DENY'
        },
        'del': {
            'DELETE': 'ALLOW',
            'PUT': 'DENY',
            'HEAD': 'DENY'
        },
        'get_hash': {
            'GETRANGEHASH': 'ALLOW',
            'GETRANGE': 'DENY',
            'GET': 'DENY'
        }
    }
    for op, compound in compounds.items():
        for r in ROLES:
            table_dict = {
                        "records": []
                    }
            for verb, access in compound.items():
                table_record =  {
                    "operation": verb,
                    "action": access,
                    "filters": [],
                    "targets": [
                        {
                            "role": r
                        }
                    ]
                }
                table_dict['records'].append(table_record)

            with open(f"{TABLES_DIR}/gen_eacl_compound_{op}_{r}", "w+") as f:
                json.dump(table_dict, f, indent=4)

def xheader_tables():
    filters = {
        'headerType': 'REQUEST',
        'matchType': 'STRING_EQUAL',
        'key': 'a',
        'value': '2'
    }
    table_dict = {
                "records": []
            }
    for verb in VERBS:
        table_record =  {
            "operation": verb,
            "action": "DENY",
            "filters": [filters],
            "targets": [
                {
                    "role": "OTHERS"
                }
            ]
        }
        table_dict['records'].append(table_record)
    with open(f"{TABLES_DIR}/gen_eacl_xheader_deny_all", "w+") as f:
        json.dump(table_dict, f, indent=4)

    table_dict = {
                "records": []
            }
    for verb in VERBS:
        table_record =  {
            "operation": verb,
            "action": "ALLOW",
            "filters": [filters],
            "targets": [
                {
                    "role": "OTHERS"
                }
            ]
        }
        table_dict['records'].append(table_record)

        table_record =  {
            "operation": verb,
            "action": "DENY",
            "filters": [],
            "targets": [
                {
                    "role": "OTHERS"
                }
            ]
        }
        table_dict['records'].append(table_record)
    with open(f"{TABLES_DIR}/gen_eacl_xheader_allow_all", "w+") as f:
        json.dump(table_dict, f, indent=4)


deny_allow_tables_per_role()
allow_pubkey_deny_others()
compound_tables()
xheader_tables()
