#!/usr/bin/python3

'''
    When doing requests to NeoFS, we get JSON output as an automatically decoded
    structure from protobuf. Some fields are decoded with boilerplates and binary
    values are Base64-encoded.

    This module contains functions which rearrange the structure and reencode binary
    data from Base64 to Base58.
'''


import base64
import base58

ROBOT_AUTO_KEYWORDS = False

def decode_simple_header(data: dict):
    '''
    This function reencodes Simple Object header and its attributes.
    '''
    try:
        data = decode_common_fields(data)

        # object attributes view normalization
        ugly_attrs = data["header"]["attributes"]
        data["header"]["attributes"] = {}
        for attr in ugly_attrs:
            data["header"]["attributes"][attr["key"]] = attr["value"]
    except Exception as exc:
        raise ValueError(f"failed to decode JSON output: {exc}") from exc

    return data


def decode_split_header(data: dict):
    '''
    This function rearranges Complex Object header.
    The header holds SplitID, a random unique
    number, which is common among all splitted objects, and IDs of the Linking
    Object and the last splitted Object.
    '''
    try:
        data["splitId"] = json_reencode(data["splitId"])
        data["lastPart"] = (
                    json_reencode(data["lastPart"]["value"])
                    if data["lastPart"] else None
                )
        data["link"] = (
                    json_reencode(data["link"]["value"])
                    if data["link"] else None
                )
    except Exception as exc:
        raise ValueError(f"failed to decode JSON output: {exc}") from exc

    return data

def decode_linking_object(data: dict):
    '''
    This function reencodes Linking Object header.
    It contains IDs of child Objects and Split Chain data.
    '''
    try:
        data = decode_simple_header(data)
        # reencoding Child Object IDs
        # { 'value': <Base58 encoded OID> } -> <Base64 encoded OID>
        for ind, val in enumerate(data['header']['split']['children']):
            data['header']['split']['children'][ind] = json_reencode(val['value'])
        data['header']['split']['splitID'] = json_reencode(data['header']['split']['splitID'])
        data['header']['split']['previous'] = (
                json_reencode(data['header']['split']['previous']['value'])
                if data['header']['split']['previous'] else None
            )
        data['header']['split']['parent'] = (
                json_reencode(data['header']['split']['parent']['value'])
                if data['header']['split']['parent'] else None
            )
    except Exception as exc:
        raise ValueError(f"failed to decode JSON output: {exc}") from exc

    return data


def decode_storage_group(data: dict):
    '''
    This function reencodes Storage Group header.
    '''
    try:
        data = decode_common_fields(data)
    except Exception as exc:
        raise ValueError(f"failed to decode JSON output: {exc}") from exc

    return data

def decode_tombstone(data: dict):
    '''
    This function reencodes Tombstone header.
    '''
    try:
        data = decode_simple_header(data)
        data['header']['sessionToken'] = decode_session_token(
                data['header']['sessionToken'])
    except Exception as exc:
        raise ValueError(f"failed to decode JSON output: {exc}") from exc
    return data


def decode_session_token(data: dict):
    '''
    This function reencodes a fragment of header which contains
    information about session token.
    '''
    data['body']['object']['address']['containerID'] = json_reencode(
            data['body']['object']['address']['containerID']['value'])
    data['body']['object']['address']['objectID'] = json_reencode(
            data['body']['object']['address']['objectID']['value'])
    return data

def json_reencode(data: str):
    '''
    According to JSON protocol, binary data (Object/Container/Storage Group IDs, etc)
    is converted to string via Base58 encoder. But we usually operate with Base64-encoded
    format.
    This function reencodes given Base58 string into the Base64 one.
    '''
    return base58.b58encode(base64.b64decode(data)).decode("utf-8")


def encode_for_json(data: str):
    '''
    This function encodes binary data for sending them as protobuf
    structures.
    '''
    return base64.b64encode(base58.b58decode(data)).decode('utf-8')

def decode_common_fields(data: dict):
    '''
    Despite of type (simple/complex Object, Storage Group, etc) every Object
    header contains several common fields.
    This function rearranges these fields.
    '''
    # reencoding binary IDs
    data["objectID"] = json_reencode(data["objectID"]["value"])
    data["header"]["containerID"] = json_reencode(data["header"]["containerID"]["value"])
    data["header"]["ownerID"] = json_reencode(data["header"]["ownerID"]["value"])
    data["header"]["homomorphicHash"] = json_reencode(data["header"]["homomorphicHash"]["sum"])
    data["header"]["payloadHash"] = json_reencode(data["header"]["payloadHash"]["sum"])
    data["header"]["version"] = (
            f"{data['header']['version']['major']}{data['header']['version']['minor']}"
        )
    return data
