*** Settings ***
Variables    common.py

Library     Collections
Library     OperatingSystem

Library     neofs.py
Library     s3_gate.py
Library     contract_keywords.py

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
@{INCLUDE_SVC} =        s3_gate     coredns

*** Test cases ***
Objects in NeoFS S3 Gateway
    # TODO: check uploading an s3 object via neofs-cli and a neofs object via s3-gate
    [Documentation]             Execute operations with objects via S3 Gate
    [Timeout]                   10 min

    [Setup]                     Setup
                                Make Up    ${INCLUDE_SVC}

    ${WALLET}   ${_}    ${WIF} =    Prepare Wallet And Deposit

    ${FILE_S3} =                Generate file of bytes    ${COMPLEX_OBJ_SIZE}
    ${FILE_S3_HASH} =           Get file hash             ${FILE_S3}
    ${_}    ${S3_OBJECT_KEY} =  Split Path                ${FILE_S3}

    ${CID}
    ...  ${BUCKET}
    ...  ${ACCESS_KEY_ID}
    ...  ${SEC_ACCESS_KEY}
    ...  ${OWNER_PRIV_KEY} =    Init S3 Credentials    ${WALLET}

    ${CONTEINERS_LIST} =        Container List               ${WIF}
                                List Should Contain Value    ${CONTEINERS_LIST}    ${CID}

    ${S3_CLIENT} =              Config S3 client    ${ACCESS_KEY_ID}    ${SEC_ACCESS_KEY}

    ${NEW_BUCKET} =             Create Bucket S3    ${S3_CLIENT}
    ${NEW_BUCKET_2} =           Create Bucket S3    ${S3_CLIENT}

                                Put object S3    ${S3_CLIENT}    ${NEW_BUCKET}    ${FILE_S3}
                                Head object S3   ${S3_CLIENT}    ${NEW_BUCKET}    ${S3_OBJECT_KEY}
                                Put object S3    ${S3_CLIENT}    ${NEW_BUCKET_2}    ${FILE_S3}
                                Head object S3   ${S3_CLIENT}    ${NEW_BUCKET_2}    ${S3_OBJECT_KEY}

    ${LIST_S3_OBJECTS} =        List objects S3              ${S3_CLIENT}             ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}       ${S3_OBJECT_KEY}
    ${LIST_S3_OBJECTS_2} =      List objects S3              ${S3_CLIENT}             ${NEW_BUCKET_2}
                                List Should Contain Value    ${LIST_S3_OBJECTS_2}     ${S3_OBJECT_KEY}

    ${LIST_V2_S3_OBJECTS} =     List objects S3 v2           ${S3_CLIENT}             ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_V2_S3_OBJECTS}    ${S3_OBJECT_KEY}

    ${OBJ_PATH} =               Get object S3    ${S3_CLIENT}    ${NEW_BUCKET}    ${S3_OBJECT_KEY}
                                Verify file hash    ${OBJ_PATH}    ${FILE_S3_HASH}
    ${HASH} =                   Get file hash       ${OBJ_PATH}
                                Should Be Equal     ${FILE_S3_HASH}      ${HASH}

    #TODO: Solve the issue on CopyObject #260 https://github.com/nspcc-dev/neofs-s3-gw/issues/260

    ${COPIED_OBJ_PATH} =        Copy object S3               ${S3_CLIENT}           ${NEW_BUCKET}       ${S3_OBJECT_KEY}
                                ${LIST_S3_OBJECTS} =         List objects S3        ${S3_CLIENT}        ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}     ${COPIED_OBJ_PATH}
    ${COPIED_OBJ_PATH_2} =      Copy object S3               ${S3_CLIENT}           ${NEW_BUCKET_2}     ${S3_OBJECT_KEY}
                                ${LIST_S3_OBJECTS_2} =       List objects S3        ${S3_CLIENT}        ${NEW_BUCKET_2}
                                List Should Contain Value    ${LIST_S3_OBJECTS_2}   ${COPIED_OBJ_PATH_2}

                                Delete object S3                 ${S3_CLIENT}          ${NEW_BUCKET}       ${S3_OBJECT_KEY}
                                ${LIST_S3_OBJECTS} =             List objects S3       ${S3_CLIENT}    ${NEW_BUCKET}
                                List Should Not Contain Value    ${LIST_S3_OBJECTS}    ${S3_OBJECT_KEY}

    [Teardown]                  Teardown    s3_gate_object
