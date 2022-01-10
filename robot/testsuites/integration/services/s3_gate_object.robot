*** Settings ***
Variables    common.py

Library     Collections
Library     neofs.py
Library     payment_neogo.py
Library     gates.py
Library     wallet_keywords.py
Library     contract_keywords.py
Library     utility_keywords.py

Resource    setup_teardown.robot

*** Variables ***
${DEPOSIT_AMOUNT} =     ${5}
${WIF} =                ${MAINNET_WALLET_WIF}
@{INCLUDE_SVC} =    s3_gate

*** Test cases ***
Objects in NeoFS S3 Gateway
    # TODO: check uploading an s3 object via neofs-cli and a neofs object via s3-gate
    [Documentation]             Execute operations with objects via S3 Gate
    [Timeout]                   10 min

    [Setup]                     Setup    
                                Make Up    ${INCLUDE_SVC}

    ${WALLET}   ${ADDR} =       Init Wallet from WIF    ${ASSETS_DIR}     ${WIF}
    ${TX_DEPOSIT} =             NeoFS Deposit                         ${WIF}    ${DEPOSIT_AMOUNT}
                                Wait Until Keyword Succeeds           1 min            15 sec
                                ...  Transaction accepted in block    ${TX_DEPOSIT}

    ${FILE_S3} =                Generate file of bytes    ${COMPLEX_OBJ_SIZE}
    ${FILE_S3_HASH} =           Get file hash             ${FILE_S3}
    ${FILE_S3_NAME} =           Get file name             ${FILE_S3}

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
                                Head object S3   ${S3_CLIENT}    ${NEW_BUCKET}    ${FILE_S3_NAME}
                                Put object S3    ${S3_CLIENT}    ${NEW_BUCKET_2}    ${FILE_S3}
                                Head object S3   ${S3_CLIENT}    ${NEW_BUCKET_2}    ${FILE_S3_NAME}

    ${LIST_S3_OBJECTS} =        List objects S3              ${S3_CLIENT}             ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}       ${FILE_S3_NAME}
    ${LIST_S3_OBJECTS_2} =      List objects S3              ${S3_CLIENT}             ${NEW_BUCKET_2}
                                List Should Contain Value    ${LIST_S3_OBJECTS_2}       ${FILE_S3_NAME}

    ${LIST_V2_S3_OBJECTS} =     List objects S3 v2           ${S3_CLIENT}             ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_V2_S3_OBJECTS}    ${FILE_S3_NAME}
                                
                                Get object S3    ${S3_CLIENT}    ${NEW_BUCKET}    ${FILE_S3_NAME}     s3_obj_get_s3

                                Verify file hash    s3_obj_get_s3    ${FILE_S3_HASH}

    #TODO: Solve the issue on CopyObject #260 https://github.com/nspcc-dev/neofs-s3-gw/issues/260

                                Copy object S3               ${S3_CLIENT}          ${NEW_BUCKET}       ${FILE_S3_NAME}    NewName
                                ${LIST_S3_OBJECTS} =         List objects S3       ${S3_CLIENT}    ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}    NewName
                                Copy object S3               ${S3_CLIENT}          ${NEW_BUCKET_2}       ${FILE_S3_NAME}    NewName_2
                                ${LIST_S3_OBJECTS_2} =       List objects S3       ${S3_CLIENT}    ${NEW_BUCKET_2}
                                List Should Contain Value    ${LIST_S3_OBJECTS_2}    NewName_2

                                Delete object S3                 ${S3_CLIENT}          ${NEW_BUCKET}       ${FILE_S3_NAME}
                                ${LIST_S3_OBJECTS} =             List objects S3       ${S3_CLIENT}    ${NEW_BUCKET}
                                List Should Not Contain Value    ${LIST_S3_OBJECTS}    ${FILE_S3_NAME}

    [Teardown]                  Teardown    s3_gate_object
