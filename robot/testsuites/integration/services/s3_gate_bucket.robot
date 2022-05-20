*** Settings ***
Variables    common.py

Library     Collections
Library     OperatingSystem

Library     neofs.py
Library     s3_gate.py
Library     contract_keywords.py
Library     utility_keywords.py

Resource    setup_teardown.robot
Resource    payment_operations.robot

*** Variables ***
@{INCLUDE_SVC} =    s3_gate     coredns

*** Test cases ***
Buckets in NeoFS S3 Gateway
    [Documentation]             Execute operations with bucket via S3 Gate
    [Timeout]                   10 min

    [Setup]                     Setup
                                Make Up    ${INCLUDE_SVC}

    ${WALLET}   ${_}    ${WIF} =        Prepare Wallet And Deposit
    ${FILE_S3}    ${_} =    Generate file    ${COMPLEX_OBJ_SIZE}
    ${_}        ${S3_OBJECT_KEY} =      Split Path                  ${FILE_S3}

    ${CID}
    ...  ${BUCKET}
    ...  ${ACCESS_KEY_ID}
    ...  ${SEC_ACCESS_KEY}
    ...  ${OWNER_PRIV_KEY} =    Init S3 Credentials    ${WALLET}

    ${CONTEINERS_LIST} =        Container List               ${WIF}
                                List Should Contain Value    ${CONTEINERS_LIST}    ${CID}

    ${S3_CLIENT} =              Config S3 client    ${ACCESS_KEY_ID}    ${SEC_ACCESS_KEY}

    ${NEW_BUCKET} =             Create Bucket S3    ${S3_CLIENT}
    ${NEW_BUCKET_EMPTY} =       Create Bucket S3    ${S3_CLIENT}

                                Head bucket S3    ${S3_CLIENT}      ${BUCKET}
                                Head bucket S3    ${S3_CLIENT}      ${NEW_BUCKET}

                                Put object S3    ${S3_CLIENT}    ${NEW_BUCKET}    ${FILE_S3}
                                Head object S3   ${S3_CLIENT}    ${NEW_BUCKET}    ${S3_OBJECT_KEY}

    ${LIST_S3_OBJECTS} =        List objects S3              ${S3_CLIENT}             ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}       ${S3_OBJECT_KEY}

                                Run Keyword and Expect Error    *
                                ...  Delete Bucket S3    ${S3_CLIENT}    ${NEW_BUCKET}
                                Head bucket S3    ${S3_CLIENT}      ${NEW_BUCKET}

                                Delete Bucket S3    ${S3_CLIENT}    ${NEW_BUCKET_EMPTY}
                                Tick Epoch
                                Run Keyword And Expect Error    *
                                ...  Head bucket S3    ${S3_CLIENT}     ${NEW_BUCKET_EMPTY}

    ${BUCKET_LIST} =            List Buckets S3    ${S3_CLIENT}
                                Tick Epoch
                                List Should Contain Value    ${BUCKET_LIST}    ${NEW_BUCKET}
                                List Should Not Contain Value    ${BUCKET_LIST}    ${NEW_BUCKET_EMPTY}

    [Teardown]                  Teardown    s3_gate_bucket
