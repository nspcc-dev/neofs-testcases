*** Settings ***
Variables                       ../../../variables/common.py
Library                         Collections
Library                         neofs.py
Library                         payment_neogo.py
Library                         gates.py
Library                         wallet_keywords.py
Library                         contract_keywords.py

Resource                        setup_teardown.robot

*** Variables ***
${DEPOSIT_AMOUNT} =     ${5}
${WIF} =                ${MAINNET_WALLET_WIF}
${DEPOSIT_TIMEOUT}=    30s

*** Test cases ***
Buckets in NeoFS S3 Gateway
    [Documentation]             Execute operations with bucket via S3 Gate
    [Timeout]                   10 min

    [Setup]                     Setup

    ${WALLET}   ${ADDR} =       Init Wallet from WIF    ${ASSETS_DIR}     ${WIF}
    ${TX_DEPOSIT} =             NeoFS Deposit                         ${WIF}    ${DEPOSIT_AMOUNT}
                                Wait Until Keyword Succeeds         ${DEPOSIT_TIMEOUT}    ${MAINNET_BLOCK_TIME}
                                ...  Transaction accepted in block  ${TX_DEPOSIT}

    ${FILE_S3} =                Generate file of bytes    ${COMPLEX_OBJ_SIZE}
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
    ${NEW_BUCKET_EMPTY} =       Create Bucket S3    ${S3_CLIENT}

                                HeadBucket S3    ${BUCKET}    ${S3_CLIENT}
                                HeadBucket S3    ${NEW_BUCKET}    ${S3_CLIENT}

                                Put object S3    ${S3_CLIENT}    ${NEW_BUCKET}    ${FILE_S3}
                                Head object S3   ${S3_CLIENT}    ${NEW_BUCKET}    ${FILE_S3_NAME}

    ${LIST_S3_OBJECTS} =        List objects S3              ${S3_CLIENT}             ${NEW_BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}       ${FILE_S3_NAME}
                                
                                Run Keyword and Expect Error    *
                                ...  Delete Bucket S3    ${S3_CLIENT}    ${NEW_BUCKET}
                                HeadBucket S3    ${NEW_BUCKET}    ${S3_CLIENT}

                                Delete Bucket S3    ${S3_CLIENT}    ${NEW_BUCKET_EMPTY}
                                Tick Epoch
                                Run Keyword And Expect Error    *
                                ...  HeadBucket S3    ${NEW_BUCKET_EMPTY}    ${S3_CLIENT}
    
    ${BUCKET_LIST} =            List Buckets S3    ${S3_CLIENT}
                                Tick Epoch
                                List Should Contain Value    ${BUCKET_LIST}    ${NEW_BUCKET}
                                List Should Not Contain Value    ${BUCKET_LIST}    ${NEW_BUCKET_EMPTY}

    [Teardown]                  Teardown    s3_gate_bucket


