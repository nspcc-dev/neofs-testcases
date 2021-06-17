*** Settings ***
Variables                       ../../../variables/common.py
Library                         Collections
Library                         ../${RESOURCES}/neofs.py
Library                         ../${RESOURCES}/payment_neogo.py
Library                         ../${RESOURCES}/gates.py
Library                         ${KEYWORDS}/wallet_keywords.py
Library                         ../${RESOURCES}/utility_keywords.py

*** Variables ***
${DEPOSIT_AMOUNT} =     ${5}

*** Test cases ***
NeoFS S3 Gateway
    [Documentation]             Execute operations via S3 Gate
    [Timeout]                   5 min

    [Setup]                     Create Temporary Directory

    ${WIF} =	                Form WIF from String    1dd37fba80fec4e6a6f13fd708d8dcb3b29def768017052f6c930fa1c5d90bbb
    ${WALLET}   ${ADDR} =       Init Wallet from WIF    ${TEMP_DIR}     ${WIF}
    ${TX_DEPOSIT} =             NeoFS Deposit                         ${WIF}    ${DEPOSIT_AMOUNT}
                                Wait Until Keyword Succeeds           1 min            15 sec
                                ...  Transaction accepted in block    ${TX_DEPOSIT}

    ${FILE_S3} =                Generate file of bytes    ${COMPLEX_OBJ_SIZE}
    ${FILE_S3_HASH} =           Get file hash             ${FILE_S3}
    ${FILE_S3_NAME} =           Get file name             ${FILE_S3}

    ${FILE_FS} =                Generate file of bytes    ${COMPLEX_OBJ_SIZE}
    ${FILE_FS_HASH} =           Get file hash             ${FILE_FS}
    ${FILE_FS_NAME} =           Get file name             ${FILE_FS}

    ${CID}
    ...  ${BUCKET}
    ...  ${ACCESS_KEY_ID}
    ...  ${SEC_ACCESS_KEY}
    ...  ${OWNER_PRIV_KEY} =    Init S3 Credentials    ${WIF}    keys/s3_docker_hcs.pub.key

    ${CONTEINERS_LIST} =        Container List               ${WIF}
                                List Should Contain Value    ${CONTEINERS_LIST}    ${CID}

    ${S3_CLIENT} =              Config S3 client    ${ACCESS_KEY_ID}    ${SEC_ACCESS_KEY}

    ${LIST_S3_BUCKETS} =        List buckets S3              ${S3_CLIENT}
                                List Should Contain Value    ${LIST_S3_BUCKETS}    ${BUCKET}

                                Put object S3    ${S3_CLIENT}    ${BUCKET}    ${FILE_S3}
                                Head object S3   ${S3_CLIENT}    ${BUCKET}    ${FILE_S3_NAME}

    ${OID_FS} =                 Put object    ${WIF}    ${FILE_FS}    ${CID}       ${EMPTY}       ${EMPTY}
                                Head object            ${WIF}    ${CID}        ${OID_FS}    ${EMPTY}

    ${LIST_S3_OBJECTS} =        List objects S3              ${S3_CLIENT}             ${BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}       ${FILE_S3_NAME}
                                List Should Contain Value    ${LIST_S3_OBJECTS}       ${FILE_FS_NAME}

    ${LIST_V2_S3_OBJECTS} =     List objects S3 v2           ${S3_CLIENT}             ${BUCKET}
                                List Should Contain Value    ${LIST_V2_S3_OBJECTS}    ${FILE_S3_NAME}
                                List Should Contain Value    ${LIST_V2_S3_OBJECTS}    ${FILE_S3_NAME}

    ${OID_LIST_S3} =            Search object    ${WIF}    ${CID}        ${EMPTY}            ${EMPTY}       FileName=${FILE_S3_NAME}
    ${OID_S3} =                 Get From List    ${OID_LIST_S3}    0

                                Get object S3    ${S3_CLIENT}    ${BUCKET}    ${FILE_S3_NAME}     s3_obj_get_s3
                                Get object S3    ${S3_CLIENT}    ${BUCKET}    ${FILE_FS_NAME}     fs_obj_get_s3

                                Verify file hash    s3_obj_get_s3    ${FILE_S3_HASH}
                                Verify file hash    fs_obj_get_s3    ${FILE_FS_HASH}

                                Get object    ${WIF}    ${CID}    ${OID_S3}    ${EMPTY}    s3_obj_get_fs
                                Get object    ${WIF}    ${CID}    ${OID_FS}    ${EMPTY}    fs_obj_get_fs

                                Verify file hash    s3_obj_get_fs    ${FILE_S3_HASH}
                                Verify file hash    fs_obj_get_fs    ${FILE_FS_HASH}

                                Copy object S3               ${S3_CLIENT}          ${BUCKET}       ${FILE_S3_NAME}    NewName
                                ${LIST_S3_OBJECTS} =         List objects S3       ${S3_CLIENT}    ${BUCKET}
                                List Should Contain Value    ${LIST_S3_OBJECTS}    NewName

                                Delete object S3                 ${S3_CLIENT}          ${BUCKET}       ${FILE_S3_NAME}
                                ${LIST_S3_OBJECTS} =             List objects S3       ${S3_CLIENT}    ${BUCKET}
                                List Should Not Contain Value    ${LIST_S3_OBJECTS}    FILE_S3_NAME

    [Teardown]                  Cleanup

*** Keywords ***

Cleanup
                            Cleanup Files
                            Get Docker Logs    s3_gate
