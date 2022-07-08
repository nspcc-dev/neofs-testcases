*** Settings ***
Variables    common.py

Library      container.py
Library      neofs_verbs.py
Library      utility_keywords.py

Resource     payment_operations.robot
Resource     setup_teardown.robot

*** Variables ***
${DEPOSIT} =    ${30}
${EACL_ERROR_MSG} =     code = 2048 message = access to object operation denied


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${PUBLIC_CID} =         Create Container    ${WALLET}       basic_acl=public-read-write
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Public Container    ${WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    ${PUBLIC_CID} =         Create Container    ${WALLET}       basic_acl=public-read-write
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Public Container    ${WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_public_container


*** Keywords ***

Check Public Container
    [Arguments]    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}    ${WALLET_OTH}

                            Transfer Mainnet Gas    ${STORAGE_WALLET_PATH}  ${DEPOSIT + 1}
                            NeoFS Deposit           ${STORAGE_WALLET_PATH}  ${DEPOSIT}
                            Transfer Mainnet Gas    ${IR_WALLET_PATH}       ${DEPOSIT + 1}  wallet_password=${IR_WALLET_PASS}
                            NeoFS Deposit           ${IR_WALLET_PATH}       ${DEPOSIT}      wallet_password=${IR_WALLET_PASS}
    # Put
    ${S_OID_USER} =         Put Object    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}
    ${S_OID_OTHER} =        Put Object    ${WALLET_OTH}    ${FILE_S}    ${PUBLIC_CID}
    ${ERR} =                Run Keyword And Expect Error    *
                            ...    Put Object    ${IR_WALLET_PATH}    ${FILE_S}    ${PUBLIC_CID}    wallet_config=${IR_WALLET_CONFIG}
                            Should Contain          ${ERR}    ${EACL_ERROR_MSG}
    ${S_OID_SYS_SN} =       Put Object    ${STORAGE_WALLET_PATH}    ${FILE_S}    ${PUBLIC_CID}

    # Get
                            Get Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read    wallet_config=${IR_WALLET_CONFIG}
                            Get Object    ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                            Get Range           ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range           ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...    Get Range    ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256    wallet_config=${IR_WALLET_CONFIG}
                            Run Keyword And Expect Error        *
                            ...    Get Range    ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            # TODO: fails with "object not found"
                            #Get Range Hash    ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256    wallet_config=${IR_WALLET_CONFIG}
                            #Get Range Hash    ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =         Create List	      ${S_OID_USER}    ${S_OID_OTHER}    ${S_OID_SYS_SN}
                            Search object     ${USER_WALLET}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}
                            Search object     ${WALLET_OTH}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}
                            Search object     ${IR_WALLET_PATH}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}    wallet_config=${IR_WALLET_CONFIG}
                            Search object     ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}

    # Head
                            Head Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}
                            Head Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}
                            Head Object    ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    wallet_config=${IR_WALLET_CONFIG}
                            Head Object    ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}

                            Head Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_OTHER}
                            Head Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_OTHER}
                            Head Object    ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_OTHER}    wallet_config=${IR_WALLET_CONFIG}
                            Head Object    ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_OTHER}

                            Head Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Head Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Head Object    ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    wallet_config=${IR_WALLET_CONFIG}
                            Head Object    ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_SYS_SN}


    # Delete
                            Delete Object            ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Run Keyword And Expect Error        *
                            ...    Delete object     ${IR_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_USER}    wallet_config=${IR_WALLET_CONFIG}
                            Run Keyword And Expect Error        *
                            ...    Delete object     ${STORAGE_WALLET_PATH}    ${PUBLIC_CID}    ${S_OID_OTHER}
                            Delete object            ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}
