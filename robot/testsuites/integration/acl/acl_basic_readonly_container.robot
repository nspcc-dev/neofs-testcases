*** Settings ***
Variables    common.py

Library     container.py
Library     neofs_verbs.py
Library     utility_keywords.py

Resource    payment_operations.robot

*** Variables ***
${DEPOSIT} =    ${30}
${EACL_ERROR_MSG} =     code = 2048 message = access to object operation denied

*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL
    [Timeout]               20 min


                            Check Read-Only Container    Simple
                            Check Read-Only Container    Complex



*** Keywords ***

Check Read-Only Container
    [Arguments]     ${COMPLEXITY}

     ${FILE_S}    ${_} =    Run Keyword If    """${COMPLEXITY}""" == """Simple"""
                            ...         Generate file    ${SIMPLE_OBJ_SIZE}
                            ...     ELSE
                            ...         Generate file    ${COMPLEX_OBJ_SIZE}

    ${USER_WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${READONLY_CID} =   Create Container    ${USER_WALLET}      basic_acl=public-read

                        Transfer Mainnet Gas    ${STORAGE_WALLET_PATH}  ${DEPOSIT + 1}
                        NeoFS Deposit           ${STORAGE_WALLET_PATH}  ${DEPOSIT}
                        Transfer Mainnet Gas    ${IR_WALLET_PATH}       ${DEPOSIT + 1}  wallet_password=${IR_WALLET_PASS}
                        NeoFS Deposit           ${IR_WALLET_PATH}       ${DEPOSIT}      wallet_password=${IR_WALLET_PASS}

    # Put
    ${S_OID_USER} =         Put Object         ${USER_WALLET}    ${FILE_S}    ${READONLY_CID}
                            Run Keyword And Expect Error        *
                            ...  Put object    ${WALLET_OTH}    ${FILE_S}    ${READONLY_CID}
    ${ERR} =                Run Keyword And Expect Error        *
                            ...  Put Object         ${IR_WALLET_PATH}    ${FILE_S}    ${READONLY_CID}    wallet_config=${IR_WALLET_CONFIG}
                            Should Contain          ${ERR}    ${EACL_ERROR_MSG}
    ${S_OID_SYS_SN} =       Put object         ${STORAGE_WALLET_PATH}    ${FILE_S}    ${READONLY_CID}

    # Get
                        Get object    ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                        Get Object    ${IR_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read    wallet_config=${IR_WALLET_CONFIG}
                        Get Object    ${STORAGE_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                        Get Range           ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}    0:256
                        Get Range           ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}    0:256
                        Run Keyword And Expect Error        *
                        ...    Get Range    ${IR_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    0:256    wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...    Get Range    ${STORAGE_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    0:256


    # Get Range Hash
                        Get Range hash    ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        #Get Range hash    ${IR_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256    wallet_config=${IR_WALLET_CONFIG}
                        #Get Range hash    ${STORAGE_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_RO} =       Create List       ${S_OID_USER}    ${S_OID_SYS_SN}
                        Search Object     ${USER_WALLET}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}
                        Search Object     ${WALLET_OTH}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}
                        Search Object     ${IR_WALLET_PATH}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}    wallet_config=${IR_WALLET_CONFIG}
                        Search Object     ${STORAGE_WALLET_PATH}    ${READONLY_CID}    keys=--root    expected_objects_list=${S_OBJ_RO}


    # Head
                        Head Object    ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}
                        Head Object    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}
                        Head Object    ${IR_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    wallet_config=${IR_WALLET_CONFIG}
                        Head Object    ${STORAGE_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}

    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_OTH}    ${READONLY_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${IR_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}    wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${STORAGE_WALLET_PATH}    ${READONLY_CID}    ${S_OID_USER}
                        Delete Object         ${USER_WALLET}    ${READONLY_CID}    ${S_OID_USER}
