*** Settings ***
Variables       common.py

Library         container.py
Library         neofs_verbs.py
Library         utility_keywords.py

Resource        payment_operations.robot
Resource        setup_teardown.robot

*** Variables ***
${DEPOSIT} =    ${30}
${EACL_ERROR_MSG} =     code = 2048 message = access to object operation denied

*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup


                            Check Private Container    Simple
                            Check Private Container    Complex

    [Teardown]              Teardown    acl_basic_private_container


*** Keywords ***

Check Private Container
    [Arguments]    ${COMPLEXITY}

     ${FILE_S}    ${_} =    Run Keyword If    """${COMPLEXITY}""" == """Simple"""
                            ...         Generate file    ${SIMPLE_OBJ_SIZE}
                            ...     ELSE
                            ...         Generate file    ${COMPLEX_OBJ_SIZE}

    ${USER_WALLET}
    ...     ${_}
    ...     ${_} =      Prepare Wallet And Deposit
    ${WALLET_OTH}
    ...     ${_}
    ...     ${_} =      Prepare Wallet And Deposit

    ${PRIV_CID} =       Create Container    ${USER_WALLET}

                        Transfer Mainnet Gas    ${STORAGE_WALLET_PATH}  ${DEPOSIT + 1}
                        NeoFS Deposit           ${STORAGE_WALLET_PATH}  ${DEPOSIT}
                        Transfer Mainnet Gas    ${IR_WALLET_PATH}       ${DEPOSIT + 1}  wallet_password=${IR_WALLET_PASS}
                        NeoFS Deposit           ${IR_WALLET_PATH}       ${DEPOSIT}      wallet_password=${IR_WALLET_PASS}
    # Put
    ${S_OID_USER} =     Put Object         ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}
                        Run Keyword And Expect Error        *
                        ...  Put object    ${WALLET_OTH}    ${FILE_S}    ${PRIV_CID}
    ${ERR} =            Run Keyword And Expect Error    *
                        ...  Put Object        ${IR_WALLET_PATH}    ${FILE_S}    ${PRIV_CID}    wallet_config=${IR_WALLET_CONFIG}
                        Should Contain          ${ERR}    ${EACL_ERROR_MSG}
    ${S_OID_SYS_SN} =    Put Object        ${STORAGE_WALLET_PATH}    ${FILE_S}    ${PRIV_CID}

    # Get
                        Get Object         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WALLET_OTH}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Get Object         ${IR_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read    wallet_config=${IR_WALLET_CONFIG}
                        Get Object         ${STORAGE_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read

    # Get Range
                        Get Range         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${IR_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256    wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${STORAGE_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

    # Get Range Hash
                        Get Range hash         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range Hash    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        # TODO: always fails for complex object
                        #Get Range hash         ${IR_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256    wallet_config=${IR_WALLET_CONFIG}
                        Get Range hash         ${STORAGE_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =     Create List    ${S_OID_USER}    ${S_OID_SYS_SN}
                        Search Object         ${USER_WALLET}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Run Keyword And Expect Error        *
                        ...  Search object    ${WALLET_OTH}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Search Object         ${IR_WALLET_PATH}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}    wallet_config=${IR_WALLET_CONFIG}
                        Search Object         ${STORAGE_WALLET_PATH}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}


    # Head
                        Head Object         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}
                        Head Object         ${IR_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    wallet_config=${IR_WALLET_CONFIG}
                        Head Object         ${STORAGE_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}


    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${IR_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}    wallet_config=${IR_WALLET_CONFIG}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${STORAGE_WALLET_PATH}    ${PRIV_CID}    ${S_OID_USER}
                        Delete Object         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}
