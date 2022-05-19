*** Settings ***
Variables       common.py

Library         container.py
Library         neofs_verbs.py
Library         utility_keywords.py

Resource        payment_operations.robot
Resource        setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${_}     ${_} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${_}     ${_} =   Prepare Wallet And Deposit

    ${PRIV_CID} =           Create Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    ${WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    ${PRIV_CID} =           Create Container    ${WALLET}
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    ${WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    [Teardown]              Teardown    acl_basic_private_container


*** Keywords ***

Check Private Container
    [Arguments]    ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}    ${WALLET_OTH}

    ${WALLET_SN}    ${ADDR_SN} =     Prepare Wallet with WIF And Deposit    ${NEOFS_SN_WIF}
    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    # Put
    ${S_OID_USER} =     Put Object         ${USER_WALLET}    ${FILE_S}    ${PRIV_CID}
                        Run Keyword And Expect Error        *
                        ...  Put object    ${WALLET_OTH}    ${FILE_S}    ${PRIV_CID}
    ${S_OID_SYS_IR} =    Put Object        ${WALLET_IR}    ${FILE_S}    ${PRIV_CID}
    ${S_OID_SYS_SN} =    Put Object        ${WALLET_SN}    ${FILE_S}    ${PRIV_CID}

    # Get
                        Get Object         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Run Keyword And Expect Error        *
                        ...  Get object    ${WALLET_OTH}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Get Object         ${WALLET_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Get Object         ${WALLET_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read

    # Get Range
                        Get Range         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${WALLET_IR}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${WALLET_SN}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

    # Get Range Hash
                        Get Range hash         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range Hash    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash         ${WALLET_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash         ${WALLET_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =     Create List    ${S_OID_USER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                        Search Object         ${USER_WALLET}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Run Keyword And Expect Error        *
                        ...  Search object    ${WALLET_OTH}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Search Object         ${WALLET_IR}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Search Object         ${WALLET_SN}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}


    # Head
                        Head Object         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}
                        Head Object         ${WALLET_IR}    ${PRIV_CID}    ${S_OID_USER}
                        Head Object         ${WALLET_SN}    ${PRIV_CID}    ${S_OID_USER}


    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_OTH}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_IR}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${WALLET_SN}    ${PRIV_CID}    ${S_OID_USER}
                        Delete Object         ${USER_WALLET}    ${PRIV_CID}    ${S_OID_USER}
