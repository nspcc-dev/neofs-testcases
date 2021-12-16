*** Settings ***
Variables       common.py

Library         neofs.py
Library         payment_neogo.py

Resource        common_steps_acl_basic.robot
Resource        payment_operations.robot
Resource        setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${PRIV_CID} =           Create Private Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =    Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    ${PRIV_CID} =           Create Private Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =    Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_basic_private_container


*** Keywords ***

Check Private Container
    [Arguments]    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    # Put
    ${S_OID_USER} =     Put Object         ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}
                        Run Keyword And Expect Error        *
                        ...  Put object    ${OTHER_KEY}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_IR} =    Put Object        ${NEOFS_IR_WIF}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_SN} =    Put Object        ${NEOFS_SN_WIF}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}

    # Get
                        Get Object         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Run Keyword And Expect Error        *
                        ...  Get object    ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Get Object         ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Get Object         ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read

    # Get Range
                        Get Range         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${OTHER_KEY}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range    ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

    # Get Range Hash
                        Get Range hash         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range Hash    ${OTHER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash         ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                        Get Range hash         ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =     Create List    ${S_OID_USER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                        Search Object         ${USER_KEY}    ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                        Run Keyword And Expect Error        *
                        ...  Search object    ${OTHER_KEY}    ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                        Search Object         ${NEOFS_IR_WIF}    ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                        Search Object         ${NEOFS_SN_WIF}    ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}


    # Head
                        Head Object         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${OTHER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        Head Object         ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                        Head Object         ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}


    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${OTHER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                        Delete Object         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
