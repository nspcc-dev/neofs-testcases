*** Settings ***
Variables    common.py

Library      neofs.py
Library      payment_neogo.py

Resource     common_steps_acl_basic.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    ${WALLET}   ${ADDR}     ${USER_KEY} =   Prepare Wallet And Deposit 
    ${WALLET_OTH}   ${ADDR_OTH}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${PUBLIC_CID} =         Create Public Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =    Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Public Container    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${OTHER_KEY}

    ${PUBLIC_CID} =         Create Public Container    ${USER_KEY}
    ${FILE_S}    ${FILE_S_HASH} =    Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Public Container    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_basic_public_container


*** Keywords ***

Check Public Container
    [Arguments]    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${OTHER_KEY}

    # Put
    ${S_OID_USER} =         Put Object    ${USER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_OTHER} =        Put Object    ${OTHER_KEY}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_IR} =       Put Object    ${NEOFS_IR_WIF}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_SN} =       Put Object    ${NEOFS_SN_WIF}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY}

    # Get
                            Get Object    ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                            Get Range           ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range           ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...    Get Range    ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...    Get Range    ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash    ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =         Create List	      ${S_OID_USER}    ${S_OID_OTHER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                            Search object     ${USER_KEY}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object     ${OTHER_KEY}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object     ${NEOFS_IR_WIF}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object     ${NEOFS_SN_WIF}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}

    # Head
                            Head Object    ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head Object    ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head Object    ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head Object    ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

                            Head Object    ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head Object    ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head Object    ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head Object    ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}

                            Head Object    ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head Object    ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head Object    ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head Object    ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}


    # Delete
                            Delete object            ${USER_KEY}    ${PUBLIC_CID}    ${S_OID_SYS_IR}    ${EMPTY}
                            Delete Object            ${OTHER_KEY}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...    Delete object     ${NEOFS_IR_WIF}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...    Delete object     ${NEOFS_SN_WIF}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}
