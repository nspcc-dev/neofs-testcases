*** Settings ***
Variables    common.py

Library      container.py
Library      neofs.py
Library      neofs_verbs.py
Library      payment_neogo.py

Resource     common_steps_acl_basic.robot
Resource     payment_operations.robot
Resource     setup_teardown.robot


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

    ${WALLET_SN}    ${ADDR_SN} =     Prepare Wallet with WIF And Deposit    ${NEOFS_SN_WIF}
    ${WALLET_IR}    ${ADDR_IR} =     Prepare Wallet with WIF And Deposit    ${NEOFS_IR_WIF}

    # Put
    ${S_OID_USER} =         Put Object    ${USER_WALLET}    ${FILE_S}    ${PUBLIC_CID}
    ${S_OID_OTHER} =        Put Object    ${WALLET_OTH}    ${FILE_S}    ${PUBLIC_CID}
    ${S_OID_SYS_IR} =       Put Object    ${WALLET_IR}    ${FILE_S}    ${PUBLIC_CID}
    ${S_OID_SYS_SN} =       Put Object    ${WALLET_SN}    ${FILE_S}    ${PUBLIC_CID}

    # Get
                            Get Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get Object    ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                            Get Range           ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range           ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...    Get Range    ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...    Get Range    ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =         Create List	      ${S_OID_USER}    ${S_OID_OTHER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                            Search object     ${USER_WALLET}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}
                            Search object     ${WALLET_OTH}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}
                            Search object     ${WALLET_IR}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}
                            Search object     ${WALLET_SN}    ${PUBLIC_CID}     keys=--root    expected_objects_list=${S_OBJ_PRIV}

    # Head
                            Head Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_USER}
                            Head Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_USER}
                            Head Object    ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_USER}
                            Head Object    ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_USER}

                            Head Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_OTHER}
                            Head Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_OTHER}
                            Head Object    ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_OTHER}
                            Head Object    ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}

                            Head Object    ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Head Object    ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Head Object    ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Head Object    ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_SYS_SN}


    # Delete
                            Delete object            ${USER_WALLET}    ${PUBLIC_CID}    ${S_OID_SYS_IR}
                            Delete Object            ${WALLET_OTH}    ${PUBLIC_CID}    ${S_OID_SYS_SN}
                            Run Keyword And Expect Error        *
                            ...    Delete object     ${WALLET_IR}    ${PUBLIC_CID}    ${S_OID_USER}
                            Run Keyword And Expect Error        *
                            ...    Delete object     ${WALLET_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}
