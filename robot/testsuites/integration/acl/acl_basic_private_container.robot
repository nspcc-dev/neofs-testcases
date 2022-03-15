*** Settings ***
Variables       common.py

Library         neofs.py
Library         neofs_verbs.py
Library         payment_neogo.py

Resource        common_steps_acl_basic.robot
Resource        payment_operations.robot
Resource        setup_teardown.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL
    [Timeout]               20 min

    [Setup]                 Setup

    ${_}   ${_}     ${USER_KEY} =   Prepare Wallet And Deposit
    ${_}   ${_}     ${OTHER_KEY} =   Prepare Wallet And Deposit

    ${PRIV_CID} =           Create Private Container    ${USER_KEY}
    ${FILE_S}    ${_} =     Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    ${PRIV_CID} =           Create Private Container    ${USER_KEY}
    ${FILE_S}    ${_} =     Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    [Teardown]              Teardown    acl_basic_private_container


*** Keywords ***

Check Private Container
    [Arguments]    ${USER_KEY}    ${FILE_S}    ${PRIV_CID}    ${OTHER_KEY}

    # Put
    ${S_OID_USER} =     Put Object         ${USER_KEY}    ${FILE_S}    ${PRIV_CID}
                        Run Keyword And Expect Error      *
                        ...  Put object    ${OTHER_KEY}    ${FILE_S}    ${PRIV_CID}
    ${S_OID_SYS_IR} =    Put Object        ${NEOFS_IR_WIF}    ${FILE_S}    ${PRIV_CID}
    ${S_OID_SYS_SN} =    Put Object        ${NEOFS_SN_WIF}    ${FILE_S}    ${PRIV_CID}

                        Sleep   5s

    # Get
                        Get Object         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                        Run Keyword And Expect Error      *
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
                        Search Object         ${USER_KEY}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Run Keyword And Expect Error        *
                        ...  Search object    ${OTHER_KEY}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Search Object         ${NEOFS_IR_WIF}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}
                        Search Object         ${NEOFS_SN_WIF}    ${PRIV_CID}    keys=--root    expected_objects_list=${S_OBJ_PRIV}


    # Head
                        Head Object         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Head object    ${OTHER_KEY}    ${PRIV_CID}    ${S_OID_USER}
                        Head Object         ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}
                        Head Object         ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}


    # Delete
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${OTHER_KEY}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${NEOFS_IR_WIF}    ${PRIV_CID}    ${S_OID_USER}
                        Run Keyword And Expect Error        *
                        ...  Delete object    ${NEOFS_SN_WIF}    ${PRIV_CID}    ${S_OID_USER}
                        Delete Object         ${USER_KEY}    ${PRIV_CID}    ${S_OID_USER}
