*** Settings ***
Variables                   ../../../variables/common.py
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py
Library                     ../${RESOURCES}/utility_keywords.py

Resource                    common_steps_acl_basic.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Create Temporary Directory

                            Generate Keys

                            Create Containers
                            Generate file    ${SIMPLE_OBJ_SIZE}
                            Check Private Container

                            Create Containers
                            Generate file    ${COMPLEX_OBJ_SIZE}
                            Check Private Container

    [Teardown]              Cleanup




*** Keywords ***

Check Private Container

    # Put
    ${S_OID_USER} =         Put object                 ${USER_KEY}         ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object            ${OTHER_KEY}        ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object            ${SYSTEM_KEY_IR}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_SN} =       Put object                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}

    # Get
                            Get object               ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Run Keyword And Expect Error        *
                            ...  Get object          ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Get object               ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Get object               ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read

    # Get Range
                            Get Range                           ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range                      ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256

    # Get Range Hash
                            Get Range Hash                      ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                 ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =	        Create List	                        ${S_OID_USER}       ${S_OID_SYS_SN}
                            Search object                       ${USER_KEY}         ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Run Keyword And Expect Error        *
                            ...  Search object                  ${OTHER_KEY}        ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_IR}    ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object                       ${SYSTEM_KEY_SN}    ${PRIV_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}


    # Head
                            Head object                         ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Head object                    ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}


    # Delete
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Delete object                  ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}
                            Delete object                       ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}


Cleanup
                            Cleanup Files
