*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py
Library                     ../${RESOURCES}/utility_keywords.py

Resource                    common_steps_acl_extended.robot


*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Create Temporary Directory

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object
                            Generate files    ${SIMPLE_OBJ_SIZE}
                            Check Сompound Operations



                            Log    Check extended ACL with complex object
                            Generate files    ${COMPLEX_OBJ_SIZE}
                            Check Сompound Operations

    [Teardown]              Cleanup


*** Keywords ***



Check Сompound Operations
                            Check eACL Сompound Get    ${OTHER_KEY}     ${EACL_COMPOUND_GET_OTHERS}
                            Check eACL Сompound Get    ${USER_KEY}      ${EACL_COMPOUND_GET_USER}
                            Check eACL Сompound Get    ${SYSTEM_KEY}    ${EACL_COMPOUND_GET_SYSTEM}

                            Check eACL Сompound Delete    ${OTHER_KEY}     ${EACL_COMPOUND_DELETE_OTHERS}
                            Check eACL Сompound Delete    ${USER_KEY}      ${EACL_COMPOUND_DELETE_USER}
                            Check eACL Сompound Delete    ${SYSTEM_KEY}    ${EACL_COMPOUND_DELETE_SYSTEM}

                            Check eACL Сompound Get Range Hash    ${OTHER_KEY}     ${EACL_COMPOUND_GET_HASH_OTHERS}
                            Check eACL Сompound Get Range Hash    ${USER_KEY}      ${EACL_COMPOUND_GET_HASH_USER}
                            Check eACL Сompound Get Range Hash    ${SYSTEM_KEY}    ${EACL_COMPOUND_GET_HASH_SYSTEM}

Check eACL Сompound Get
    [Arguments]             ${KEY}    ${DENY_EACL}

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
                            Put object             ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}
                            Get object           ${KEY}         ${CID}       ${S_OID_USER}    ${EMPTY}    local_file_eacl
                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}     --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}

                            Get object           ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       local_file_eacl
                            Get Range                       ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}           0:256
                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256


Check eACL Сompound Delete
    [Arguments]             ${KEY}    ${DENY_EACL}

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
    ${D_OID_USER} =         Put object             ${USER_KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${EMPTY}
                            Put object             ${KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}
                            Delete object                   ${KEY}         ${CID}       ${D_OID_USER}    ${EMPTY}

                            Set eACL                        ${USER_KEY}    ${CID}       ${DENY_EACL}     --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Head object                ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error    *
                            ...  Put object        ${KEY}    ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}

                            Delete object                   ${KEY}    ${CID}       ${S_OID_USER}    ${EMPTY}



Check eACL Сompound Get Range Hash
    [Arguments]             ${KEY}    ${DENY_EACL}

    ${CID} =                Create Container Public

    ${S_OID_USER} =         Put object             ${USER_KEY}         ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_USR_HEADER}
                            Put object             ${KEY}              ${FILE_S}    ${CID}           ${EMPTY}    ${FILE_OTH_HEADER}
                            Get Range Hash                  ${SYSTEM_KEY_SN}    ${CID}       ${S_OID_USER}    ${EMPTY}    0:256

                            Set eACL                        ${USER_KEY}         ${CID}       ${DENY_EACL}     --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep    ${NEOFS_CONTRACT_CACHE_TIMEOUT}

                            Run Keyword And Expect Error    *
                            ...  Get Range                  ${KEY}    ${CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Run Keyword And Expect Error    *
                            ...  Get object      ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       local_file_eacl

                            Get Range Hash                  ${KEY}    ${CID}    ${S_OID_USER}    ${EMPTY}       0:256



Cleanup
                            Cleanup Files
                            Get Docker Logs    acl_extended
