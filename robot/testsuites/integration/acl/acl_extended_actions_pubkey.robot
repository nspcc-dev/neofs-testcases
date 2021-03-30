*** Settings ***
Variables                   ../../../variables/common.py
Library                     Collections
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_extended.robot
     
*** Test cases ***
Extended ACL Operations
    [Documentation]         Testcase to validate NeoFS operations with extended ACL.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Prepare eACL Role rules

                            Log    Check extended ACL with simple object
                            Generate files    1024
                            Check eACL Deny All Other and Allow All Pubkey

                            Log    Check extended ACL with complex object
                            Generate files    70e+6
                            Check eACL Deny All Other and Allow All Pubkey
                             
                             
    [Teardown]              Cleanup  

    
*** Keywords ***

Check eACL Deny All Other and Allow All Pubkey

    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}            ${FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	               ${S_OID_USER}

                            Put object                          ${EACL_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object                          ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}            ${FILE_USR_HEADER}        ${S_OBJ_H}            
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}             
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}            s_get_range         ${EMPTY}            0:256
                            Get Range Hash                      ${EACL_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}    ${CID}        ${EACL_ALLOW_ALL_Pubkey}    --await

                            # The current ACL cache lifetime is 30 sec
                            Sleep       30s

                            Get eACL                            ${USER_KEY}    ${CID}

                            Run Keyword And Expect Error        *
                            ...  Put object                          ${OTHER_KEY}    ${FILE_S}     ${CID}            ${EMPTY}            ${FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object                          ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}            local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${OTHER_KEY}    ${CID}        ${EMPTY}          ${EMPTY}            ${FILE_USR_HEADER}      ${S_OBJ_H}            
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}            
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${OTHER_KEY}    ${CID}        ${S_OID_USER}     s_get_range     ${EMPTY}            0:256
                            Run Keyword And Expect Error        *
                            ...  Get Range Hash                      ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}        0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${OTHER_KEY}    ${CID}        ${S_OID_USER}     ${EMPTY}

                            Put object                          ${EACL_KEY}    ${FILE_S}     ${CID}                  ${EMPTY}            ${FILE_OTH_HEADER} 
                            Get object                          ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            local_file_eacl
                            Search object                       ${EACL_KEY}    ${CID}        ${EMPTY}                ${EMPTY}            ${FILE_USR_HEADER}     ${S_OBJ_H}
                            Head object                         ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            
                            Get Range                           ${EACL_KEY}    ${CID}        ${S_OID_USER}           s_get_range         ${EMPTY}            0:256
                            Get Range Hash                      ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}            0:256
                            Delete object                       ${EACL_KEY}    ${CID}        ${S_OID_USER}           ${EMPTY}


Cleanup
                            Cleanup Files     
                            Get Docker Logs    acl_extended
