*** Settings ***
Variables                   ../../../variables/common.py
  
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_basic.robot


*** Test cases ***
Basic ACL Operations for Read-Only Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Read-Only Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys

                            Create Containers
                            Generate file    1024
                            Check Read-Only Container
                            
                            Create Containers
                            Generate file    70e+6
                            Check Read-Only Container

    [Teardown]              Cleanup  
    

*** Keywords ***


Check Read-Only Container
    # Check Read Only container:

    # Put
    ${S_OID_USER} =         Put object                 ${USER_KEY}         ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object            ${OTHER_KEY}        ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
                            Run Keyword And Expect Error        *
                            ...  Put object            ${SYSTEM_KEY_IR}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}
    ${S_OID_SYS_SN} =       Put object                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${READONLY_CID}    ${EMPTY}    ${EMPTY}

    # Get
                            Get object               ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object               ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object               ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read 
                            Get object               ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read

    # Get Range
                            Get Range                           ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range                           ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash                      ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash                      ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_RO} =	        Create List	                        ${S_OID_USER}       ${S_OID_SYS_SN}     
                            Search object                       ${USER_KEY}         ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}
                            Search object                       ${OTHER_KEY}        ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}
                            Search object                       ${SYSTEM_KEY_IR}    ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}
                            Search object                       ${SYSTEM_KEY_SN}    ${READONLY_CID}    --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_RO}

 
    # Head
                            Head object                         ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object                         ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

    # Delete
                            Run Keyword And Expect Error        *       
                            ...  Delete object                  ${OTHER_KEY}        ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *  
                            ...  Delete object                  ${SYSTEM_KEY_IR}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                            Run Keyword And Expect Error        *  
                            ...  Delete object                  ${SYSTEM_KEY_SN}    ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}
                            Delete object                       ${USER_KEY}         ${READONLY_CID}    ${S_OID_USER}    ${EMPTY}



Cleanup
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    s_file_read    s_get_range  
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_basic