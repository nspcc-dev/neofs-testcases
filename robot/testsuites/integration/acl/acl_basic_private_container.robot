*** Settings ***
Variables                   ../../../variables/common.py
  
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_basic.robot


*** Test cases ***
Basic ACL Operations for Private Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Private Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
                            Create Containers
    
                            Generate file    1024
                            Check Private Container

                            Generate file    10e+6
                            Check Private Container

    [Teardown]              Cleanup  
    
 


*** Keywords ***

Check Private Container
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}         ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY} 
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${OTHER_KEY}        ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY}         
                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS            ${SYSTEM_KEY_IR}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_SN} =       Put object to NeoFS                 ${SYSTEM_KEY_SN}    ${FILE_S}    ${PRIV_CID}    ${EMPTY}    ${EMPTY} 

                        
    # Get
                            Get object from NeoFS               ${USER_KEY}         ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS          ${OTHER_KEY}        ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Get object from NeoFS               ${SYSTEM_KEY_IR}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read
                            Get object from NeoFS               ${SYSTEM_KEY_SN}    ${PRIV_CID}    ${S_OID_USER}    ${EMPTY}      s_file_read 

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
    @{CLEANUP_FILES} =      Create List	       ${FILE_S}    s_file_read    s_get_range  
                            Cleanup Files      @{CLEANUP_FILES}
                            Get Docker Logs    acl_basic