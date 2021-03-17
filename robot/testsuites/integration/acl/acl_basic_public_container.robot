*** Settings ***
Variables                   ../../../variables/common.py
Library                     ../${RESOURCES}/neofs.py
Library                     ../${RESOURCES}/payment_neogo.py

Resource                    common_steps_acl_basic.robot


*** Test cases ***
Basic ACL Operations for Public Container
    [Documentation]         Testcase to validate NeoFS operations with ACL for Public Container.
    [Tags]                  ACL  NeoFS  NeoCLI
    [Timeout]               20 min

                            Generate Keys
    
                            Create Containers
                            Generate file    1024
                            Check Public Container

                            Create Containers
                            Generate file    70e+6
                            Check Public container

    [Teardown]              Cleanup  
    
 
*** Keywords ***

Check Public Container

    # Put
    ${S_OID_USER} =         Put object        ${USER_KEY}         ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_OTHER} =        Put object        ${OTHER_KEY}        ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_IR} =       Put object        ${SYSTEM_KEY_IR}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 
    ${S_OID_SYS_SN} =       Put object        ${SYSTEM_KEY_SN}    ${FILE_S}    ${PUBLIC_CID}    ${EMPTY}    ${EMPTY} 

    # Get
                            Get object        ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object        ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object        ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read
                            Get object        ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    s_file_read 

    # Get Range
                            Get Range         ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range         ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range         ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256
                            Get Range         ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    s_get_range    ${EMPTY}    0:256


    # Get Range Hash
                            Get Range Hash    ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256
                            Get Range Hash    ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    0:256

    # Search
    @{S_OBJ_PRIV} =	        Create List	      ${S_OID_USER}       ${S_OID_OTHER}    ${S_OID_SYS_SN}    ${S_OID_SYS_IR}
                            Search object     ${USER_KEY}         ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object     ${OTHER_KEY}        ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object     ${SYSTEM_KEY_IR}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}
                            Search object     ${SYSTEM_KEY_SN}    ${PUBLIC_CID}     --root    ${EMPTY}    ${EMPTY}    ${S_OBJ_PRIV}

    # Head
                            Head object       ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object       ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}
                            Head object       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_USER}    ${EMPTY}    ${EMPTY}

                            Head object       ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object       ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}
                            Head object       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}    ${EMPTY}    ${EMPTY}

                            Head object       ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object       ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object       ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}
                            Head object       ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}    ${EMPTY}


    # Delete
                            Delete object     ${USER_KEY}         ${PUBLIC_CID}    ${S_OID_SYS_IR}    ${EMPTY}     
                            Delete object     ${OTHER_KEY}        ${PUBLIC_CID}    ${S_OID_SYS_SN}    ${EMPTY}
                            Delete object     ${SYSTEM_KEY_IR}    ${PUBLIC_CID}    ${S_OID_USER}      ${EMPTY}  
                            Delete object     ${SYSTEM_KEY_SN}    ${PUBLIC_CID}    ${S_OID_OTHER}     ${EMPTY}



Cleanup
                            Cleanup Files