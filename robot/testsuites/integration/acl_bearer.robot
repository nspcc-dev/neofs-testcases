*** Settings ***
Variables   ../../variables/common.py

Library     Collections
Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neo.py
Library     ${RESOURCES}/neofs.py


*** Variables ***
&{FILE_USR_HEADER} =        key1=1    key2='abc1'
&{FILE_USR_HEADER_DEL} =    key1=del  key2=del
&{FILE_OTH_HEADER} =        key1=oth  key2=oth

*** Test cases ***
Extended ACL Operations
    [Documentation]     Testcase to validate NeoFS operations with extended ACL.
    [Tags]              ACL  NeoFS  NeoCLI BearerToken
    [Timeout]           20 min

    Generate Keys
    Generate file
    Prepare eACL Role rules
    Check Bearer
    
    
 
*** Keywords ***


Check Bearer
    Check eACL Deny and Allow All Bearer
    Check Container Inaccessible and Allow All Bearer
 




Generate Keys
    ${USER_KEY_GEN} =       Generate Neo private key
    ${OTHER_KEY_GEN} =      Generate Neo private key
    ${EACL_KEY_GEN} =       Form Privkey from String            782676b81a35c5f07325ec523e8521ee4946b6e5d4c6cd652dd0c3ba51ce03de
                            Get Neo public key                  ${EACL_KEY_GEN}
    ${SYSTEM_KEY_GEN} =	    Form Privkey from String            c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    


    ${SYSTEM_KEY_GEN_SN} =  Form Privkey from String            0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2


                            Set Global Variable     ${USER_KEY}                  ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}                 ${OTHER_KEY_GEN}
                            Set Global Variable     ${EACL_KEY}                  ${EACL_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}                ${SYSTEM_KEY_GEN}

                            Set Global Variable     ${SYSTEM_KEY_SN}      ${SYSTEM_KEY_GEN_SN}


Create Container Public
                            Log	                                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container                    ${USER_KEY}     0x2FFFFFFF
    [Return]                ${PUBLIC_CID_GEN}


Create Container Inaccessible
                            Log	                                Create Inaccessible Container
    ${PUBLIC_CID_GEN} =     Create container                    ${USER_KEY}     0x20000000
    [Return]                ${PUBLIC_CID_GEN}



Generate file
    # Generate small file
    ${FILE_S_GEN} =         Generate file of bytes              1024
                            Set Global Variable     ${FILE_S}          ${FILE_S_GEN}
 

Prepare eACL Role rules
                            Log	                    Set eACL for different Role cases
                            Set Global Variable     ${EACL_DENY_ALL_OTHER}        0007000000020000000100000001000000030000000000020000000300000001000000030000000000020000000200000001000000030000000000020000000500000001000000030000000000020000000400000001000000030000000000020000000600000001000000030000000000020000000700000001000000030000
                            Set Global Variable     ${EACL_ALLOW_ALL_OTHER}       0007000000010000000100000001000000030000000000010000000300000001000000030000000000010000000200000001000000030000000000010000000500000001000000030000000000010000000400000001000000030000000000010000000600000001000000030000000000010000000700000001000000030000
                            
                            Set Global Variable     ${EACL_DENY_ALL_USER}         0007000000020000000100000001000000010000000000020000000300000001000000010000000000020000000200000001000000010000000000020000000500000001000000010000000000020000000400000001000000010000000000020000000600000001000000010000000000020000000700000001000000010000
                            Set Global Variable     ${EACL_ALLOW_ALL_USER}        0007000000010000000100000001000000010000000000010000000300000001000000010000000000010000000200000001000000010000000000010000000500000001000000010000000000010000000400000001000000010000000000010000000600000001000000010000000000010000000700000001000000010000

                            Set Global Variable     ${EACL_DENY_ALL_SYSTEM}       0007000000020000000100000001000000020000000000020000000300000001000000020000000000020000000200000001000000020000000000020000000500000001000000020000000000020000000400000001000000020000000000020000000600000001000000020000000000020000000700000001000000020000
                            Set Global Variable     ${EACL_ALLOW_ALL_SYSTEM}      0007000000010000000100000001000000020000000000010000000300000001000000020000000000010000000200000001000000020000000000010000000500000001000000020000000000010000000400000001000000020000000000010000000600000001000000020000000000010000000700000001000000020000
 
                            
                            Set Global Variable     ${EACL_ALLOW_ALL_Pubkey}      000e000000010000000100000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000300000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000200000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000500000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000400000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000600000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000010000000700000001000000000001002103db43cba61ef948a65c20b326b9409911341436478dfdd7472c9af6b10bb60000000000020000000100000001000000030000000000020000000300000001000000030000000000020000000200000001000000030000000000020000000500000001000000030000000000020000000400000001000000030000000000020000000600000001000000030000000000020000000700000001000000030000

 
Check Container Inaccessible and Allow All Bearer
    ${CID} =                Create Container Inaccessible

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EACL_ALLOW_ALL_USER}              &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EACL_ALLOW_ALL_USER}              @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}              ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}



Check eACL Deny and Allow All Bearer
    ${CID} =                Create Container Public
    ${S_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}  &{FILE_USR_HEADER} 
    ${D_OID_USER} =         Put object to NeoFS                 ${USER_KEY}     ${FILE_S}            ${CID}            ${EMPTY}  &{FILE_USR_HEADER_DEL} 
    @{S_OBJ_H} =	        Create List	                        ${S_OID_USER}

 


                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              ${True}
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              0:256
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EMPTY}

                            Set eACL                            ${USER_KEY}     ${CID}        ${EACL_DENY_ALL_USER}
                            Sleep                               ${MORPH_BLOCK_TIMEOUT}

                            Run Keyword And Expect Error        *
                            ...  Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EMPTY}              &{FILE_USR_HEADER} 
                            Run Keyword And Expect Error        *
                            ...  Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              local_file_eacl
                            Run Keyword And Expect Error        *
                            ...  Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EMPTY}              @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Run Keyword And Expect Error        *
                            ...  Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              ${True}
                            Run Keyword And Expect Error        *
                            ...  Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}              0:256
                            Run Keyword And Expect Error        *
                            ...  Delete object                       ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EMPTY}


                            Put object to NeoFS                 ${USER_KEY}    ${FILE_S}     ${CID}                   ${EACL_ALLOW_ALL_USER}               &{FILE_OTH_HEADER} 
                            Get object from NeoFS               ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}               local_file_eacl
                            Search object                       ${USER_KEY}    ${CID}        ${EMPTY}                 ${EACL_ALLOW_ALL_USER}               @{S_OBJ_H}            &{FILE_USR_HEADER}
                            Head object                         ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}               ${True}
                            Get Range                           ${USER_KEY}    ${CID}        ${S_OID_USER}            ${EACL_ALLOW_ALL_USER}               0:256     
                            Delete object                       ${USER_KEY}    ${CID}        ${D_OID_USER}            ${EACL_ALLOW_ALL_USER}


  


Check eACL Deny and Allow All
    [Arguments]     ${KEY}       ${DENY_EACL}    ${ALLOW_EACL}

    


                             

