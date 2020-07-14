*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neo.py
Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment.py
Library     ${RESOURCES}/assertions.py
Library     ${RESOURCES}/neo.py

*** Variables ***
&{FILE_USR_HEADER} =    key1=1  key2='abc'


*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    ${PRIV_KEY} =       Generate Neo private key
    ${PUB_KEY} =        Get Neo public key                  ${PRIV_KEY}
    ${ADDR} =           Get Neo address                     ${PRIV_KEY}
#    ${TX} =             Request NeoFS Deposit               ${PUB_KEY}       
#                        Wait Until Keyword Succeeds         1 min          15 sec        
#                        ...  Transaction accepted in block  ${TX}
#                        Get Transaction                     ${TX}
# Due to develop branch with zero-payment for container and different blockchains for payment.
# Temporarily removed.
#    ${BALANCE} =        Wait Until Keyword Succeeds         10 min         1 min        
#                        ...  Get Balance                    ${PUB_KEY}  
#                        Expected Balance                    ${PUB_KEY}     0             50
    ${CID} =            Create container                    ${PRIV_KEY}
                        Container Existing                  ${PRIV_KEY}    ${CID}
# Due to develop branch with zero-payment for container and different blockchains for payment.
# Fail will be ignored temporarily. Temporarily removed.
#                        Run Keyword And Ignore Error
#                        ...  Wait Until Keyword Succeeds    2 min          30 sec
#                        ...  Expected Balance               ${PUB_KEY}     ${BALANCE}    -0.00001424
    ${FILE} =           Generate file of bytes              1024
    ${FILE_HASH} =      Get file hash                       ${FILE}
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}         
    ${H_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}         &{FILE_USR_HEADER} 

                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}         ${S_OID}    
    ${SGID} =           Create storage group                ${PRIV_KEY}    ${CID}        ${S_OID}       ${H_OID}
    
    @{S_OBJ_SG} =	    Create List	                        ${SGID}
    @{S_OBJ_ALL} =	    Create List	                        ${S_OID}       ${H_OID}      ${SGID}
    @{S_OBJ_H} =	    Create List	                        ${H_OID}

                        Search object                       ${PRIV_KEY}    ${CID}        --sg           @{S_OBJ_SG}               
                        Get storage group                   ${PRIV_KEY}    ${CID}        ${SGID}
                        Get object from NeoFS               ${PRIV_KEY}    ${CID}        ${S_OID}       s_file_read
                        Get object from NeoFS               ${PRIV_KEY}    ${CID}        ${S_OID}       h_file_read
                        Search object                       ${PRIV_KEY}    ${CID}        ${EMPTY}       @{S_OBJ_ALL}   
                        Search object                       ${PRIV_KEY}    ${CID}        ${EMPTY}       @{S_OBJ_H}       &{FILE_USR_HEADER} 
                        Head object                         ${PRIV_KEY}    ${CID}        ${S_OID}       ${True}     
                        Head object                         ${PRIV_KEY}    ${CID}        ${H_OID}       ${True}          &{FILE_USR_HEADER}
                        
                        Run Keyword And Expect Error        REGEXP:User header (\\w+=\\w+\\s?)+ was not found              
                        ...  Head object                    ${PRIV_KEY}    ${CID}        ${H_OID}       ${False}         &{FILE_USR_HEADER}                    
                        
                        Verify file hash                    s_file_read    ${FILE_HASH} 
                        Verify file hash                    h_file_read    ${FILE_HASH} 
    &{ID_OBJ_S} =	    Create Dictionary	                ID=${S_OID}
                        Delete object                       ${PRIV_KEY}    ${CID}        ${S_OID}
                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${S_OID}
# Removed due to tombstones zombies.
#                        Wait Until Keyword Succeeds         2 min          30 sec        
#                        ...  Search object                  ${PRIV_KEY}    ${CID}        ${EMPTY}       @{EMPTY}        &{ID_OBJ_S}
#                        Run Keyword And Expect Error        *       
#                        ...  Get object from NeoFS          ${PRIV_KEY}    ${CID}        ${S_OID}       s_file_read_2
    &{ID_OBJ_H} =	    Create Dictionary	                ID=${H_OID}
                        Delete object                       ${PRIV_KEY}    ${CID}        ${H_OID}
                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${H_OID}

# Removed due to tombstones zombies.	
#                       Search object                       ${PRIV_KEY}    ${CID}        ${EMPTY}       @{EMPTY}        &{FILE_USR_HEADER}                        
#                       Wait Until Keyword Succeeds         2 min          30 sec 
#                       ...  Search object                  ${PRIV_KEY}    ${CID}        ${EMPTY}       @{EMPTY}        &{ID_OBJ_H}
#                       Run Keyword And Expect Error        *                 
#                       ...  Get object from NeoFS          ${PRIV_KEY}    ${CID}        ${H_OID}       s_file_read_2


    &{SGID_OBJ} =	    Create Dictionary	                ID=${SGID}
                        Delete object                       ${PRIV_KEY}    ${CID}        ${SGID}
                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${SGID}
# Removed due to tombstones zombies.
#                        Search object                       ${PRIV_KEY}    ${CID}        --sg           @{EMPTY}
#                        Wait Until Keyword Succeeds         2 min          30 sec 
#                        ...  Search object                  ${PRIV_KEY}    ${CID}        ${EMPTY}       @{EMPTY}        &{SGID_OBJ} 
#                        Run Keyword And Expect Error        *              
#                        ...  Get object from NeoFS          ${PRIV_KEY}    ${CID}        ${SGID}        s_file_read_2

                        Cleanup File                        ${FILE}   
                        Cleanup File                        s_file_read
                        Cleanup File                        h_file_read
                        Run Keyword And Expect Error        Error: 's_file_read_2' file not found              
                        ...  Cleanup File                   s_file_read_2

