*** Settings ***
Variables   ../../../variables/common.py

Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Resource    common_steps_object.robot
 

*** Test cases ***
NeoFS Simple Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           6 min

                        Payment operations
                        Prepare container

    ${FILE} =           Generate file of bytes              ${SIMPLE_OBJ_SIZE}
    ${FILE_HASH} =      Get file hash                       ${FILE}


    ${S_OID} =          Put object                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${EMPTY}  
    ${H_OID} =          Put object                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER} 
    ${H_OID_OTH} =      Put object                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER_OTH}

                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}            ${S_OID}    
                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}            ${H_OID}    
                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}            ${H_OID_OTH}    

    @{S_OBJ_ALL} =	    Create List	                        ${S_OID}       ${H_OID}      ${H_OID_OTH}
    @{S_OBJ_H} =	    Create List	                        ${H_OID}
    @{S_OBJ_H_OTH} =    Create List	                        ${H_OID_OTH}

    ${GET_OBJ_S} =      Get object               ${PRIV_KEY}    ${CID}        ${S_OID}           ${EMPTY}       s_file_read
    ${GET_OBJ_H} =      Get object               ${PRIV_KEY}    ${CID}        ${H_OID}           ${EMPTY}       h_file_read
                                    
                        Verify file hash                    ${GET_OBJ_S}   ${FILE_HASH} 
                        Verify file hash                    ${GET_OBJ_H}   ${FILE_HASH} 

                        Get Range Hash                      ${PRIV_KEY}    ${CID}        ${S_OID}            ${EMPTY}       0:10
                        Get Range Hash                      ${PRIV_KEY}    ${CID}        ${H_OID}            ${EMPTY}       0:10

                        Get Range                           ${PRIV_KEY}    ${CID}        ${S_OID}            s_get_range    ${EMPTY}       0:10
                        Get Range                           ${PRIV_KEY}    ${CID}        ${H_OID}            h_get_range    ${EMPTY}       0:10
                       
                        Search object                       ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${EMPTY}                  ${S_OBJ_ALL}   
                        Search object                       ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${FILE_USR_HEADER}        ${S_OBJ_H}        
                        Search object                       ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${FILE_USR_HEADER_OTH}    ${S_OBJ_H_OTH}    

                        Head object                         ${PRIV_KEY}    ${CID}        ${S_OID}            ${EMPTY}             
                        Head object                         ${PRIV_KEY}    ${CID}        ${H_OID}            ${EMPTY}       ${FILE_USR_HEADER}
                          
    ${TOMBSTONE_S} =    Delete object                       ${PRIV_KEY}    ${CID}        ${S_OID}            ${EMPTY}
    ${TOMBSTONE_H} =    Delete object                       ${PRIV_KEY}    ${CID}        ${H_OID}            ${EMPTY}
     
                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${TOMBSTONE_S}     ${S_OID}    ${ADDR}
                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${TOMBSTONE_H}     ${H_OID}    ${ADDR}

                        Sleep                               2min
                        
                        Run Keyword And Expect Error        *       
                        ...  Get object          ${PRIV_KEY}    ${CID}        ${S_OID}           ${EMPTY}       ${GET_OBJ_S}

                        Run Keyword And Expect Error        *       
                        ...  Get object          ${PRIV_KEY}    ${CID}        ${H_OID}           ${EMPTY}       ${GET_OBJ_H}
    
    [Teardown]          Cleanup                             



*** Keywords ***

Cleanup
                        Cleanup Files                       
                        Get Docker Logs                     object_simple
 




