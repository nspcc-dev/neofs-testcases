*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment_neogo.py

*** Variables ***
${FILE_USR_HEADER} =        key1=1,key2=abc
${FILE_USR_HEADER_OTH} =    key1=2


*** Test cases ***
NeoFS Complex Object Operations
    [Documentation]     Testcase to validate NeoFS operations with simple object.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    ${WALLET} =         Init wallet
                        Generate wallet         ${WALLET}
    ${ADDR} =           Dump Address            ${WALLET}
    ${PRIV_KEY} =       Dump PrivKey            ${WALLET}               ${ADDR}
    ${TX} =             Transfer Mainnet Gas    wallets/wallet.json     NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     55
                        Wait Until Keyword Succeeds         1 min       15 sec        
                        ...  Transaction accepted in block  ${TX}
                        Get Transaction                     ${TX}
                        Expexted Mainnet Balance            ${ADDR}     55

    ${SCRIPT_HASH} =    Get ScripHash           ${PRIV_KEY}  

    ${TX_DEPOSIT} =     NeoFS Deposit                       ${WALLET}      ${ADDR}       ${SCRIPT_HASH}      50
                        Wait Until Keyword Succeeds         1 min          15 sec        
                        ...  Transaction accepted in block  ${TX_DEPOSIT}
                        Get Transaction                     ${TX_DEPOSIT}

    ${BALANCE} =        Wait Until Keyword Succeeds         5 min         1 min        
                        ...  Expected Balance               ${PRIV_KEY}    0             50

    ${CID} =            Create container                    ${PRIV_KEY}
                        Container Existing                  ${PRIV_KEY}    ${CID}
                        
                        Wait Until Keyword Succeeds         2 min          30 sec
                        ...  Expected Balance               ${PRIV_KEY}    50            -7e-08

    ${SIZE} =           Set Variable	                    20e+6
    ${FILE} =           Generate file of bytes              ${SIZE}
    ${FILE_HASH} =      Get file hash                       ${FILE}

    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${EMPTY}  
    ${H_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER} 
    ${H_OID_OTH} =      Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER_OTH}   

    Should Be True     '${S_OID}'!='${H_OID}' and '${H_OID}'!='${H_OID_OTH}'

                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}         ${S_OID}    
                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}         ${H_OID}  
                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}         ${H_OID_OTH}  

    @{S_OBJ_ALL} =	    Create List	                        ${S_OID}       ${H_OID}     ${H_OID_OTH}   
    @{S_OBJ_H} =	    Create List	                        ${H_OID}
    @{S_OBJ_H_OTH} =    Create List	                        ${H_OID_OTH}

                        Run Keyword And Expect Error        *       
                        ...  Search object                  ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${EMPTY}                @{S_OBJ_ALL}   

                        Get object from NeoFS               ${PRIV_KEY}    ${CID}        ${S_OID}           ${EMPTY}       s_file_read
                        Get object from NeoFS               ${PRIV_KEY}    ${CID}        ${H_OID}           ${EMPTY}       h_file_read
                                    
                        Verify file hash                    s_file_read    ${FILE_HASH} 
                        Verify file hash                    h_file_read    ${FILE_HASH} 

                        Get Range Hash                      ${PRIV_KEY}    ${CID}        ${S_OID}          ${EMPTY}       0:10
                        Get Range Hash                      ${PRIV_KEY}    ${CID}        ${H_OID}          ${EMPTY}       0:10

                        Get Range                           ${PRIV_KEY}    ${CID}        ${S_OID}          s_get_range    ${EMPTY}       0:10
                        Get Range                           ${PRIV_KEY}    ${CID}        ${H_OID}          h_get_range    ${EMPTY}       0:10

                        Search object                       ${PRIV_KEY}    ${CID}        --root            ${EMPTY}       ${EMPTY}                @{S_OBJ_ALL}   
                        Search object                       ${PRIV_KEY}    ${CID}        --root            ${EMPTY}       ${FILE_USR_HEADER}      @{S_OBJ_H}    
                        Search object                       ${PRIV_KEY}    ${CID}        --root            ${EMPTY}       ${FILE_USR_HEADER_OTH}  @{S_OBJ_H_OTH} 
                        
                        Head object                         ${PRIV_KEY}    ${CID}        ${S_OID}          ${EMPTY}             
                        Head object                         ${PRIV_KEY}    ${CID}        ${H_OID}          ${EMPTY}       ${FILE_USR_HEADER}
       
                        Verify Split Chain                  ${PRIV_KEY}    ${CID}        ${S_OID}
                        Verify Split Chain                  ${PRIV_KEY}    ${CID}        ${H_OID}

    ${TOMBSTONE_S} =    Delete object                       ${PRIV_KEY}    ${CID}        ${S_OID}          ${EMPTY}
    ${TOMBSTONE_H} =    Delete object                       ${PRIV_KEY}    ${CID}        ${H_OID}          ${EMPTY}

                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${TOMBSTONE_S}     ${S_OID}    ${ADDR}
                        Verify Head tombstone               ${PRIV_KEY}    ${CID}        ${TOMBSTONE_H}     ${H_OID}    ${ADDR}

                        Sleep                               2min
                        
                        Run Keyword And Expect Error        *       
                        ...  Get object from NeoFS          ${PRIV_KEY}    ${CID}        ${S_OID}           ${EMPTY}       s_file_read
                        Run Keyword And Expect Error        *       
                        ...  Get object from NeoFS          ${PRIV_KEY}    ${CID}        ${H_OID}           ${EMPTY}       h_file_read
   
    [Teardown]          Cleanup                             ${FILE}



*** Keywords ***

Cleanup
    [Arguments]         ${FILE}
    @{CLEANUP_FILES} =  Create List	                        ${FILE}    s_file_read    h_file_read    s_get_range    h_get_range
                        Cleanup Files                       @{CLEANUP_FILES}
                        Get Docker Logs                     object_complex
 




