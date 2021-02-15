*** Settings ***
Variables   ../../../variables/common.py
Library     Collections
Library     ../${RESOURCES}/neofs.py
Library     ../${RESOURCES}/payment_neogo.py
Resource    common_steps_object.robot


*** Test cases ***
NeoFS Complex Object Operations
    [Documentation]     Testcase to validate NeoFS operations with Storagegroup.
    [Tags]              Object  NeoFS  NeoCLI
    [Timeout]           20 min

    ${WALLET} =         Init wallet
                        Generate wallet         ${WALLET}
    ${ADDR} =           Dump Address            ${WALLET}
    ${PRIV_KEY} =       Dump PrivKey            ${WALLET}               ${ADDR}
    ${TX} =             Transfer Mainnet Gas    wallets/wallet.json     NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     15
                        Wait Until Keyword Succeeds         1 min       15 sec        
                        ...  Transaction accepted in block  ${TX}
                        Get Transaction                     ${TX}
                        Expexted Mainnet Balance            ${ADDR}     15

    ${SCRIPT_HASH} =    Get ScripHash           ${PRIV_KEY}  

    ${TX_DEPOSIT} =     NeoFS Deposit           ${WALLET}               ${ADDR}     ${SCRIPT_HASH}      10
                        Wait Until Keyword Succeeds         1 min          15 sec        
                        ...  Transaction accepted in block  ${TX_DEPOSIT}
                        Get Transaction                     ${TX_DEPOSIT}

    ${BALANCE} =        Wait Until Keyword Succeeds         5 min         1 min        
                        ...  Expected Balance               ${PRIV_KEY}    0             10

    ${CID} =            Create container                    ${PRIV_KEY}
                        Container Existing                  ${PRIV_KEY}    ${CID}

    ${FILE_S} =           Generate file of bytes            70e+6
    ${FILE_HASH_S} =      Get file hash                     ${FILE_S}


    # Put two Simple Object
    ${S_OID_1} =        Put object    ${PRIV_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${EMPTY}  
    ${S_OID_2} =        Put object    ${PRIV_KEY}    ${FILE_S}    ${CID}    ${EMPTY}    ${FILE_USR_HEADER} 
    
    @{S_OBJ_ALL} =	    Create List    ${S_OID_1}    ${S_OID_2} 
    
                        Log    Storage group with 1 object
    ${SG_OID_1} =       Put Storagegroup    ${PRIV_KEY}    ${CID}   ${EMPTY}    ${S_OID_1}
                        List Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_1}
    @{SPLIT_OBJ_1} =    Get Split objects    ${PRIV_KEY}    ${CID}   ${S_OID_1}
                        Get Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_1}    70000000    @{SPLIT_OBJ_1}
    ${Tombstone} =      Delete Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_1}
                        Verify Head tombstone    ${PRIV_KEY}    ${CID}    ${Tombstone}    ${SG_OID_1}    ${ADDR}
                        Run Keyword And Expect Error    *       
                        ...  Get Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_1}    70000000    @{SPLIT_OBJ_1}
                        List Storagegroup    ${PRIV_KEY}    ${CID}    @{EMPTY}


                        Log    Storage group with 2 objects
    ${SG_OID_2} =       Put Storagegroup    ${PRIV_KEY}    ${CID}    ${EMPTY}    @{S_OBJ_ALL}
                        List Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_2}
    @{SPLIT_OBJ_2} =    Get Split objects    ${PRIV_KEY}    ${CID}   ${S_OID_2}
    @{SPLIT_OBJ_ALL} =  Combine Lists    ${SPLIT_OBJ_1}    ${SPLIT_OBJ_2}
                        Get Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_2}    140000000    @{SPLIT_OBJ_ALL}
    ${Tombstone} =      Delete Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_2}
                        Verify Head tombstone    ${PRIV_KEY}    ${CID}    ${Tombstone}    ${SG_OID_2}    ${ADDR}
                        Run Keyword And Expect Error    *       
                        ...  Get Storagegroup    ${PRIV_KEY}    ${CID}    ${SG_OID_2}    140000000    @{SPLIT_OBJ_ALL}
                        List Storagegroup    ${PRIV_KEY}    ${CID}    @{EMPTY}

                        Log    Incorrect input

                        Run Keyword And Expect Error    *       
                        ...  Put Storagegroup    ${PRIV_KEY}    ${CID}    ${EMPTY}    ${UNEXIST_OID}
                        Run Keyword And Expect Error    *       
                        ...  Delete Storagegroup    ${PRIV_KEY}    ${CID}    ${UNEXIST_OID}

    [Teardown]          Cleanup                             ${FILE_S}

*** Keywords ***

Cleanup
    [Arguments]         ${FILE}

    @{CLEANUP_FILES} =  Create List	                        ${FILE}    
                        Cleanup Files                       @{CLEANUP_FILES}
                        Get Docker Logs                     object_storage_group_complex




