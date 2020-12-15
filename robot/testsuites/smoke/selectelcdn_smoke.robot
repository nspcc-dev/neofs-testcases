# -*- coding: robot -*-

*** Settings ***
Variables                      ../../variables/common.py
Variables                      ../../variables/selectelcdn_smoke.py

Library                        Collections
Library                        ${RESOURCES}/neofs.py
Library                        ${RESOURCES}/payment_neogo.py
Library                        ${RESOURCES}/gates.py



*** Test cases ***

NeoFS Storage Smoke
    [Documentation]             Creates container and does PUT, GET and LIST on it via CLI and via HTTP Gate
    [Timeout]                   5 min

    # user.key, container owner
    ${PRIV_KEY} =	            Form WIF from String    1dd37fba80fec4e6a6f13fd708d8dcb3b29def768017052f6c930fa1c5d90bbb

    ${WALLET} =                 Init wallet
       
                                Generate wallet from WIF    ${WALLET}      ${PRIV_KEY}
    ${ADDR} =                   Dump Address                ${WALLET}  
                                Dump PrivKey                ${WALLET}      ${ADDR}
    ${SCRIPT_HASH} =            Get ScripHash               ${PRIV_KEY}  



    ${TX} =                     Transfer Mainnet Gas    ${WALLET_ROOT}     NbTiM6h8r99kpRtb428XcsUk1TzKed2gTc      ${ADDR}     5    one
                                Sleep  30sec
                                Mainnet Balance                     ${ADDR}
                                Get Balance                         ${PRIV_KEY}    

                                Wait Until Keyword Succeeds         2 min       15 sec        
                                ...  Transaction accepted in block  ${TX}
                        
                                Get Transaction                     ${TX}


    ${TX_DEPOSIT} =             NeoFS Deposit                       ${WALLET}        ${ADDR}    ${SCRIPT_HASH}    5    
                                Wait Until Keyword Succeeds         1 min            15 sec
                                ...  Transaction accepted in block  ${TX_DEPOSIT}
                                Get Transaction                     ${TX_DEPOSIT}
                                Get Balance                         ${PRIV_KEY}    

    ${CID}
    ...  ${BUCKET}
    ...  ${ACCESS_KEY_ID} 
    ...  ${SEC_ACCESS_KEY} 
    ...  ${OWNER_PRIV_KEY} =    Init S3 Credentials    ${PRIV_KEY}         keys/s3_selectel_hcs.pub.key

    ${S3_CLIENT} =              Config S3 client       ${ACCESS_KEY_ID}    ${SEC_ACCESS_KEY} 

                               # List buckets S3        ${S3_CLIENT}

    ${CONTEINERS_LIST} =        Container List               ${PRIV_KEY}      
                                List Should Contain Value    ${CONTEINERS_LIST}    ${CID}

    ${FILE_S3} =                Generate file of bytes    1024
    ${FILE_S3_HASH} =           Get file hash             ${FILE_S3}
    ${FILE_S3_NAME} =           Get file name             ${FILE_S3} 

    ${FILE_FS} =                Generate file of bytes    1024
    ${FILE_FS_HASH} =           Get file hash             ${FILE_FS}
    ${FILE_FS_NAME} =           Get file name             ${FILE_FS}  

    # PUT NeoFS, S3 -> GET NeoFS, S3, HTTP-gate each object (x2) -> check uploaded hashes

    ${OID_FS} =                 Put object to NeoFS    ${PRIV_KEY}     ${FILE_FS}    ${CID}        ${EMPTY}    ${EMPTY}
                                Put object S3          ${S3_CLIENT}    ${BUCKET}     ${FILE_S3}

    ${OID_LIST_S3} =            Search object    ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       FileName=${FILE_S3_NAME} 
    ${OID_S3} =                 Get From List    ${OID_LIST_S3}    0

                                Get object from NeoFS    ${PRIV_KEY}     ${CID}       ${OID_FS}           ${EMPTY}         s_file_read
                                Get object S3            ${S3_CLIENT}    ${BUCKET}    ${FILE_FS_NAME}     s3_obj_get_s3
    ${FILEPATH_FS} =            Get via HTTP Gate        ${CID}          ${OID_FS}

                                Get object from NeoFS    ${PRIV_KEY}     ${CID}       ${OID_S3}           ${EMPTY}         s_file_read
                                Get object S3            ${S3_CLIENT}    ${BUCKET}    ${FILE_S3_NAME}     s3_obj_get_s3
    ${FILEPATH_S3} =            Get via HTTP Gate        ${CID}          ${OID_S3}

    [Teardown]                  Cleanup Files            ${FILE_S3}    ${FILE_FS}    s_file_read