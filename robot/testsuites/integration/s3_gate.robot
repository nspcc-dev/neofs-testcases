*** Settings ***
Variables                   ../../variables/common.py

Library                     ${RESOURCES}/neofs.py
Library                     ${RESOURCES}/payment_neogo.py
Library                     ${RESOURCES}/gates.py

*** Variables ***
${FILE_USR_HEADER} =        FileName=cat1.jpg





*** Test cases ***
NeoFS S3 Gateway 
    [Documentation]         Execute operations via S3 Gate
    [Timeout]               5 min


    ${PRIV_KEY} =	        Form WIF from String    1dd37fba80fec4e6a6f13fd708d8dcb3b29def768017052f6c930fa1c5d90bbb
    ${WALLET} =             Init wallet
       
                            Generate wallet from WIF  ${WALLET}    ${PRIV_KEY}
    
    ${ADDR} =               Dump Address            ${WALLET}  
                            Dump PrivKey            ${WALLET}              ${ADDR}

    ${TX} =                 Transfer Mainnet Gas    wallets/wallet.json    NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx    ${ADDR}    55

                            Wait Until Keyword Succeeds         1 min      15 sec        
                            ...  Transaction accepted in block  ${TX}
                            Get Transaction                     ${TX}

#                            Expexted Mainnet Balance            ${ADDR}    55

    ${SCRIPT_HASH} =        Get ScripHash                       ${PRIV_KEY}  

    ${TX_DEPOSIT} =         NeoFS Deposit                       ${WALLET}    ${ADDR}    ${SCRIPT_HASH}    50      
                            Wait Until Keyword Succeeds         1 min        15 sec
                            ...  Transaction accepted in block  ${TX_DEPOSIT}
                            Get Transaction                     ${TX_DEPOSIT}



    ${FILE} =               Generate file of bytes              256
    ${FILE_HASH} =          Get file hash                       ${FILE}
    ${FILE_NAME} =          Get file name                       ${FILE}
   
                            Container List                      ${PRIV_KEY}     


    
    ${CID}
    ...  ${BUCKET}
    ...  ${ACCESS_KEY_ID} 
    ...  ${SEC_ACCESS_KEY} 
    ...  ${OWNER_PRIV_KEY} =    Init S3 Credentials                 ${PRIV_KEY}   

                                Container List                      ${PRIV_KEY}      

        
                                Search object        ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${EMPTY}
                                Search object        ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${FILE_USR_HEADER} 
                                Search object        ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       FileName=${FILE_NAME} 



    ${S3_CLIENT} =              Config S3 client     ${ACCESS_KEY_ID}   ${SEC_ACCESS_KEY}  
                                List buckets S3      ${S3_CLIENT} 
                                Put object S3        ${S3_CLIENT}    ${BUCKET}       ${FILE}


    #${S_OID} =                 Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         ${FILE_USR_HEADER}  



                                List objects S3      ${S3_CLIENT}    ${BUCKET}
                                List objects S3 v2   ${S3_CLIENT}    ${BUCKET}

                                Search object        ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${EMPTY}
                                Search object        ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       ${FILE_USR_HEADER} 
                                Search object        ${PRIV_KEY}    ${CID}        ${EMPTY}            ${EMPTY}       FileName=${FILE_NAME} 


                                Get object S3        ${S3_CLIENT}    ${BUCKET}    ${FILE_NAME}  ${ACCESS_KEY_ID}   ${SEC_ACCESS_KEY} 

                                Delete object S3     ${S3_CLIENT}    ${BUCKET}    ${FILE_NAME}
                                List objects S3      ${S3_CLIENT}    ${BUCKET}
    
#                               List objects S3     ${PRIV_KEY}    ${CID}    

                            