*** Variables ***
${FILE_USR_HEADER} =    key1=1,key2=abc
${FILE_USR_HEADER_OTH} =    key1=2
${UNEXIST_OID} =    B2DKvkHnLnPvapbDgfpU1oVUPuXQo5LTfKVxmNDZXQff
${TRANSFER_AMOUNT} =    15
${DEPOSIT_AMOUNT} =    10

*** Keywords ***

Payment operations
    ${WALLET} =         Init wallet
                        Generate wallet         ${WALLET}
    ${ADDR} =           Dump Address            ${WALLET}
    ${PRIV_KEY} =       Dump PrivKey            ${WALLET}    ${ADDR}
    ${TX} =             Transfer Mainnet Gas    ${MAINNET_WALLET_PATH}    ${DEF_WALLET_ADDR}    ${ADDR}     ${TRANSFER_AMOUNT}

                        Wait Until Keyword Succeeds           ${BASENET_WAIT_TIME}    ${BASENET_BLOCK_TIME}        
                        ...  Transaction accepted in block    ${TX}
                        Get Transaction                       ${TX}
                        Expected Mainnet Balance              ${ADDR}    ${TRANSFER_AMOUNT}

    ${SCRIPT_HASH} =    Get ScriptHash                        ${PRIV_KEY}  

    ${TX_DEPOSIT} =     NeoFS Deposit                         ${WALLET}    ${ADDR}    ${SCRIPT_HASH}    ${DEPOSIT_AMOUNT}
                        Wait Until Keyword Succeeds           ${BASENET_WAIT_TIME}    ${BASENET_BLOCK_TIME}      
                        ...  Transaction accepted in block    ${TX_DEPOSIT}
                        Get Transaction                       ${TX_DEPOSIT}

    ${BALANCE} =        Wait Until Keyword Succeeds           ${NEOFS_EPOCH_TIMEOUT}    ${MORPH_BLOCK_TIME}      
                        ...  Expected Balance                 ${PRIV_KEY}    0    ${DEPOSIT_AMOUNT}

                        Set Global Variable                   ${PRIV_KEY}    ${PRIV_KEY}
                        Set Global Variable                   ${ADDR}    ${ADDR}


Prepare container
    ${CID} =            Create container                      ${PRIV_KEY}
                        Container Existing                    ${PRIV_KEY}    ${CID}
                        
                        Wait Until Keyword Succeeds           ${NEOFS_EPOCH_TIMEOUT}    ${MORPH_BLOCK_TIME}
                        ...  Expected Balance                 ${PRIV_KEY}    ${DEPOSIT_AMOUNT}    ${NEOFS_CREATE_CONTAINER_GAS_FEE}

                        Set Global Variable                   ${CID}    ${CID}
                        