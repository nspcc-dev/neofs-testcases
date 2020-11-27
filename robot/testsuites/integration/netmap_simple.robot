*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/assertions.py
Library     ${RESOURCES}/payment_neogo.py
 

*** Test cases ***
NeoFS Simple Netmap
    [Documentation]     Testcase to validate NeoFS Netmap.
    [Tags]              Netmap  NeoFS  NeoCLI
    [Timeout]           20 min

    Generate Key and Pre-payment
    Generate file


    

    # https://github.com/nspcc-dev/neofs-api-go/issues/212
    # Validate Policy     REP 2 IN X CBF 2 SELECT 2 FROM * AS X         2       @{EMPTY}

    Validate Policy     REP 2 IN X CBF 1 SELECT 2 FROM * AS X           2       @{EMPTY}

    Validate Policy     REP 3 IN X CBF 1 SELECT 3 FROM * AS X           3       @{EMPTY}

    Validate Policy     REP 1 IN X CBF 1 SELECT 1 FROM * AS X           1       @{EMPTY}

    # https://github.com/nspcc-dev/neofs-api-go/issues/212
    # Validate Policy     REP 1 IN X CBF 2 SELECT 1 FROM * AS X         1       @{EMPTY}

    Validate Policy     REP 4 IN X CBF 1 SELECT 4 FROM * AS X           4       @{EMPTY}

    Validate Policy     REP 2 IN X CBF 1 SELECT 4 FROM * AS X           2       @{EMPTY}

    @{EXPECTED} =	    Create List     s01.neofs.devenv:8080  s02.neofs.devenv:8080  s03.neofs.devenv:8080  s04.neofs.devenv:8080
    Validate Policy     REP 4 IN X CBF 1 SELECT 4 FROM * AS X           4       @{EXPECTED}   

    @{EXPECTED} =	    Create List     s03.neofs.devenv:8080
    Validate Policy     REP 1 IN LOC_PLACE CBF 1 SELECT 1 FROM LOC_SW AS LOC_PLACE FILTER Country EQ Sweden AS LOC_SW           1       @{EXPECTED}   

    @{EXPECTED} =	    Create List     s02.neofs.devenv:8080
    Validate Policy     REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER City EQ 'Saint-Petersburg' AS LOC_SPB           1       @{EXPECTED}   


    # https://github.com/nspcc-dev/neofs-api-go/issues/215
    # @{EXPECTED} =	    Create List     s01.neofs.devenv:8080  s02.neofs.devenv:8080
    # Validate Policy     REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE FILTER City EQ 'Saint-Petersburg' AS LOC_SPB FILTER City EQ 'Moscow' AS LOC_MSK          2       @{EXPECTED}   


# REP 1 IN LOC_SPB_PLACE REP 1 IN LOC_MSK_PLACE CBF 1 SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE FILTER City EQ 'Saint-Petersburg' AS LOC_SPB FILTER City EQ 'Moscow' AS LOC_MSK 
# REP 1 IN LOC_SPB_PLACE
# REP 1 IN LOC_MSK_PLACE
# CBF 1 
# SELECT 1 FROM LOC_SPB AS LOC_SPB_PLACE
# SELECT 1 FROM LOC_MSK AS LOC_MSK_PLACE
# FILTER City EQ 'Saint-Petersburg' AS LOC_SPB 
# FILTER City EQ 'Moscow' AS LOC_MSK 

    # https://github.com/nspcc-dev/neofs-api-go/issues/213
    # @{EXPECTED} =	    Create List     s01.neofs.devenv:8080  s02.neofs.devenv:8080  s03.neofs.devenv:8080  s04.neofs.devenv:8080
    # Validate Policy     REP 4 CBF 1 SELECT 4 FROM LOC_EU FILTER Location EQ Europe AS LOC_EU           4       @{EXPECTED}   

    @{EXPECTED} =	    Create List     s02.neofs.devenv:8080
    Validate Policy     REP 1 CBF 1 SELECT 1 FROM LOC_SPB FILTER City NE 'Moscow' AND City NE 'Stockholm' AND City NE 'Helsinki' AS LOC_SPB           1       @{EXPECTED}   

    # https://github.com/nspcc-dev/neofs-api-go/issues/213
    # @{EXPECTED} =	    Create List     s01.neofs.devenv:8080  s02.neofs.devenv:8080
    # Validate Policy     REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER City NE 'Stockholm' AND City NE 'Helsinki' AS LOC_RU           2       @{EXPECTED}   


    # https://github.com/nspcc-dev/neofs-api-go/issues/214
    # @{EXPECTED} =	    Create List     s01.neofs.devenv:8080    s02.neofs.devenv:8080
    # Validate Policy     REP 2 CBF 1 SELECT 2 FROM LOC_RU FILTER Country EQ 'Russia' AS LOC_RU           2       @{EXPECTED}   

    # https://github.com/nspcc-dev/neofs-api-go/issues/214
    # @{EXPECTED} =	    Create List     s03.neofs.devenv:8080    s04.neofs.devenv:8080
    # Validate Policy     REP 2 CBF 1 SELECT 2 FROM LOC_EU FILTER Country NE 'Russia' AS LOC_EU           2       @{EXPECTED} 
 
 # Log	                                Operation should be failed with container rule "RF 1 SELECT 6 Node"
 #   ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 6 Node
 
*** Keywords ***




NeoFS Simple Netmap CONTINUE1
   

 

                        Log	                                Operation should be failed with container rule "RF 2 SELECT 2 Node FILTER Country NE GB Country NE DE"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 2 SELECT 2 Node FILTER Country NE GB Country NE DE
                        Container Existing                  ${PRIV_KEY}    ${CID}   
                        Run Keyword And Expect Error        *       
                        ...  Put object to NeoFS            ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}     ${EMPTY}          
 

                        Log	                                Operation should be failed with container rule "RF 3 SELECT 2 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 3 SELECT 2 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
                        Run Keyword And Expect Error        *       
                        ...  Put object to NeoFS            ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}     ${EMPTY}          
 

                        Log	                                Operation should be failed with container rule "RF 1 SELECT 6 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 6 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
                        Run Keyword And Expect Error        *       
                        ...  Put object to NeoFS            ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}     ${EMPTY}  






Generate file
    ${FILE} =           Generate file of bytes              1024
                        Set Global Variable                 ${FILE}          ${FILE}
 
Generate Key and Pre-payment
    ${WALLET} =             Init wallet
                            Generate wallet         ${WALLET}
    ${ADDR} =               Dump Address            ${WALLET}
    ${USER_KEY_GEN} =       Dump PrivKey            ${WALLET}           ${ADDR}            

                            Set Global Variable     ${PRIV_KEY}                  ${USER_KEY_GEN}
 
                            Payment Operations      ${WALLET}       ${ADDR}      ${PRIV_KEY}  

 
Payment Operations
    [Arguments]    ${WALLET}   ${ADDR}   ${KEY}
    
    ${TX} =             Transfer Mainnet Gas    wallets/wallet.json     NTrezR3C4X8aMLVg7vozt5wguyNfFhwuFx      ${ADDR}     55
                        Wait Until Keyword Succeeds         1 min       15 sec        
                        ...  Transaction accepted in block  ${TX}
                        Get Transaction                     ${TX}
                        Expexted Mainnet Balance            ${ADDR}     55

    ${SCRIPT_HASH} =    Get ScripHash           ${KEY}

    ${TX_DEPOSIT} =     NeoFS Deposit           ${WALLET}               ${ADDR}     ${SCRIPT_HASH}      50
                        Wait Until Keyword Succeeds         1 min          15 sec        
                        ...  Transaction accepted in block  ${TX_DEPOSIT}
                        Get Transaction                     ${TX_DEPOSIT}

Validate Policy
    [Arguments]    ${POLICY}    ${EXPECTED_VAL}     @{EXPECTED_LIST}
                        
                        Log	                                Container with rule ${POLICY}

    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      ${POLICY} 
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}     ${EMPTY}          
                        Validate storage policy for object  ${PRIV_KEY}    ${EXPECTED_VAL}             ${CID}            ${S_OID}   @{EXPECTED_LIST}
                        # Can be on each of nodes

