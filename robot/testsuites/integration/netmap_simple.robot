*** Settings ***
Variables   ../../variables/common.py

Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neo.py
Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment.py
Library     ${RESOURCES}/assertions.py
Library     ${RESOURCES}/neo.py

 

*** Test cases ***
NeoFS Simple Netmap
    [Documentation]     Testcase to validate NeoFS Netmap.
    [Tags]              Netmap  NeoFS  NeoCLI
    [Timeout]           20 min

    ${PRIV_KEY} =       Generate Neo private key
    ${PUB_KEY} =        Get Neo public key                  ${PRIV_KEY}
    ${ADDR} =           Get Neo address                     ${PRIV_KEY}

    ${FILE} =           Generate file of bytes              1024

                        Log	                                Container with rule "RF 2 SELECT 2 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 2 SELECT 2 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}            ${S_OID}  


                        Log	                                Container with rule "RF 1 SELECT 1 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 1 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    1             ${CID}            ${S_OID}


                        Log	                                Container with rule "RF 2 SELECT 1 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 2 SELECT 1 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    1             ${CID}            ${S_OID}


                        Log	                                Container with rule "RF 1 SELECT 4 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 4 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    4             ${CID}            ${S_OID}


                        Log	                                Container with rule "RF 2 SELECT 1 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 2 SELECT 1 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    1             ${CID}            ${S_OID}


                        Log	                                Container with rule "RF 1 SELECT 1 Node FILTER Country EQ GB"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 1 Node FILTER Country EQ GB
    @{EXPECTED} =	    Create List                         192.168.123.74
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    1             ${CID}            ${S_OID}


                        Log	                                Container with rule "RF 1 SELECT 1 Node FILTER Country NE GB Country NE SG Country NE DE"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 1 Node FILTER Country NE GB Country NE SG Country NE DE
    @{EXPECTED} =	    Create List                         192.168.123.71
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    1             ${CID}            ${S_OID}


                        Log	                                Container with rule "RF 1 SELECT 2 Node FILTER Country NE GB Country NE DE"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 2 Node FILTER Country NE GB Country NE DE
    @{EXPECTED} =	    Create List                         192.168.123.71  192.168.123.72
                        Container Existing                  ${PRIV_KEY}    ${CID}   
    ${S_OID} =          Put object to NeoFS                 ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
                        Validate storage policy for object  ${PRIV_KEY}    2             ${CID}            ${S_OID}


                        Log	                                Operation should be failed with container rule "RF 2 SELECT 2 Node FILTER Country NE GB Country NE DE"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 2 SELECT 2 Node FILTER Country NE GB Country NE DE
                        Container Existing                  ${PRIV_KEY}    ${CID}   
                        Run Keyword And Expect Error        *       
                        ...  Put object to NeoFS            ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
 

                        Log	                                Operation should be failed with container rule "RF 3 SELECT 2 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 3 SELECT 2 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
                        Run Keyword And Expect Error        *       
                        ...  Put object to NeoFS            ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY}         
 

                        Log	                                Operation should be failed with container rule "RF 1 SELECT 6 Node"
    ${CID} =            Create container                    ${PRIV_KEY}    ${EMPTY}      RF 1 SELECT 6 Node
                        Container Existing                  ${PRIV_KEY}    ${CID}   
                        Run Keyword And Expect Error        *       
                        ...  Put object to NeoFS            ${PRIV_KEY}    ${FILE}       ${CID}            ${EMPTY} 


#	Netmap: {"Epoch":916,"NetMap":[
#{"address":"/ip4/192.168.123.74/tcp/8080","pubkey":"A4yGKVnla0PiD3kYfE/p4Lx8jGbBYD5s8Ox/h6trCNw1",
#"options":["/City:London","/Capacity:100","/Price:1","/Location:Europe","/Country:GB"],"status":0},

#{"address":"/ip4/192.168.123.72/tcp/8080","pubkey":"A/9ltq55E0pNzp0NOdOFHpurTul6v4boHhxbvFDNKCau",
#"options":["/City:Singapore","/Capacity:100","/Price:1","/Location:Asia","/Country:SG"],"status":0},

#{"address":"/ip4/192.168.123.71/tcp/8080","pubkey":"Aiu0BBxQ1gf/hx3sfkzXd4OI4OpoSdhMy9mqjzLhaoEx",
#"options":["/Location:NorthAmerica","/Country:US","/City:NewYork","/Capacity:100","/Price:1"],"status":0},

#{"address":"/ip4/192.168.123.73/tcp/8080","pubkey":"AqySDNffC2GyiQcua5RuLaThoxuascYhu0deMPpKsQLD",
#"options":["/Capacity:100","/Price:1","/Location:Europe","/Country:DE","/City:Frankfurt"],"status":0}]}