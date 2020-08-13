*** Settings ***
Variables   ../../variables/common.py

 
Library     ${RESOURCES}/environment.py
Library     ${RESOURCES}/neo.py
Library     ${RESOURCES}/neofs.py
Library     ${RESOURCES}/payment.py
Library     ${RESOURCES}/assertions.py
Library     ${RESOURCES}/neo.py


*** Test cases ***
Basic ACL Operations
    [Documentation]     Testcase to validate NeoFS operations with ACL.
    [Tags]              ACL  NeoFS  NeoCLI
    [Timeout]           20 min

    Generate Keys
    Create Containers
    Generate file
    Check Private Container
    Check Public Container
    Check Read-Only Container
 


*** Keywords ***

Generate Keys
    ${USER_KEY_GEN} =       Generate Neo private key
    ${OTHER_KEY_GEN} =      Generate Neo private key
    ${SYSTEM_KEY_GEN} =	    Form Privkey from String            c428b4a06f166fde9f8afcf918194acdde35aa2612ecf42fe0c94273425ded21    
    ${SYSTEM_KEY_GEN_SN} =  Form Privkey from String            0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2

                            Set Global Variable     ${USER_KEY}                  ${USER_KEY_GEN}
                            Set Global Variable     ${OTHER_KEY}                 ${OTHER_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY}                ${SYSTEM_KEY_GEN}
                            Set Global Variable     ${SYSTEM_KEY_STOR_NODE}      ${SYSTEM_KEY_GEN_SN}

    # Basic ACL manual page: https://neospcc.atlassian.net/wiki/spaces/NEOF/pages/362348545/NeoFS+ACL
    # TODO: X - Sticky bit validation on public container!!!

Create Containers
    # Create containers:
                            Log	                                Create Private Container
    ${PRIV_CID_GEN} =       Create container                    ${USER_KEY}     0x1C8C8CCC 
                            Container Existing                  ${USER_KEY}     ${PRIV_CID_GEN}  

                            Log	                                Create Public Container
    ${PUBLIC_CID_GEN} =     Create container                    ${USER_KEY}     0x3FFFFFFF
                            Container Existing                  ${USER_KEY}     ${PUBLIC_CID_GEN}  

                            Log	                                Create Read-Only Container
    ${READONLY_CID_GEN} =   Create container                    ${USER_KEY}     0x1FFF8CFF
                            Container Existing                  ${USER_KEY}     ${READONLY_CID_GEN}   

    Set Global Variable     ${PRIV_CID}          ${PRIV_CID_GEN}
    Set Global Variable     ${PUBLIC_CID}        ${PUBLIC_CID_GEN}
    Set Global Variable     ${READONLY_CID}      ${READONLY_CID_GEN}


Generate file
    # Generate small file
    ${FILE_S_GEN} =         Generate file of bytes              1024
    ${FILE_S_HASH_GEN} =    Get file hash                       ${FILE_S_GEN}

    Set Global Variable     ${FILE_S}          ${FILE_S_GEN}
    Set Global Variable     ${FILE_S_HASH}     ${FILE_S_HASH_GEN}

Check Private Container
    # Check Private:
    # Expected: User - pass, Other - fail, System(IR) - pass (+ System(Container node) - pass, Non-container node - fail). 

    # Put
    ${S_OID_USER} =     Put object to NeoFS                 ${USER_KEY}      ${FILE_S}       ${PRIV_CID}
                        Run Keyword And Expect Error        *
                        ...  Put object to NeoFS            ${OTHER_KEY}     ${FILE_S}       ${PRIV_CID}
                        Run Keyword And Expect Error        *
                        ...  Put object to NeoFS            ${SYSTEM_KEY}    ${FILE_S}       ${PRIV_CID}
    ${S_OID_SYS_SN} =   Put object to NeoFS                 ${SYSTEM_KEY_STOR_NODE}    ${FILE_S}       ${PRIV_CID}

                        


    # Get
                        Get object from NeoFS               ${USER_KEY}      ${PRIV_CID}     ${S_OID_USER}       s_file_read
                        Run Keyword And Expect Error        *
                        ...  Get object from NeoFS          ${OTHER_KEY}      ${PRIV_CID}     ${S_OID_USER}      s_file_read
                        Run Keyword And Expect Error        *
                        ...  Get object from NeoFS          ${SYSTEM_KEY}      ${PRIV_CID}    ${S_OID_USER}      s_file_read
                        Get object from NeoFS               ${SYSTEM_KEY_STOR_NODE}      ${PRIV_CID}    ${S_OID_USER}      s_file_read 

    # Get Range
                        Get Range                           ${USER_KEY}      ${PRIV_CID}     ${S_OID_USER}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range                      ${OTHER_KEY}     ${PRIV_CID}     ${S_OID_USER}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range                      ${SYSTEM_KEY}    ${PRIV_CID}     ${S_OID_USER}    0:256
                        Run Keyword And Expect Error        *
                        ...  Get Range                      ${SYSTEM_KEY_STOR_NODE}    ${PRIV_CID}     ${S_OID_USER}    0:256

    # TODO: GetRangeHash 
    # get-range-hash --cid <cid> --oid <oid> [--bearer <hex>] [--verify --file </path/to/file>] [--salt <hex>] [<offset1>:<length1> [...]]
    #    neospcc@neospcc:~/GIT/neofs-testcases$ docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2 object get-range-hash --cid 4H9iChvzYdBg6qntfYUWGWCzsJFBDdo99KegefsD721Q --oid a101d078-b3d4-4325-8fe8-41dce6917097
    # invalid input
    # Usage: get-range-hash --cid <cid> --oid <oid> [--bearer <hex>] [--verify --file </path/to/file>] [--salt <hex>] [<offset1>:<length1> [...]]


    # Search
    @{S_OBJ_PRIV} =	    Create List	                        ${S_OID_USER}   ${S_OID_SYS_SN}
                        Search object                       ${USER_KEY}     ${PRIV_CID}   ${EMPTY}  @{S_OBJ_PRIV}
                        Run Keyword And Expect Error        *
                        ...  Search object                  ${OTHER_KEY}    ${PRIV_CID}  ${EMPTY}  @{S_OBJ_PRIV}
                        Search object                       ${SYSTEM_KEY}   ${PRIV_CID}  ${EMPTY}  @{S_OBJ_PRIV}
                        Search object                       ${SYSTEM_KEY_STOR_NODE}   ${PRIV_CID}  ${EMPTY}  @{S_OBJ_PRIV}

 
    # Head
                        Head object                         ${USER_KEY}    ${PRIV_CID}        ${S_OBJ_PRIV}       ${True}
                        Run Keyword And Expect Error        *
                        ...  Head object                    ${OTHER_KEY}    ${PRIV_CID}        ${S_OBJ_PRIV}       ${True}
                        Head object                         ${SYSTEM_KEY}    ${PRIV_CID}        ${S_OBJ_PRIV}       ${True}
                        Head object                         ${SYSTEM_KEY_STOR_NODE}    ${PRIV_CID}        ${S_OBJ_PRIV}       ${True}



    # Delete
                        Delete object                       ${USER_KEY}      ${PRIV_CID}     ${S_OID_USER}     
                        Run Keyword And Expect Error        *
                        ...  Delete object                  ${OTHER_KEY}     ${PRIV_CID}     ${S_OID_USER}  
                        Run Keyword And Expect Error        *
                        ...  Delete object                  ${SYSTEM_KEY}    ${PRIV_CID}     ${S_OID_USER}   
                        Run Keyword And Expect Error        *
                        ...  Delete object                  ${SYSTEM_KEY_STOR_NODE}    ${PRIV_CID}     ${S_OID_USER}

 

Check Public Container
    # Check Public:
    # Expected: User - pass, Other - fail, System(IR) - pass (+ System(Container node) - pass, Non-container node - fail). 

    # Put
    ${S_OID_USER} =     Put object to NeoFS                 ${USER_KEY}      ${FILE_S}       ${PUBLIC_CID}
    ${S_OID_OTHER} =    Put object to NeoFS                 ${OTHER_KEY}     ${FILE_S}       ${PUBLIC_CID}
    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                        Run Keyword And Expect Error        *
                        ...  Put object to NeoFS            ${SYSTEM_KEY}    ${FILE_S}       ${PUBLIC_CID}
    ${S_OID_SYS_SN} =   Put object to NeoFS                 ${SYSTEM_KEY_STOR_NODE}    ${FILE_S}       ${PUBLIC_CID}

    # Get
                        Get object from NeoFS               ${USER_KEY}      ${PUBLIC_CID}     ${S_OID_USER}       s_file_read
                        Get object from NeoFS               ${OTHER_KEY}     ${PUBLIC_CID}     ${S_OID_USER}       s_file_read
    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                        Run Keyword And Expect Error        *
                        ...  Get object from NeoFS          ${SYSTEM_KEY}    ${PUBLIC_CID}     ${S_OID_USER}       s_file_read 
                        Get object from NeoFS               ${SYSTEM_KEY_STOR_NODE}     ${PUBLIC_CID}     ${S_OID_USER}       s_file_read

    # Get Range
                        Get Range                      ${USER_KEY}      ${PUBLIC_CID}     ${S_OID_USER}    0:256
                        Get Range                      ${OTHER_KEY}     ${PUBLIC_CID}     ${S_OID_USER}    0:256
    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                        Run Keyword And Expect Error        *
                        ...  Get Range                 ${SYSTEM_KEY}    ${PUBLIC_CID}     ${S_OID_USER}    0:256
                        Get Range                      ${SYSTEM_KEY_STOR_NODE}     ${PUBLIC_CID}     ${S_OID_USER}    0:256

    # TODO: GetRangeHash 
    # get-range-hash --cid <cid> --oid <oid> [--bearer <hex>] [--verify --file </path/to/file>] [--salt <hex>] [<offset1>:<length1> [...]]
    #    neospcc@neospcc:~/GIT/neofs-testcases$ docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2 object get-range-hash --cid 4H9iChvzYdBg6qntfYUWGWCzsJFBDdo99KegefsD721Q --oid a101d078-b3d4-4325-8fe8-41dce6917097
    # invalid input
    # Usage: get-range-hash --cid <cid> --oid <oid> [--bearer <hex>] [--verify --file </path/to/file>] [--salt <hex>] [<offset1>:<length1> [...]]


    # Search
    @{S_OBJ_PRIV} =	    Create List	                        ${S_OID_USER}   ${S_OID_OTHER}   ${S_OID_SYS_SN}
                        Search object                       ${USER_KEY}     ${PUBLIC_CID}  ${EMPTY}  @{S_OBJ_PRIV}
                        Search object                       ${OTHER_KEY}    ${PUBLIC_CID}  ${EMPTY}  @{S_OBJ_PRIV}
                        Search object                       ${SYSTEM_KEY}   ${PUBLIC_CID}  ${EMPTY}  @{S_OBJ_PRIV}
                        Search object                       ${SYSTEM_KEY_STOR_NODE}   ${PUBLIC_CID}  ${EMPTY}  @{S_OBJ_PRIV}


    # Head
                        Head object                         ${USER_KEY}     ${PUBLIC_CID}      ${S_OID_USER}       ${True}
                        Head object                         ${OTHER_KEY}    ${PUBLIC_CID}      ${S_OID_USER}       ${True}
                        Head object                         ${SYSTEM_KEY}   ${PUBLIC_CID}      ${S_OID_USER}       ${True}
                        Head object                         ${SYSTEM_KEY_STOR_NODE}   ${PUBLIC_CID}      ${S_OID_USER}       ${True}

                        Head object                         ${USER_KEY}     ${PUBLIC_CID}      ${S_OID_OTHER}      ${True}
                        Head object                         ${OTHER_KEY}    ${PUBLIC_CID}      ${S_OID_OTHER}      ${True}
                        Head object                         ${SYSTEM_KEY}   ${PUBLIC_CID}      ${S_OID_OTHER}      ${True}
                        Head object                         ${SYSTEM_KEY_STOR_NODE}   ${PUBLIC_CID}      ${S_OID_OTHER}      ${True}

                        Head object                         ${USER_KEY}     ${PUBLIC_CID}      ${S_OID_SYS_SN}       ${True}
                        Head object                         ${OTHER_KEY}    ${PUBLIC_CID}      ${S_OID_SYS_SN}       ${True}
                        Head object                         ${SYSTEM_KEY}   ${PUBLIC_CID}      ${S_OID_SYS_SN}       ${True}
                        Head object                         ${SYSTEM_KEY_STOR_NODE}   ${PUBLIC_CID}      ${S_OID_SYS_SN}       ${True}


    # Delete
                        Delete object                       ${USER_KEY}      ${PUBLIC_CID}     ${S_OID_USER}     
                        Delete object                       ${OTHER_KEY}     ${PUBLIC_CID}     ${S_OID_USER}
                        Run Keyword And Expect Error        *  
                        ...  Delete object                  ${SYSTEM_KEY}    ${PUBLIC_CID}     ${S_OID_USER}   
                        Delete object                       ${SYSTEM_KEY_STOR_NODE}     ${PUBLIC_CID}     ${S_OID_USER}


Check Read-Only Container
    # Check Read Only container:

    # Put
    ${S_OID_USER} =     Put object to NeoFS                 ${USER_KEY}      ${FILE_S}       ${READONLY_CID}
                        Run Keyword And Expect Error        *
                        ...  Put object to NeoFS            ${OTHER_KEY}     ${FILE_S}       ${READONLY_CID}
                        Run Keyword And Expect Error        *
                        ...  Put object to NeoFS            ${SYSTEM_KEY}    ${FILE_S}       ${READONLY_CID}
    ${S_OID_SYS_SN} =   Put object to NeoFS                 ${SYSTEM_KEY_STOR_NODE}    ${FILE_S}       ${READONLY_CID}

    # Get
                        Get object from NeoFS               ${USER_KEY}      ${READONLY_CID}     ${S_OID_USER}       s_file_read
                        Get object from NeoFS               ${OTHER_KEY}     ${READONLY_CID}     ${S_OID_USER}       s_file_read
    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                        Run Keyword And Expect Error        *
                        ...  Get object from NeoFS          ${SYSTEM_KEY}    ${READONLY_CID}     ${S_OID_USER}       s_file_read 
                        Get object from NeoFS               ${SYSTEM_KEY_STOR_NODE}      ${READONLY_CID}     ${S_OID_USER}       s_file_read

    # Get Range
                        Get Range                      ${USER_KEY}      ${READONLY_CID}     ${S_OID_USER}    0:256
                        Get Range                      ${OTHER_KEY}     ${READONLY_CID}     ${S_OID_USER}    0:256
    # By discussion, IR can not make any operations instead of HEAD, SEARCH and GET RANGE HASH at the current moment
                        Run Keyword And Expect Error        *
                        ...  Get Range                 ${SYSTEM_KEY}    ${READONLY_CID}     ${S_OID_USER}    0:256
                        Get Range                      ${SYSTEM_KEY_STOR_NODE}      ${READONLY_CID}     ${S_OID_USER}    0:256

    # TODO: GetRangeHash 
    # get-range-hash --cid <cid> --oid <oid> [--bearer <hex>] [--verify --file </path/to/file>] [--salt <hex>] [<offset1>:<length1> [...]]
    #    neospcc@neospcc:~/GIT/neofs-testcases$ docker exec neofs-cli neofs-cli --host 192.168.123.71:8080 --key 0fa21a94be2227916284e4b3495180d9c93d04f095fe9d5a86f22044f5c411d2 object get-range-hash --cid 4H9iChvzYdBg6qntfYUWGWCzsJFBDdo99KegefsD721Q --oid a101d078-b3d4-4325-8fe8-41dce6917097
    # invalid input
    # Usage: get-range-hash --cid <cid> --oid <oid> [--bearer <hex>] [--verify --file </path/to/file>] [--salt <hex>] [<offset1>:<length1> [...]]


    # Search
    @{S_OBJ_RO} =	    Create List	                        ${S_OID_USER}   ${S_OID_SYS_SN}
                        Search object                       ${USER_KEY}     ${READONLY_CID}  ${EMPTY}  @{S_OBJ_RO}
                        Search object                       ${OTHER_KEY}    ${READONLY_CID}  ${EMPTY}  @{S_OBJ_RO}
                        Search object                       ${SYSTEM_KEY}   ${READONLY_CID}  ${EMPTY}  @{S_OBJ_RO}
                        Search object                       ${SYSTEM_KEY_STOR_NODE}   ${READONLY_CID}  ${EMPTY}  @{S_OBJ_RO}

 
    # Head
                        Head object                         ${USER_KEY}     ${READONLY_CID}      ${S_OID_USER}       ${True}
                        Head object                         ${OTHER_KEY}    ${READONLY_CID}      ${S_OID_USER}       ${True}
                        Head object                         ${SYSTEM_KEY}   ${READONLY_CID}      ${S_OID_USER}       ${True}
                        Head object                         ${SYSTEM_KEY_STOR_NODE}   ${READONLY_CID}      ${S_OID_USER}       ${True}

    # Delete
                        Delete object                       ${USER_KEY}      ${READONLY_CID}     ${S_OID_USER}
                        Run Keyword And Expect Error        *       
                        ...  Delete object                       ${OTHER_KEY}     ${READONLY_CID}     ${S_OID_USER}
                        Run Keyword And Expect Error        *  
                        ...  Delete object                  ${SYSTEM_KEY}    ${READONLY_CID}     ${S_OID_USER}  
                        Run Keyword And Expect Error        *  
                        ...  Delete object                  ${SYSTEM_KEY_STOR_NODE}    ${READONLY_CID}     ${S_OID_USER}  