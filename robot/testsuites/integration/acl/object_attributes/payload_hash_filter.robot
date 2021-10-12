*** Settings ***

Resource        ../common_steps_acl_extended.robot
Resource        ../../${RESOURCES}/setup_teardown.robot

*** Test cases ***
Payload Hash Object Filter for Extended ACL 
    [Documentation]         Testcase to validate if $Object:payloadHash eACL filter is correctly handled.
    [Tags]                  ACL  eACL  NeoFS  NeoCLI
    [Timeout]               20 min

    [Setup]                 Setup

    Log    Check eACL payloadHash Filter with MatchType String Equal
    Check eACL Filters with MatchType String Equal    $Object:payloadHash

    [Teardown]          Teardown    payload_hash_filter
