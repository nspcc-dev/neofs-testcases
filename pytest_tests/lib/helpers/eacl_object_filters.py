EACL_OBJ_FILTERS = {
    "$Object:objectID": "objectID",
    "$Object:containerID": "containerID",
    "$Object:ownerID": "ownerID",
    "$Object:creationEpoch": "creationEpoch",
    "$Object:payloadLength": "payloadLength",
    "$Object:payloadHash": "payloadHash",
    "$Object:objectType": "objectType",
    "$Object:version": "version",
}

VERB_FILTER_DEP = {
    "$Object:objectID": ["GET", "HEAD", "DELETE"],
    "$Object:containerID": ["GET", "PUT", "HEAD", "DELETE", "SEARCH"],
    "$Object:ownerID": ["GET", "HEAD"],
    "$Object:creationEpoch": ["GET", "PUT", "HEAD"],
    "$Object:payloadLength": ["GET", "PUT", "HEAD"],
    "$Object:payloadHash": ["GET", "PUT", "HEAD"],
    "$Object:objectType": ["GET", "PUT", "HEAD"],
    "$Object:version": ["GET", "PUT", "HEAD"],
}
