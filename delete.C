#include "catalog.h"
#include "query.h"
 
/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
    Status status;
    HeapFileScan *hfs;
    RID rid;
    
    // Check if this is an unconditional delete (no WHERE clause)
    if (attrName.empty()) {
        // Unconditional delete - delete all records
        hfs = new HeapFileScan(relation, status);
        if (status != OK) {
            return status;
        }
        
        // Start an unconditional scan (pass 0 for offset, 0 for length)
        status = hfs->startScan(0, 0, STRING, NULL, EQ);
        if (status != OK) {
            delete hfs;
            return status;
        }
        
        // Delete all records
        while (hfs->scanNext(rid) == OK) {
            status = hfs->deleteRecord();
            if (status != OK) {
                delete hfs;
                return status;
            }
        }
        
        delete hfs;
        return OK;
    }
    
    // Conditional delete - need to get attribute information from catalog
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) {
        return status;
    }
    
    // Create HeapFileScan object
    hfs = new HeapFileScan(relation, status);
    if (status != OK) {
        return status;
    }
    
    // Convert attrValue to the appropriate type and start the scan
    int intValue;
    float floatValue;
    
    switch (type) {
        case INTEGER:
            intValue = atoi(attrValue);
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, 
                                   INTEGER, (char*)&intValue, op);
            break;
            
        case FLOAT:
            floatValue = atof(attrValue);
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, 
                                   FLOAT, (char*)&floatValue, op);
            break;
            
        case STRING:
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, 
                                   STRING, attrValue, op);
            break;
            
        default:
            delete hfs;
            return BADCATPARM;
    }
    
    if (status != OK) {
        delete hfs;
        return status;
    }
    
    // Delete all matching records
    while (hfs->scanNext(rid) == OK) {
        status = hfs->deleteRecord();
        if (status != OK) {
            delete hfs;
            return status;
        }
    }
    
    // Clean up and return
    delete hfs;
    return OK;
}