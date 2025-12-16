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
	// 
    Status status; 
    HeapFileScan *hfs;
    RID rid;
    
    // Check if this is an unconditional delete (no WHERE clause)
    if (attrName.empty()) {
		// Opens HeapFileScan for the relation 
        hfs = new HeapFileScan(relation, status);
        if (status != OK) {
            return status;
        }
        // Start scan without filtering
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
		// when done 
        delete hfs;
        return OK;
    }
    
    // Get attribute info from catalog
    AttrDesc attrDesc;
    status = attrCat->getInfo(relation, attrName, attrDesc);
    if (status != OK) {
        return status;
    }
    
    // Create HeapFileScan objec 
    hfs = new HeapFileScan(relation, status);
    if (status != OK) {
        return status;
    }
    
    // Convert attrValue to the approriate type and start scan
    int intValue;
    float floatValue;
	// determines type
    switch (type) {
        case INTEGER:
            intValue = atoi(attrValue); // convert to integer
			// thenn start scan
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, INTEGER, (char*)&intValue, op);
            break;
        case FLOAT:
            floatValue = atof(attrValue); // convert to float
			// then start scan
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, FLOAT, (char*)&floatValue, op);
            break;
        case STRING:
			// then start scan
            status = hfs->startScan(attrDesc.attrOffset, attrDesc.attrLen, STRING, attrValue, op);
            break;
        default:
            delete hfs;
            return BADCATPARM;
    }
    
	// ensures that status is correct
    if (status != OK) {
        delete hfs;
        return status;
    }
    
    // Delete all matching records
    while (hfs->scanNext(rid) == OK) {
        status = hfs->deleteRecord(); // delete record
        if (status != OK) {
            delete hfs;
            return status;
        }
    }
    
    // Clean up the HeapFileScan and SUCCESS
    delete hfs;
    return OK;
}