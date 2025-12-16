#include "catalog.h"
#include "query.h"
#include "stdio.h"
#include "stdlib.h"




// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen); 

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;
	

	Status status;
	int reclen = 0; // length of projected record

	// checks if projection count is valud
	if (projCnt <= 0){
		return OK;
	}

	// converts projection info from attrInfo to AttrDesc (offset, type, length)
	AttrDesc* attrDescArray = new AttrDesc[projCnt];

	// iterates output tuple's attributes to get infor from attr catalog and compute output tuple length
	for (int i = 0; i<projCnt; i++){
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArray[i]);
		if(status !=OK){ // checks status
			delete[] attrDescArray; // clean up the allocated mem
			return status;
		}
		reclen += attrDescArray[i].attrLen; /// computes output tuple length
	}

	AttrDesc attrDesc; // references the attribute to be filtered
	const AttrDesc* attrDescPtr = NULL; // point to NULL if no filtering

	int prevInt = 0; // references the attribute value to be filtered
	float prevFloat = 0.0; // ref. the float value 
	char *prevString = NULL; // string value
	const char *filter = NULL; // points to filter value

	// checks if there is any filtering
	if(attr!=NULL){
		// checks if WHERE clause valud is valud
		if (attrValue == NULL){
			delete[] attrDescArray;// clean up memory
			return BADSCANPARM; // otherwise, heapfilescan error
		}

		// Checks attribute info in catalog
		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if(status != OK){
			delete[] attrDescArray;// clean up memory
			return status;
		}

		// sets up fultering info
		attrDescPtr = &attrDesc;
		// converts attrValue to appropriate type
		switch(attrDesc.attrType){
			case INTEGER:
				prevInt = atoi(attrValue); // convert to integer
				filter = (const char*)&prevInt; // set filter to int value
				break;
			case FLOAT:
				prevFloat = (float)atof(attrValue);
				filter = (const char*)&prevFloat; // set filter to float value
				break;
			case STRING:
				prevString = new char[attrDesc.attrLen];
				memset(prevString, 0, attrDesc.attrLen); // initialize memory
				strncpy(prevString, attrValue, attrDesc.attrLen); // copy string
				filter = prevString; // set filter to string value
				break;
			default:
				delete[] attrDescArray; // clean up memory
				return BADSCANPARM; // returns error for bad scan parameter
		}
	}

	// calls ScanSelect to perform the selection to result relation
	status = ScanSelect(result, projCnt, attrDescArray, attrDescPtr, op, filter, reclen);
	if(prevString != NULL){
		delete [] prevString;// clean up memory
	}

	// clean up memory
	delete[] attrDescArray;// clean up memory
	return status;

}


const Status ScanSelect(const string & result, 

			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen)
{
    cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

	Status status;
	// Create InsertFileScan object for result relation
	InsertFileScan resultRel(result, status);
	if(status != OK){
		return status;
	}

	// Create HeapFileScan object for input relation 
	HeapFileScan heapFileScan(string(projNames[0].relName), status);
	if(status != OK){
		return status;
	}

	// Start the scan with or without filtering
	if(attrDesc != NULL){
		// checks filtering (WHERE clause)
		status = heapFileScan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
	} else{
		// ...otherwise start scan without filtering (without WHERE clause)
		status = heapFileScan.startScan(0,0,STRING,NULL, EQ); 
	}

	// checks status
	if(status!=OK){
		return status;
	}

	// alloc. memory for output record
	char *outputData = new char[reclen]; 
	Record outputRec; // output record
	outputRec.data = (void*)outputData; // 
	outputRec.length = reclen; // set output record length

	// scans input relation and projects attributes --> inserts into result relation
	RID rid;
	// loops through records that satisfy the scan
	while((status = heapFileScan.scanNext(rid)) == OK){
		Record rec; // represents the input record
		status = heapFileScan.getRecord(rec); // get record

		// checks status
		if(status != OK){
			delete[] outputData; // clean up memory when error
			heapFileScan.endScan(); // end scan when error
			return status;
		}

		// project attributes to output record
		int offset=0; // represents the offset in output record
		for(int i=0; i<projCnt; i++){
			// copy attribute data to output record
			memcpy((char*)outputRec.data + offset, (char*)rec.data + projNames[i].attrOffset, projNames[i].attrLen);
			offset += projNames[i].attrLen; // update offset
		}
		// insert projected record into result relation
		RID outRid;
		status = resultRel.insertRecord(outputRec, outRid);
		if(status != OK){
			delete[] outputData; // clean up memory when error
			heapFileScan.endScan(); // end scan when error
			return status;
		}
	}

	// clean up and end scan when doen
	delete[] outputData;
	heapFileScan.endScan();

	return OK; // DONE

}
