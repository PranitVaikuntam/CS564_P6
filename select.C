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
	
	// checks for valid 
	Status status;
	int reclen = 0;

	if (projCnt <= 0){
		return OK;
	}

	AttrDesc* attrDescArray = new AttrDesc[projCnt];
	int recLen = 0;

	for (int i = 0; i<projCnt; i++){
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, attrDescArray[i]);
		if(status !=OK){
			delete[] attrDescArray;
			return status;
		}
		reclen += attrDescArray[i].attrLen;
	}

	AttrDesc attrDesc;
	const AttrDesc* attrDescPtr = NULL;

	int prevInt = 0;
	float prevFloat = 0.0;
	char *prevString = NULL;
	const char *filter = NULL;

	if(attr!=NULL){
		if (attrValue == NULL){
			delete[] attrDescArray;
			return BADSCANPARM;
		}

		status = attrCat->getInfo(attr->relName, attr->attrName, attrDesc);
		if(status != OK){
			delete[] attrDescArray;
			return status;
		}

		attrDescPtr = &attrDesc;
		switch(attrDesc.attrType){
			case INTEGER:
				prevInt = atoi(attrValue);
				filter = (const char*)&prevInt;
				break;
			case FLOAT:
				prevFloat = (float)atof(attrValue);
				filter = (const char*)&prevFloat;
				break;
			case STRING:
				prevString = new char[attrDesc.attrLen];
				memset(prevString, 0, attrDesc.attrLen);
				strncpy(prevString, attrValue, attrDesc.attrLen);
				filter = prevString;
				break;
			default:
				delete[] attrDescArray;
				return BADSCANPARM;
		}
	}

	status = ScanSelect(result, projCnt, attrDescArray, attrDescPtr, op, filter, reclen);
	if(prevString != NULL){
		delete [] prevString;
	}

	delete[] attrDescArray;
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
	InsertFileScan resultRel(result, status);
	if(status != OK){
		return status;
	}


	HeapFileScan heapFileScan(string(projNames[0].relName), status);
	if(status != OK){
		return status;
	}


	if(attrDesc != NULL){
		status = heapFileScan.startScan(attrDesc->attrOffset, attrDesc->attrLen,
										(Datatype)attrDesc->attrType, filter, op);
	} else{
		status = heapFileScan.startScan(0,0,STRING,NULL, EQ);
	}

	if(status!=OK){
		return status;
	}

	char *outputData = new char[reclen]; 
	Record outputRec;
	outputRec.data = (void*)outputData;
	outputRec.length = reclen;

	RID rid;
	while((status = heapFileScan.scanNext(rid)) == OK){
		Record rec;
		status = heapFileScan.getRecord(rec);

		if(status != OK){
			delete[] outputData;
			heapFileScan.endScan();
			return status;
		}

		// project the desired attributes
		int offset=0;
		for(int i=0; i<projCnt; i++){
			memcpy((char*)outputRec.data + offset, (char*)rec.data + projNames[i].attrOffset,
				   projNames[i].attrLen);
			offset += projNames[i].attrLen;
		}
		RID outRid;
		status = resultRel.insertRecord(outputRec, outRid);
		if(status != OK){
			delete[] outputData;
			heapFileScan.endScan();
			return status;
		}
	}

	delete[] outputData;
	heapFileScan.endScan();


	return OK;

}
