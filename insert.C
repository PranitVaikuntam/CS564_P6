#include "catalog.h"
#include "query.h"


/*
 * Inserts a record into the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Insert(const string & relation, 
	const int attrCnt, 
	const attrInfo attrList[])
{
	//Get info about the attributes for this relation
	int actualAttrCnt;
	AttrDesc* attrs;
	attrCat->getRelInfo(relation, actualAttrCnt, attrs); //attrCat is the global variable. 

	// //Check if the attribute count specified in the arguments matches that of the attrCat relation
	// if(attrCnt != actualAttrCnt) {
	// 	return ATTRTYPEMISMATCH; //closest thing
	// }


	// //Find length of the tuple to be created
	// int length = 0;
	// for(int i = 0; i < attrCnt; i++) {
	// 	length += attrInfo[i].attrLen;
	// }

	// //Create the record to be inserted
	// char *data = new char[length];
	// for(int i = 0; i < attrCnt; i++) {
	// 	//Check if the attribute type and length match
	// 	if(attrList[i].attrType != attrs[i].attrType || attrList[i].attrLen != attrs[i].attrLen) {
	// 		free(attrs); ///attrs was malloced in getRelInfo
	// 		delete [] data; //C++ way of freeing
	// 		return ATTRTYPEMISMATCH;
	// 	}

	// 	//Copy the attribute value into the data buffer at the correct offset
	// 	memcpy(data + offset, attrList[i].attrValue, attrList[i].attrLen);

	// }
	return OK;
}
