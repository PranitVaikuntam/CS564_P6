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
	Status status; //Will use this throughout the function

	//Get info about the attributes for this relation
	int actualAttrCnt; //Number of attributes specified in the relation
	AttrDesc* attrs;
	status = attrCat->getRelInfo(relation, actualAttrCnt, attrs); //attrCat is the global variable. 
	if(status != OK) {
		return status; //RELNOTFOUND?
	}
 
	//Check if the attribute count specified in the arguments matches that of the attrCat relation
	if(attrCnt != actualAttrCnt) {
		free(attrs);
		return ATTRTYPEMISMATCH; //closest thing
	}


	//Find length of the tuple to be created
	int length = 0;
	for(int i = 0; i < actualAttrCnt; i++) {
		length += attrs[i].attrLen;
	}

	//Create the record to be inserted
	char *data = new char[length]; //have to use char. Can't use void
	memset(data, 0, length); //Initialize to zero

	//attrs containts how the attributes are organized int attrCat. the attributes in attrList might not be organized in the same way
	//Thus, we have to find the matching attribute in attrList as we loop through the attributes in attrs
    for (int i = 0; i < actualAttrCnt; i++) {
        bool found = false; //If the attribute was found

        for (int j = 0; j < actualAttrCnt; j++) {

            if (strcmp(attrs[i].attrName, attrList[j].attrName) == 0) {
				//Check if attrValue is NULL
				if(attrList[j].attrValue == NULL) {
					delete[] data;
					free(attrs);
					return ATTRNOTFOUND;
				}

                // Copy attribute value into correct offset. The data is different based on if its an int, float, or string
                if (attrs[i].attrType == INTEGER) {
					int val = atoi((char*)attrList[j].attrValue);
					memcpy(data + attrs[i].attrOffset, &val, attrs[i].attrLen);
				}
				else if (attrs[i].attrType == FLOAT) {
					float val = atof((char*)attrList[j].attrValue);
					memcpy(data + attrs[i].attrOffset, &val, attrs[i].attrLen);
				}
				else { // STRING
					memcpy(data + attrs[i].attrOffset, attrList[j].attrValue, attrs[i].attrLen);
				}

                found = true;
                break;
            }
        }

		//Attribute not found in the provided attrList
        if (!found) {
            delete[] data;
            free(attrs);
            return ATTRNOTFOUND;
        }
    }

	//Create InsertFileScan Object for inserting record
    InsertFileScan ifs(relation, status); //C++ way of constructing objects
    if (status != OK) {
        delete[] data;
        free(attrs);
        return status;
    }

	//Create record
    Record rec;
    rec.data = data;
    rec.length = length;

    RID rid; //Not sure if this variable is used. It is the outRID
    status = ifs.insertRecord(rec, rid); //insert record
	if (status != OK) {
		delete[] data;
		free(attrs);
		return status;
	}

    //Cleanup - Success
    delete[] data;
    free(attrs);
    return status;
}
