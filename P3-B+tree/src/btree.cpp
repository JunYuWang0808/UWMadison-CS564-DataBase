/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <zconf.h>
#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


//#define DEBUG

namespace badgerdb
{


// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string & relationName,
		std::string & outIndexName,
		BufMgr *bufMgrIn,
		const int attrByteOffset,
		const Datatype attrType)
{
    {
        this->leafOccupancy = 0;
        this->nodeOccupancy = 0;
        this->lowValDouble = 0;
        this->highValDouble = 0;
       // memset(bufMgrIn, 0, sizeof(bufMgrIn));
        this->bufMgr = bufMgrIn;
        this->attrByteOffset = attrByteOffset;
        this->attributeType = attrType;
        // create the outindexname
        std::string index_name = relationName;
        index_name += ".";
        index_name += std::to_string(attrByteOffset);
        outIndexName = index_name;
        // Check if the file exist
        if(!BlobFile::exists(index_name)) {
            // create blobfile, fill in metainfo, etc
            // ------------------------------------------
            this->file = new BlobFile(index_name, true);
            PageId new_page_number;
            Page *new_page;
            bufMgr->allocPage(file, new_page_number, new_page);
            IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(new_page);
            metaInfo->attrByteOffset = attrByteOffset;
            metaInfo->attrType = attrType;
            metaInfo->rootPageNo = Page::INVALID_NUMBER;
            metaInfo->root_is_leaf = false;
            metaInfo->root_level = Page::INVALID_NUMBER;
            //std::cout<<"-------1-----"<<std::endl;
            // TEST
         //   bufMgrIn->unPinPage(file, new_page_number, true);
            for(int k = 0; k < index_name.length(); k ++) {
                metaInfo->relationName[k] = index_name[k];
            }
            this->name = metaInfo->relationName;
        //    std::cout<<this->name<<std::endl;
            this->headerPageNum = new_page_number;
            this->rootPageNum = Page::INVALID_NUMBER;
            this->metaInfo = metaInfo;
            this->attrByteOffset = metaInfo->attrByteOffset;
            this->attributeType = metaInfo->attrType;
            this->scanExecuting = false;
	    bufMgr->unPinPage(file, new_page_number, true);
            // ------------------------------------------
            //fill the newly created Blob File using filescan
            FileScan fileScan(relationName, bufMgr);
            RecordId rid;
            try
            {
                while(1)
                {
                    fileScan.scanNext(rid);
                    std::string record = fileScan.getRecord();
                    //  std::cout<<record.c_str()+attrByteOffset<<std::endl;
                    insertEntry(record.c_str() + attrByteOffset, rid);
                }
            }
            catch(EndOfFileException e)
            {
		//std::cout<<"--------2-----"<<std::endl;
                // save Btee index file to disk
                bufMgr->flushFile(file);
            }
        } else {
            this->file = new BlobFile(index_name, false);
            Page *new_page;
            bufMgr->readPage(file, file->getFirstPageNo(), new_page);
            IndexMetaInfo *metaInfo = reinterpret_cast<IndexMetaInfo *>(new_page);
            // TEST
            this->metaInfo = metaInfo;
            this->root_level = metaInfo->root_level;
            this->rootPageNum = metaInfo->rootPageNo;
            this->name = index_name;
            this->attrByteOffset = metaInfo->attrByteOffset;
            this->attributeType = metaInfo->attrType;
	    this->scanExecuting = false;
            bufMgr->unPinPage(file, file->getFirstPageNo(), true);
//            this->file = &blobFile;
        }
    }

}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
    // close the blobfile
    // flush the bufMgr/.
    //bufMgr->printSelf();
    //bufMgr->unPinPage(file, headerPageNum, true);
   // std::cout<<this->rootPageNum<<" : "<<this->metaInfo->rootPageNo<<std::endl;
    bufMgr->flushFile(file);
    scanExecuting = false;
    delete file;
   // delete this->file;
    //File::remove(this->name);
    //file = NULL;
 //   std::cout<<"I have deleted"<<std::endl;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry<
// -----------------------------------------------------------------------------

    const bool is_empty_leaf(LeafNodeInt *rootpage) {
        for(int i = 0; i < INTARRAYLEAFSIZE; i ++) {
            if(rootpage->keyArray[i] == -1)
                return true;
        }
        return false;
    }

    const bool is_empty_nonleaf(NonLeafNodeInt *rootpage) {
        for(int i = 0; i < INTARRAYNONLEAFSIZE+1; i ++) {
            if(rootpage->pageNoArray[i] == Page::INVALID_NUMBER)
                return true;
        }
        return false;
    }
    void insert_leaf(LeafNodeInt* curr, RIDKeyPair<int> new_pair) {
        int t = INTARRAYLEAFSIZE;
        while(curr->keyArray[t-1] == -1 && t > 0) {
            t --;
        }
        while(t > 0) {
            if (curr->keyArray[t-1] > new_pair.key) {
                curr->keyArray[t] = curr->keyArray[t-1];
                curr->ridArray[t] = curr->ridArray[t-1];
                t --;
            } else {
                break;
            }
        }
        curr->keyArray[t] = new_pair.key;
        curr->ridArray[t] = new_pair.rid;
        return;
    }
    void insert_nonleaf(NonLeafNodeInt* curr, int key, PageId pageId) {
        int t = INTARRAYNONLEAFSIZE;
        while(curr->pageNoArray[t] == Page::INVALID_NUMBER && t > 0) {
            t --;
        }
        // copy and find
        while(t > 0) {
            if (curr->keyArray[t-1] > key) {
                curr->keyArray[t] = curr->keyArray[t-1];
                curr->pageNoArray[t+1] = curr->pageNoArray[t];
            } else {
                break;
            }
            t --;
        }
        //
        if(t == 0) {
            curr->keyArray[t] = key;
            curr->pageNoArray[t+1] = pageId;
        } else {
            curr->keyArray[t] = key;
            curr->pageNoArray[t+1] = pageId;
        }
        return;
    }
// set up a new page
void BTreeIndex::split_leaf(badgerdb::LeafNodeInt *root, int *new_child_key, PageId *new_page_id, RIDKeyPair<int> new_pair){
    PageId new_sibling_id;
    Page *new_sibling;
    // allocate a new sibling page
    this->bufMgr->allocPage(this->file, new_sibling_id, new_sibling);
    LeafNodeInt *sibling = reinterpret_cast<LeafNodeInt* >(new_sibling);
    if(new_sibling_id == 5) {

    }
    // init
    for(int i = 0; i < INTARRAYLEAFSIZE; i ++) {
        sibling->keyArray[i] = -1;
    }
    if(new_sibling_id == 5) {

    }
    // copy the content to the new sibling
    sibling->rightSibPageNo = Page::INVALID_NUMBER;
    for(int i = INTARRAYLEAFSIZE/2; i < INTARRAYLEAFSIZE; i ++) {
        sibling->keyArray[i-(INTARRAYLEAFSIZE/2)] = root->keyArray[i];
        root->keyArray[i] = -1;
        sibling->ridArray[i-(INTARRAYLEAFSIZE/2)] = root->ridArray[i];
    }
    PageId temp_right = root->rightSibPageNo;
    root->rightSibPageNo = new_sibling_id;
    sibling->rightSibPageNo = temp_right;
    // Determine which side to add the page
    *new_child_key = sibling->keyArray[0];
    *new_page_id = new_sibling_id;
    if(new_pair.key > *new_child_key) {
        insert_leaf(sibling, new_pair);
    } else {
        insert_leaf(root, new_pair);
    }


    bufMgr->unPinPage(this->file, new_sibling_id, true);
}

void BTreeIndex::split_nonleaf(badgerdb::NonLeafNodeInt *curr, int *new_child_key, badgerdb::PageId *new_page_id) {
    PageId new_sibling_id;
    Page *new_sibling;
    this->bufMgr->allocPage(this->file, new_sibling_id, new_sibling);
    NonLeafNodeInt *sibling = reinterpret_cast<NonLeafNodeInt*>(new_sibling);
    // init
    for(int i = 0; i <= INTARRAYNONLEAFSIZE; i ++) {
        sibling->pageNoArray[i] = Page::INVALID_NUMBER;
    }
    int index = INTARRAYNONLEAFSIZE/2; // 2d + 1 = 1023; d = 511
    curr->keyArray[index] = -1;
    index ++;
    int i;
    for(i = index; i < INTARRAYNONLEAFSIZE; i ++) {
        sibling->keyArray[i-index] = curr->keyArray[i];
        sibling->pageNoArray[i-index] = curr->pageNoArray[i];
        curr->keyArray[i] = -1;
        curr->pageNoArray[i] = Page::INVALID_NUMBER;
    }
    sibling->pageNoArray[i-index] = curr->pageNoArray[i];
    curr->pageNoArray[i] = Page::INVALID_NUMBER;
    if(*new_child_key > sibling->keyArray[0]) {
        insert_nonleaf(sibling, *new_child_key, *new_page_id);
    } else {
        insert_nonleaf(curr, *new_child_key, *new_page_id);
    }
    new_child_key = &sibling->keyArray[0];
    new_page_id = &new_sibling_id;
    bufMgr->unPinPage(this->file, new_sibling_id, true);
}

// Determine whether a node is full

void BTreeIndex::insert(int level, badgerdb::Page *rootpage, badgerdb::RIDKeyPair<int> new_pair,
                              int *new_child_key, PageId *new_page_id, badgerdb::BufMgr *b) {
    // If this is a leaf node
    if(level == 0) {
        LeafNodeInt* curr = reinterpret_cast<LeafNodeInt *>(rootpage);
//        int index = 0;
//        while(curr->keyArray[index] != -1 && index < INTARRAYLEAFSIZE)
//            index ++;
        // If there is not enough space
        if(!is_empty_leaf(curr)) {
            // split
            split_leaf(curr, new_child_key, new_page_id, new_pair);
            return;
        } else {
            // if there is enough space
            // insert
            insert_leaf(curr, new_pair);
            return;
        }
    } else {
        int index = 0;
        NonLeafNodeInt* curr = reinterpret_cast<NonLeafNodeInt *>(rootpage);
        // find the index of the key-page
        if(new_pair.key < curr->keyArray[0]) {
            index = 0;
        }
        else {
            while(curr->pageNoArray[index+1]!=Page::INVALID_NUMBER && index <= INTARRAYNONLEAFSIZE) {
                if(new_pair.key > curr->keyArray[index]) {
                    index ++;
                } else {
                    break;
                }
            }
        }
        // curr->page[index + 1] is the pageid
        Page* next;
        bufMgr->readPage(file, curr->pageNoArray[index], next);
        insert(level-1, next, new_pair, new_child_key, new_page_id, b);
        bufMgr->unPinPage(this->file, curr->pageNoArray[index], true);
        // if the child has split
        if(*new_child_key != -1) {
            // if this Nonleaf page has space
            if(is_empty_nonleaf(curr)) {
                insert_nonleaf(curr, *new_child_key, *new_page_id);
                *new_child_key = -1;
            } else {
                split_nonleaf(curr, new_child_key, new_page_id);
            }
        }

    }
    return;
}


const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
    RIDKeyPair<int> ridKeyPair;
    ridKeyPair.set(rid, *(int*)key);
    //std::cout<<*(int*)key<<std::endl;
    if(*(int*)key == 7314){

    }
// std::cout<<"this is"<<*(int*)key<<std::endl;
    // Read the metapage to get the level info
    // if there is no root, construct one
    PageId root_id;
    Page *root_p;
    // if the file don't have the root
    if(this->rootPageNum == Page::INVALID_NUMBER) {
        // allocate a new page
        bufMgr->allocPage(this->file, root_id, root_p);
        this->metaInfo->rootPageNo = root_id;
        this->metaInfo->root_is_leaf = true;
        this->metaInfo->root_level = 0;
        this->root_level = 0;
        LeafNodeInt* temp = reinterpret_cast<LeafNodeInt *>(root_p);
        for(int i = 0; i < INTARRAYLEAFSIZE; i ++) {
            temp->keyArray[i] = -1;
        }
        temp->rightSibPageNo = Page::INVALID_NUMBER;
        this->rootPageNum = root_id;
    } else {
        this->bufMgr->readPage(this->file, this->rootPageNum, root_p);
    }

    // Now we get the root_p as our root
    int pass = -1;
    int *new_child_key = &pass;
    PageId new_page_id = Page::INVALID_NUMBER;
    // insert the new record
    insert(this->root_level, root_p, ridKeyPair, new_child_key, &new_page_id , this->bufMgr);
    // If there is new element need to be added
    if(*new_child_key != -1) {
        Page *new_root;
        PageId new_root_id;
        this->bufMgr->allocPage(file, new_root_id, new_root);
        NonLeafNodeInt *temp_root = reinterpret_cast<NonLeafNodeInt*>(new_root);
        // init
        // Use the pageNoArray[i] = Page::INVALID_NUMBER to mark it unused
        temp_root->level = this->root_level + 1;
        for(int i = 0; i < INTARRAYNONLEAFSIZE+1; i ++) {
            temp_root->pageNoArray[i] = Page::INVALID_NUMBER;
        }
        temp_root->keyArray[0] = *new_child_key;
        temp_root->pageNoArray[0] = metaInfo->rootPageNo;
        temp_root->pageNoArray[1] = new_page_id;
        bufMgr->unPinPage(this->file, this->rootPageNum, true);
        // update the metadata
        this->metaInfo->rootPageNo = new_root_id;
        this->metaInfo->root_level = temp_root->level;
        this->root_level = temp_root->level;
	//std::cout<<"the level is "<<this->root_level<<std::endl;
        this->metaInfo->root_is_leaf = false;
        this->rootPageNum = new_root_id;
        bufMgr->unPinPage(this->file, new_root_id, true);
    } else {
        bufMgr->unPinPage(this->file, this->rootPageNum, true);
    }
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------
void BTreeIndex::scan(Page* curr, int level, PageId pagenum) {
    // Now is in the leaf node
//    Page* test;
//    bufMgr->readPage(file, this->metaInfo->rootPageNo, test);
//    NonLeafNodeInt* test_root = reinterpret_cast<NonLeafNodeInt*>(test);
//
    if(level == 0) {
	//std::cout<<"-------3----"<<std::endl;
        LeafNodeInt* curr_leaf = reinterpret_cast<LeafNodeInt*>(curr);
        if(this->lowOp == GT) {
            int i = 0;
            // stop until if find some value lager than the lowValint
            while(curr_leaf->keyArray[i] <= this->lowValInt && i < INTARRAYLEAFSIZE) {
                // You don't find it
                if(curr_leaf->keyArray[i] == -1 )
                    break;
                i ++;
            }
            this->currentPageNum = pagenum;
            this->currentPageData = curr;
            if(i == INTARRAYLEAFSIZE) {
                this->nextEntry = -1; // not find
            } else if(curr_leaf->keyArray[i] == -1) {
                this->nextEntry = -1;
            } else {
                this->nextEntry = i;
            }
        }
        else {
            int i = 0;
            while(curr_leaf->keyArray[i] < this->lowValInt && i < INTARRAYLEAFSIZE) {
                // You don't find it
                if(curr_leaf->keyArray[i] == -1 )
                    break;
                i ++;
            }
            this->currentPageNum = pagenum;
            this->currentPageData = curr;
            if(i == INTARRAYLEAFSIZE) {
                this->nextEntry = -1; // not find
                throw new NoSuchKeyFoundException;
            } else if(curr_leaf->keyArray[i] == -1) {
                this->nextEntry = -1;
                throw new NoSuchKeyFoundException;
            } else {
                this->nextEntry = i;
            }
        }
        return;
     }
    else {
        // find a place to goto the  next tree node
	//std::cout<<"-------4-----"<<std::endl;
        NonLeafNodeInt *curr_nonleaf = reinterpret_cast<NonLeafNodeInt*>(curr);
	// for(int i = 0; i <INTARRAYNONLEAFSIZE; i ++) {
	// 	if(curr_nonleaf->keyArray[i]>0){
	// 		std::cout<<curr_nonleaf->keyArray[i]<<std::endl;
	// 	}
	// }
	// for(int i = 0; i <INTARRAYNONLEAFSIZE; i ++) {
 //                if(curr_nonleaf->pageNoArray[i]>0){
 //                        std::cout<<curr_nonleaf->pageNoArray[i]<<std::endl;
 //                }
 //        }
        this->currentPageNum = pagenum;
        this->currentPageData = curr;
        int i =0;
        while(curr_nonleaf->keyArray[i] <= this->lowValInt) {
            i ++;
            if(i == INTARRAYNONLEAFSIZE){
                break;
            }
            if(curr_nonleaf->pageNoArray[i+1] == Page::INVALID_NUMBER)
                break;
        }
        Page *next;
        bufMgr->readPage(file, curr_nonleaf->pageNoArray[i], next);
        bufMgr->unPinPage(file, this->currentPageNum, false);
        scan(next, level-1, curr_nonleaf->pageNoArray[i]);
     //   bufMgr->printSelf();
    }
}

void BTreeIndex::maintain() {
    this->metaInfo->attrType = this->attributeType;
    this->metaInfo->attrByteOffset = this->attrByteOffset;
    this->metaInfo->rootPageNo = this->rootPageNum;
    this->metaInfo->root_level = this->root_level;
    for(int k = 0; k < this->name.length(); k ++) {
        metaInfo->relationName[k] = this->name[k];
    }
}
const void BTreeIndex::startScan(const void* lowValParm,
				   const Operator lowOpParm,
				   const void* highValParm,
				   const Operator highOpParm)
{
  //  std::cout<<"enter start scan"<<std::endl;
   // std::cout<<"2"<<std::endl;
    if(this->scanExecuting == true) {
        // release
	////
	//std::cout<<"what??"<<std::endl;
        bufMgr->unPinPage(file,currentPageNum,false);
    }
   // std::cout<<"this->level "<<this->root_level<<" |2 pagenum "<<this->currentPageNum<<std::endl;
    if(lowOpParm != GTE && lowOpParm != GT)
        throw BadOpcodesException();
    if(highOpParm != LTE && highOpParm != LT)
        throw BadOpcodesException();
    // set the startscan to be true
    if(lowOpParm != GT && lowOpParm != GTE)
        throw BadOpcodesException();
    if(highOpParm != LT && highOpParm != LTE)
        throw BadOpcodesException();
   // std::cout<<"this->level "<<this->root_level<<" |3 pagenum "<<this->currentPageNum<<std::endl;
    this->scanExecuting = true;
    this->lowValInt = *(int *)lowValParm;
    this->lowOp = lowOpParm;
    this->highValInt = *(int *)highValParm;
    this->highOp = highOpParm;
    this->currentPageNum = this->rootPageNum;
    if(lowValInt > highValInt)
        throw BadScanrangeException();
    //std::cout<<"this->level "<<this->root_level<<" |1 pagenum "<<this->currentPageNum<<std::endl;
    bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
    //std::cout<<"this->level "<<this->root_level<<" | pagenum "<<this->currentPageNum<<std::endl;
    scan(this->currentPageData, this->root_level, this->currentPageNum);

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId& outRid)
{
    if(currentPageNum == Page::INVALID_NUMBER) {
        throw IndexScanCompletedException();
    }
    //std::cout<<this->metaInfo->rootPageNo<<std::endl;
    if(this->scanExecuting == false)
            throw ScanNotInitializedException();
    Page *curr = currentPageData;
    //bufMgr->readPage(this->file, this->currentPageNum, curr);
    LeafNodeInt* curr_leaf = reinterpret_cast<LeafNodeInt*>(curr);
    int key = curr_leaf->keyArray[this->nextEntry];
    //std::cout<<"next entry is "<<key<<std::endl;
    if(this->highOp == LT) {
        if(key >= this->highValInt)
            throw IndexScanCompletedException();
    }
    if(this->highOp == LTE) {
        if(key > this->highValInt)
            throw IndexScanCompletedException();
    }
    outRid.page_number = curr_leaf->ridArray[this->nextEntry].page_number;
    outRid.slot_number = curr_leaf->ridArray[this->nextEntry].slot_number;
    if(this->nextEntry == INTARRAYLEAFSIZE-1) {
        bufMgr->unPinPage(file, this->currentPageNum, false);
        this->currentPageNum = curr_leaf->rightSibPageNo;
        if(this->currentPageNum == Page::INVALID_NUMBER)
            throw IndexScanCompletedException();
        bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
        this->nextEntry = 0;
        return;
    }
    else if(curr_leaf->keyArray[this->nextEntry + 1] == -1) {
        bufMgr->unPinPage(file, this->currentPageNum, false);
        this->currentPageNum = curr_leaf->rightSibPageNo;
        if(this->currentPageNum == Page::INVALID_NUMBER)
            return;
        if(this->currentPageNum == 0) {
            return;
        }
        bufMgr->readPage(file, this->currentPageNum, this->currentPageData);
        this->nextEntry = 0;
        return;
    }
    this->nextEntry += 1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
    if(this->scanExecuting == false)
        throw ScanNotInitializedException();
    if(this->currentPageNum != Page::INVALID_NUMBER){
        bufMgr->unPinPage(file,currentPageNum,false);
    }
    this->scanExecuting = false;
   // bufMgr->printSelf();
}

}
