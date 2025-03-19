/**
 * @file buf.C
 *
 * This file contains the implementation of allocBuf, readPage, unPinPage, allocPage,
 * which allows the buffer manager to allocate a page, read the content of a
 * page, deallocate a page.
 *
 * @author Laura Kuo
 * @studentID 9082113458
 * @author Henry Pruski
 * @studentID 9083254160
 * @author Yi Wei
 * @studentID 9084811984
 */

#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    int attempts = numBufs;

    while (attempts--) {
        BufDesc &currFrame = bufTable[clockHand];

        //if frame is available, return this frame        
        if (!currFrame.valid) {
            frame = clockHand;
            currFrame.Clear();
            advanceClock();
            return OK;
        }

        //if recently referenced, clear refbit and move to next frame
        if (currFrame.refbit) {
            currFrame.refbit = false;
            continue;
        } 

        if (currFrame.pinCnt == 0) {
            //write to memory if dirty
            if (currFrame.dirty) {
                Status rc = currFrame.file->writePage(currFrame.pageNo, &bufPool[clockHand]);
                
                if (rc == UNIXERR) {
                    return UNIXERR;
                }
            }

            //remove page from hashtable
            hashTable->remove(currFrame.file, bufTable[clockHand].pageNo);
            frame = clockHand;
            currFrame.Clear();

            return OK;
        } else {
            advanceClock();
        }
    }

    //all pages are pinned
    return BUFFEREXCEEDED;
}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status rc = hashTable->lookup(file, PageNo, frameNo);

    //if page is already in buffer, increment pin count and set ref bit
    if (rc == OK) {
        bufTable[frameNo].pinCnt++;
        bufTable[frameNo].refbit = true;
        //assign address of page to pointer
        page = &bufPool[frameNo];

        return OK;
    } 
    
    
    //page is not in buffer, allocate a frame to the buffer
    rc = allocBuf(frameNo);
    //if successfuly allocated, insert page and set buffer descriptor
    if (rc == OK) {
        rc = file->readPage(PageNo, &bufPool[frameNo]);
        if (rc == OK) {
            rc = hashTable->insert(file, PageNo, frameNo);
            bufTable[frameNo].Set(file, PageNo);
            //assign address of page to pointer
            page = &bufPool[frameNo];
        }
    }

    return rc;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    //lookup frame number of the file and page
    Status rc = hashTable->lookup(file, PageNo, frameNo);

    //return error if page not found
    if (rc != OK) return HASHNOTFOUND;

    BufDesc &currFrame = bufTable[frameNo];

    //return error if page not pinned
    if (currFrame.pinCnt <= 0) return PAGENOTPINNED;

    //decrement pin count
    currFrame.pinCnt--;

    //set dirty bit if requested by user
    if (dirty) {
        currFrame.dirty = true;
    }

    return OK;
}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status rc = file->allocatePage(pageNo);

    //return error if failed to allocage page
    if (rc != OK) return UNIXERR;

    //allocate a frame
    int frameNo;
    rc = allocBuf(frameNo);

    //return error if failed to allocate frame
    if (rc != OK) return rc;

    //insert page into frame
    rc = hashTable->insert(file, pageNo, frameNo);

    //return error if failed to insert page into frame
    if (rc != OK) return HASHTBLERROR;

    //set up buffer descriptor
    bufTable[frameNo].Set(file, pageNo);
    //assign address of page to pointer
    page = &bufPool[frameNo];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


