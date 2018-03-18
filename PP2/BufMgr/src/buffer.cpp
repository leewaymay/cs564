/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	// Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table.
	for (FrameId i = 0; i < numBufs; i++) 
	{
  		if ( bufDescTable[i].valid && bufDescTable[i].dirty)	{
			//Dirty pages, need to be flushed out
  			bufDescTable[i].file->writePage(bufPool[i]);
  		}
	}
	// TODO clean bufpool bufdesctable array
	delete[] bufPool;
	delete[] bufDescTable;
}

void BufMgr::advanceClock()
{
	// Advance clock to next frame in the buffer pool.
	clockHand++;
	if ( clockHand >= numBufs ) clockHand = clockHand % numBufs;
}

void BufMgr::allocBuf(FrameId & frame) 
{
	// Allocates a free frame using the clock algorithm; This private method will get called by the readPage() and allocPage() methods described below.
	FrameId occupiedframe = 0;
	while ( occupiedframe < numBufs) {
		advanceClock();
		if ( !bufDescTable[clockHand].valid ) {
			// The current frame has not been used. Choose this frame
			frame = clockHand;
			return;
		}
		else if ( bufDescTable[clockHand].refbit ) {
			// The current frame refbit is true. Set to false and advance
			bufDescTable[clockHand].refbit = false;
		}
		else if ( bufDescTable[clockHand].pinCnt == 0) {
			// The current frame is not pinned. Choose this frame.
			if(bufDescTable[clockHand].dirty) {
				// The page of frame is dirty, writing back to disk.
  				bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			}
			frame = clockHand;
			return;
		}
		if (bufDescTable[clockHand].pinCnt > 0) occupiedframe++;
	}

	// All frames pinned, throws BufferExceededException  
	throw BufferExceededException();

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	// First check whether the page is already in the buffer pool hashTable by invoking the lookup() method,
	// which may throw HashNotFoundException when page is not in the buffer pool, on the
	/**
	hashtable to get a frame number. There are two cases to be handled depending on the
	outcome of the lookup() call:
	• Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and
	then call the method file->readPage() to read the page from disk into the buffer pool
	frame. Next, insert the page into the hashtable. Finally, invoke Set() on the frame to
	set it up properly. Set() will leave the pinCnt for the page set to 1. Return a pointer
	to the frame containing the page via the page parameter.
	• 
	**/
	FrameId frameNo;


	try {
		hashTable->lookup(file, pageNo, frameNo);
		
		// Case 2: Page is in the buffer pool.  increment

		// Set the appropriate refbit, he pinCnt for the page
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;

		// and then return a pointer to the frame containing the page via the page parameter.
		page = &(bufPool[frameNo]);
		
	}
	catch(HashNotFoundException e) {
		
		// Case 1: Page is not in the buffer pool. 
		
		try {
			// Call allocBuf() to allocate a buffer frame
			allocBuf(frameNo);
			// Call file->readPage() to read the page from disk into the buffer pool frame 
			bufPool[frameNo] = file->readPage(pageNo);
			// Insert the page into the hashtable
			if (bufDescTable[frameNo].valid) {
				hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);
			}
			hashTable->insert(file, pageNo, frameNo);
			//  invoke Set() on the frame to set it up properly
			bufDescTable[frameNo].Set(file, pageNo);
			// Return a pointer	to the frame containing the page via the page parameter
			page = &(bufPool[frameNo]);			
		}
		catch(BufferExceededException e)
		{
			//TODO anything?
			std::cout << "BufferExceededException!\n";
			return;
		}

	}

	



}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	/* Decrements the pinCnt of the frame containing (file, PageNo) and, if dirty == true, sets
	the dirty bit. Throws PAGENOTPINNED if the pin count is already 0. Does nothing if
	page is not found in the hash table lookup.
	*/
	FrameId frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo);
		if (dirty==true) {
			bufDescTable[frameNo].dirty = true;
		}
		if (bufDescTable[frameNo].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		else {
			bufDescTable[frameNo].pinCnt--;
			if (bufDescTable[frameNo].pinCnt == 0) {
				bufDescTable[frameNo].refbit = true;
			}
		}
	}
	catch(HashNotFoundException e) {
	}
}

void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file)	{
			if (bufDescTable[i].pinCnt > 0)	{
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
			}
			if (! bufDescTable[i].valid) {
				throw BadBufferException(i, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
			if(bufDescTable[i].valid && bufDescTable[i].dirty)	{
				bufDescTable[i].file->writePage(bufPool[i]);
			}
			try {
				hashTable->remove(file, bufDescTable[i].pageNo);  
			}
			catch(HashNotFoundException e) {
			}
			bufDescTable[i].Clear();			
		}
	}	
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	/*
	The first step in this method is to to allocate an empty page in the specified file 

an entry is inserted into the
hash table and Set() is invoked on the frame to set it up properly. The method returns
both the page number of the newly allocated page to the caller via the pageNo parameter
and a pointer to the buffer frame allocated for the page via the page parameter.
	*/

	FrameId frameNo;
	try {
		// Call file->allocatePage() , return a newly allocated page.
		Page tempPage = file->allocatePage();
		pageNo = tempPage.page_number();
		// AllocBuf() is called to obtain a buffer pool frame
		allocBuf(frameNo);
		// Insert entry to hashtable
		if (bufDescTable[frameNo].valid) {
			hashTable->remove(bufDescTable[frameNo].file, bufDescTable[frameNo].pageNo);				
		}
		hashTable->insert(file, pageNo, frameNo);
		// Invoke set on the frame
		bufDescTable[frameNo].Set(file, pageNo);
		bufPool[frameNo] = tempPage;
		// return the page in the buffer frame
		page = &(bufPool[frameNo]);			

	}
	catch(BufferExceededException e) {
		disposePage(file, pageNo);
		throw BufferExceededException();
	}
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    FrameId frameNo;
	try {
		hashTable->lookup(file, PageNo, frameNo);
		bufDescTable[frameNo].Clear();
		hashTable->remove(file, PageNo);  
	}
	catch(HashNotFoundException e) {
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
	std::cout << "clockHand =  " << clockHand <<"\n";
}

}
