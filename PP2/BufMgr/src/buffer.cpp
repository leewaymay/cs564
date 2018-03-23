/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 * 
 * 	   Name    			ID
 * Jiewei Hong  	jhong58
 * Wei Li           wli284
 * Wei Tang         wtang44
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
  		if ( bufDescTable[i].valid && bufDescTable[i].dirty) {
			//Dirty pages, need to be flushed out
  			bufDescTable[i].file->writePage(bufPool[i]);
  		}
	}
	// clean bufpool bufdesctable array
	delete[] bufPool;
	delete[] bufDescTable;
	delete hashTable;
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
	for (FrameId i = 1; i <= numBufs; i++) {
		advanceClock();
		if ( !bufDescTable[clockHand].valid ) {
			// The current frame has not been used. Choose this frame
			frame = clockHand;
			return;
		}
		else if ( bufDescTable[clockHand].refbit ) {
			// The current frame refbit is true. Set to false and advance
			bufDescTable[clockHand].refbit = false;
			// reset counter to restart the search
			i = 0;
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
	}
	// All frames pinned, throws BufferExceededException  
	throw BufferExceededException();
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try {
		// First check whether the page is already in the buffer pool hashTable by invoking the lookup() method
		hashTable->lookup(file, pageNo, frameNo);
		
		// Case 2: Page is in the buffer pool. Increment the pinCnt for the page, set the appropriate refbit, 
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;

		// and then return a pointer to the frame containing the page via the page parameter.
		page = &(bufPool[frameNo]);
		
	}
	catch(HashNotFoundException e) {
		// Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame
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
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try {
		hashTable->lookup(file, pageNo, frameNo);
		// if dirty == true, sets the dirty bit.
		if (dirty==true) {
			bufDescTable[frameNo].dirty = true;
		}
		// Throws PAGENOTPINNED if the pin count is already 0
		if (bufDescTable[frameNo].pinCnt == 0) {
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		else {
			// Decrements the pinCnt of the frame containing (file, PageNo)
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
	/* Scan bufTable for pages belonging to the file. If there exists any pages that with 
	   non-zero pincount, should not flush any pages back to the file and throw PagePinnedException. */
	for(FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file)	{
			if (bufDescTable[i].pinCnt > 0)	{
				throw PagePinnedException(file->filename(), bufDescTable[i].pageNo, i);
			}
		}
	}
	/* For each page that belongs to the file, if it is dirty, flush it back to the file; then remove the
	   page from the hashtable, and clear the status of the frame.
	*/	
	for(FrameId i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file)	{
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
		// check if the page to be deleted is allocated a frame in the buffer pool
		hashTable->lookup(file, PageNo, frameNo);
		// if found, clear the frame.
		bufDescTable[frameNo].Clear();
		// and remove the corresponding entry from hashtable
		hashTable->remove(file, PageNo);  
	}
	catch(HashNotFoundException e) {
	}
	// delete the page from the file
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
