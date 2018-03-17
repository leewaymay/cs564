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
			//TODO			
  		}
	}
	delete[] BbufPool;
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
	int occupiedframe = 0;
	while ( occupiedframe < numBufs) {
		advanceClock();
		if ( !bufDesc[clockHand].valid ) {
			// The current frame has not been used. Choose this frame
			frame = clockHand;
			return;
		}
		else if ( bufDesc[clockHand].refbit ) {
			// The current frame refbit is true. Set to false and advance
			bufDesc[clockHand].refbit = false;
			continue;
		}
		else if ( bufDesc[clockHand].pinCnt == 0) {
			// The current frame is not pinned. Choose this frame.
			if(bufDesc[clockHand].dirty) {
				// The page of frame is dirty, writing back to disk.
				//TODO
			}
			frame = clockHand;
			return;
		}
		else {
			if (bufDesc[clockHand].pinCnt > 0) occupied++;
		}
		
	}

	// All frames pinned, throws BufferExceededException  
	throw BufferExceededException();

}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	// First check whether the page is already in the buffer pool 
		hashTable.
by invoking the lookup() method,
which may throw HashNotFoundException when page is not in the buffer pool, on the
hashtable to get a frame number. There are two cases to be handled depending on the
outcome of the lookup() call:
• Case 1: Page is not in the buffer pool. Call allocBuf() to allocate a buffer frame and
then call the method file->readPage() to read the page from disk into the buffer pool
frame. Next, insert the page into the hashtable. Finally, invoke Set() on the frame to
set it up properly. Set() will leave the pinCnt for the page set to 1. Return a pointer
to the frame containing the page via the page parameter.
• Case 2: Page is in the buffer pool. In this case set the appropriate refbit, increment
the pinCnt for the page, and then return a pointer to the frame containing the page
via the page parameter.
	FrameId *frame;
	
	try {
		allocBuf( frame);
	}
	catch(BufferExceededException e)
	{
	}


}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
}

void BufMgr::flushFile(const File* file) 
{
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
    
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
}

}
