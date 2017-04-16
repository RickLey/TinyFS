package com.client;


/* We can tell if an RID is invalid because it has been deleted, but we can't tell
 *  If an RID is invalid because the offset is wrong. That would require prof's method
 *  of having an array structure at the end of a chunk. Then it would just be checking
 *  if an index is inbounds and using that to offset into the array to get the actual offset
 */
 
public class RID {
	public String chunkHandle;
	public int chunkOffset;
}
