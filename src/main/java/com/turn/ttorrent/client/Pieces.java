/**
 * Copyright (C) 2011-2012 Turn, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.turn.ttorrent.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turn.ttorrent.client.peer.SharingPeer;
import com.turn.ttorrent.common.Torrent;


/**
 * A lot of torrent pieces.
 *
 * <p>
 * This class represents a bunch of torrent pieces. Torrents are made of pieces,
 * which are in turn made of blocks that are exchanged using the peer protocol.
 * The piece length is defined at the torrent level, but the last piece that
 * makes the torrent might be smaller.
 * </p>
 *
 * <p>
 * If the torrent has multiple files, pieces can spread across file boundaries.
 * The TorrentByteStorage abstracts this problem to give Piece objects the
 * impression of a contiguous, linear byte storage.
 * </p>
 *
 * <p>
 * This implementation keeps a large number of torrent pieces in a single class,
 * creating parallel arrays to keep data about each piece. it is assumed that
 * data at the same index in different arrays belong to the same piece.
 * </p>
 *
 * @author mpetazzoni & Akos Maroy
 */
public class Pieces {

	private static final Logger logger =
		LoggerFactory.getLogger(Pieces.class);

	private final SharedTorrent torrent;
	private final int nPieces;
    private final long length;
    private final long lastLength;

	private volatile BitSet valid;
	private final int[] seen;
	private final ByteBuffer[] data;

	/**
	 * Initialize a bunch of pieces.
	 *
	 * @param bucket The parent torrent.
	 * @param nPieces the number of pieces to store.
	 * @param length the length of the pieces
	 * @param lastLength the length of the last piece
	 */
	public Pieces(SharedTorrent torrent, int nPieces, long length, long lastLength) {
		this.torrent = torrent;
		this.nPieces = nPieces;
		this.length = length;
		this.lastLength = lastLength;

		// Piece is considered invalid until first check.
		this.valid = new BitSet(nPieces);
		valid.clear();

		// Piece start unseen
		this.seen = new int[nPieces];

		this.data = new ByteBuffer[nPieces];
	}

    /**
     * Tells the number of pieces.
     *
     * @return the number of pieces
     */
	public int getNumPieces() {
	    return nPieces;
	}

	/**
	 * Tells whether a piece's data is valid or not.
	 *
	 * @param idx the index of the piece.
	 */
	public boolean isValid(int idx) {
		return this.valid.get(idx);
	}

	/**
	 * Returns the index of a piece in the torrent.
     *
     * @param idx the index of the piece.
	 */
	public int getIndex(int idx) {
		return idx;
	}

	/**
	 * Returns the size, in bytes, of a piece.
	 *
	 * <p>
	 * All pieces, except the last one, are expected to have the same size.
	 * </p>
     *
     * @param idx the index of the piece.
	 */
	public long size(int idx) {
		return idx < data.length - 1 ? length : lastLength;
	}

	public ByteBuffer getData(int idx) {
	    return data[idx];
	}

	/**
	 * Tells whether a piece is available in the current connected peer swarm.
     *
     * @param idx the index of the piece.
	 */
	public boolean available(int idx) {
		return this.seen[idx] > 0;
	}

	/**
	 * Mark this piece as being seen at the given peer.
	 *
     * @param idx the index of the piece.
	 * @param peer The sharing peer this piece has been seen available at.
	 */
	public void seenAt(int idx, SharingPeer peer) {
		this.seen[idx]++;
	}

	/**
	 * Mark this piece as no longer being available at the given peer.
	 *
     * @param idx the index of the piece.
	 * @param peer The sharing peer from which the piece is no longer available.
	 */
	public void noLongerAt(int idx, SharingPeer peer) {
		this.seen[idx]--;
	}

	/**
	 * Validates this piece.
	 *
     * @param idx the index of the piece.
	 * @return Returns true if this piece, as stored in the underlying byte
	 * storage, is valid, i.e. its SHA1 sum matches the one from the torrent
	 * meta-info.
	 */
	public boolean validate(int idx) throws IOException {
		if (this.torrent.isSeeder()) {
			logger.trace("Skipping validation of {} (seeder mode).", this);
			this.valid.set(idx);
			return true;
		}

		logger.trace("Validating {}...", this);

		try {
			ByteBuffer buffer = this._read(idx, 0, size(idx));
			boolean hashOk = checkHash(idx, buffer);

			synchronized (this.valid) {
			    this.valid.set(idx, hashOk);
			}

			return hashOk;
		} catch (NoSuchAlgorithmException nsae) {
			logger.error("{}", nsae);
            synchronized (this.valid) {
                this.valid.clear(idx);
            }

            return false;
		}
	}

	protected boolean checkHash(int idx, ByteBuffer data)
	                                          throws NoSuchAlgorithmException {
		byte[] calculatedHash = Torrent.hash(data);

		int torrentHashPosition = idx * Torrent.PIECE_HASH_SIZE;
		ByteBuffer torrentHash = this.torrent.getPiecesHashes();
		for (int i = 0; i < Torrent.PIECE_HASH_SIZE; i++) {
			byte value = torrentHash.get(torrentHashPosition + i);
			if (value != calculatedHash[i]) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Internal piece data read function.
	 *
	 * <p>
	 * This function will read the piece data without checking if the piece has
	 * been validated. It is simply meant at factoring-in the common read code
	 * from the validate and read functions.
	 * </p>
	 *
     * @param idx the index of the piece.
	 * @param offset Offset inside this piece where to start reading.
	 * @param length Number of bytes to read from the piece.
	 * @return A byte buffer containing the piece data.
	 * @throws IllegalArgumentException If <em>offset + length</em> goes over
	 * the piece boundary.
	 * @throws IOException If the read can't be completed (I/O error, or EOF
	 * reached, which can happen if the piece is not complete).
	 */
	private ByteBuffer _read(int idx, long offset, long length)
	                                                      throws IOException {
		if (offset + length > size(idx)) {
			throw new IllegalArgumentException("Piece#" + idx +
				" overrun (" + offset + " + " + length + " > " +
				this.length + ") !");
		}

		// TODO: remove cast to int when large ByteBuffer support is
		// implemented in Java.
		ByteBuffer buffer = ByteBuffer.allocate((int)length);
		int bytes = this.torrent.getBucket().read(buffer, this.getBucketOffset(idx)
		                                                + offset);
		buffer.rewind();
		buffer.limit(bytes >= 0 ? bytes : 0);
		return buffer;
	}

	/**
	 * Read a piece block from the underlying byte storage.
	 *
	 * <p>
	 * This is the public method for reading this piece's data, and it will
	 * only succeed if the piece is complete and valid on disk, thus ensuring
	 * any data that comes out of this function is valid piece data we can send
	 * to other peers.
	 * </p>
	 *
     * @param idx the index of the piece.
	 * @param offset Offset inside this piece where to start reading.
	 * @param length Number of bytes to read from the piece.
	 * @return A byte buffer containing the piece data.
	 * @throws IllegalArgumentException If <em>offset + length</em> goes over
	 * the piece boundary.
	 * @throws IllegalStateException If the piece is not valid when attempting
	 * to read it.
	 * @throws IOException If the read can't be completed (I/O error, or EOF
	 * reached, which can happen if the piece is not complete).
	 */
	public ByteBuffer read(int idx, long offset, int length)
		throws IllegalArgumentException, IllegalStateException, IOException {
		if (!this.valid.get(idx)) {
			throw new IllegalStateException("Attempting to read an " +
					"known-to-be invalid piece!");
		}

		return this._read(idx, offset, length);
	}

	/**
	 * Record the given block at the given offset in this piece.
	 *
	 * <p>
	 * <b>Note:</b> this has synchronized access to the underlying byte storage.
	 * </p>
	 *
     * @param idx the index of the piece.
	 * @param block The ByteBuffer containing the block data.
	 * @param offset The block offset in this piece.
	 */
	public void record(int idx, ByteBuffer block, int offset)
	                                                    throws IOException {
    		if (this.data[idx] == null || offset == 0) {
    			// TODO: remove cast to int when large ByteBuffer support is
    			// implemented in Java.
    			this.data[idx] = ByteBuffer.allocate((int) size(idx));
    		}

        synchronized(data[idx]) {
    		ByteBuffer bb = data[idx];

    		int pos = block.position();
    		bb.position(offset);
    		bb.put(block);
    		block.position(pos);

    		if (block.remaining() + offset == size(idx)) {
    			bb.rewind();
    			logger.trace("Recording {}...", this);
    			this.torrent.getBucket().write(bb, this.getBucketOffset(idx));
    			this.data[idx] = null;
    		}
	    }
	}

	long getBucketOffset(int idx) {
		return ((long)idx) * this.torrent.getPieceLength();
	}

	/**
	 * Return a human-readable representation of a piece.
	 *
	 * @param idx the index a piece to represent
	 */
    public String toString(int idx) {
		return String.format("piece#%4d%s",
			idx,
			this.isValid(idx) ? "+" : "-");
	}

	/**
	 * Piece comparison function for ordering pieces based on their
	 * availability.
	 *
     * @param one the index of a piece to compare
	 * @param other The index of a piece to compare with
	 */
	public int compareTo(int one, int other) {
		if (one == other) {
			return 0;
		}

		if (seen[one] < seen[other]) {
			return -1;
		} else {
			return 1;
		}
	}

	/**
	 * A {@link Callable} to call the piece validation function.
	 *
	 * <p>
	 * This {@link Callable} implementation allows for the calling of the piece
	 * validation function in a controlled context like a thread or an
	 * executor. It returns the piece it was created for. Results of the
	 * validation can easily be extracted from the {@link Pieces} object after
	 * it is returned.
	 * </p>
	 *
	 * @author mpetazzoni
	 */
	public static class CallableHasher implements Callable<Integer> {

	    private final Pieces pieces;
		private final int idx;

		public CallableHasher(Pieces pieces, int idx) {
		    this.pieces = pieces;
			this.idx = idx;
		}

		@Override
		public Integer call() throws IOException {
			pieces.validate(idx);
			return idx;
		}
	}
}
