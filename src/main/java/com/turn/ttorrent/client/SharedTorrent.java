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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.turn.ttorrent.bcodec.InvalidBEncodingException;
import com.turn.ttorrent.client.peer.PeerActivityListener;
import com.turn.ttorrent.client.peer.SharingPeer;
import com.turn.ttorrent.client.storage.FileCollectionStorage;
import com.turn.ttorrent.client.storage.FileStorage;
import com.turn.ttorrent.client.storage.TorrentByteStorage;
import com.turn.ttorrent.common.Torrent;


/**
 * A torrent shared by the BitTorrent client.
 *
 * <p>
 * The {@link SharedTorrent} class extends the Torrent class with all the data
 * and logic required by the BitTorrent client implementation.
 * </p>
 *
 * <p>
 * <em>Note:</em> this implementation currently only supports single-file
 * torrents.
 * </p>
 *
 * @author mpetazzoni
 */
public class SharedTorrent extends Torrent implements PeerActivityListener {

	private static final Logger logger =
		LoggerFactory.getLogger(SharedTorrent.class);

	/** Randomly select the next piece to download from a peer from the
	 * RAREST_PIECE_JITTER available from it. */
	private static final int RAREST_PIECE_JITTER = 42;

	/** End-game trigger ratio.
	 *
	 * <p>
	 * Eng-game behavior (requesting already requested pieces from available
	 * and ready peers to try to speed-up the end of the transfer) will only be
	 * enabled when the ratio of completed pieces over total pieces in the
	 * torrent is over this value.
	 * </p>
	 */
	private static final float ENG_GAME_COMPLETION_RATIO = 0.95f;

	private final Random random;
	private boolean stop;

	private long uploaded;
	private long downloaded;
	private long left;

	private final TorrentByteStorage bucket;

	private final int pieceLength;
	private final ByteBuffer piecesHashes;

	private boolean initialized;
	private Pieces pieces;
	private final SortedSet<Integer> rarest;
	private BitSet completedPieces;
	private final BitSet requestedPieces;

	/**
	 * Create a new shared torrent from a base Torrent object.
	 *
	 * <p>
	 * This will recreate a SharedTorrent object from the provided Torrent
	 * object's encoded meta-info data.
	 * </p>
	 *
	 * @param torrent The Torrent object.
	 * @param destDir The destination directory or location of the torrent
	 * files.
	 * @throws FileNotFoundException If the torrent file location or
	 * destination directory does not exist and can't be created.
	 * @throws IOException If the torrent file cannot be read or decoded.
	 * @throws NoSuchAlgorithmException
	 */
	public SharedTorrent(Torrent torrent, File destDir)
		throws FileNotFoundException, IOException, NoSuchAlgorithmException {
		this(torrent, destDir, false);
	}

	/**
	 * Create a new shared torrent from a base Torrent object.
	 *
	 * <p>
	 * This will recreate a SharedTorrent object from the provided Torrent
	 * object's encoded meta-info data.
	 * </p>
	 *
	 * @param torrent The Torrent object.
	 * @param destDir The destination directory or location of the torrent
	 * files.
	 * @param seeder Whether we're a seeder for this torrent or not (disables
	 * validation).
	 * @throws FileNotFoundException If the torrent file location or
	 * destination directory does not exist and can't be created.
	 * @throws IOException If the torrent file cannot be read or decoded.
	 * @throws NoSuchAlgorithmException
	 */
	public SharedTorrent(Torrent torrent, File destDir, boolean seeder)
		throws FileNotFoundException, IOException, NoSuchAlgorithmException {
		this(torrent.getEncoded(), destDir, seeder);
	}

	/**
	 * Create a new shared torrent from meta-info binary data.
	 *
	 * @param torrent The meta-info byte data.
	 * @param destDir The destination directory or location of the torrent
	 * files.
	 * @throws FileNotFoundException If the torrent file location or
	 * destination directory does not exist and can't be created.
	 * @throws IOException If the torrent file cannot be read or decoded.
	 */
	public SharedTorrent(byte[] torrent, File destDir)
		throws FileNotFoundException, IOException, NoSuchAlgorithmException {
		this(torrent, destDir, false);
	}

	/**
	 * Create a new shared torrent from meta-info binary data.
	 *
	 * @param torrent The meta-info byte data.
	 * @param parent The parent directory or location the torrent files.
	 * @param seeder Whether we're a seeder for this torrent or not (disables
	 * validation).
	 * @throws FileNotFoundException If the torrent file location or
	 * destination directory does not exist and can't be created.
	 * @throws IOException If the torrent file cannot be read or decoded.
	 * @throws NoSuchAlgorithmException
	 * @throws URISyntaxException When one of the defined tracker addresses is
	 * invalid.
	 */
	public SharedTorrent(byte[] torrent, File parent, boolean seeder)
		throws FileNotFoundException, IOException, NoSuchAlgorithmException {
		super(torrent, seeder);

		if (parent == null || !parent.isDirectory()) {
			throw new IllegalArgumentException("Invalid parent directory!");
		}

		String parentPath = parent.getCanonicalPath();

		try {
			this.pieceLength = this.decoded_info.get("piece length").getInt();
			this.piecesHashes = ByteBuffer.wrap(this.decoded_info.get("pieces")
					.getBytes());

			if (this.piecesHashes.capacity() / Torrent.PIECE_HASH_SIZE *
					(long)this.pieceLength < this.getSize()) {
				throw new IllegalArgumentException("Torrent size does not " +
						"match the number of pieces and the piece size!");
			}
		} catch (InvalidBEncodingException ibee) {
			throw new IllegalArgumentException(
					"Error reading torrent meta-info fields!");
		}

		List<FileStorage> files = new LinkedList<FileStorage>();
		long offset = 0L;
		for (Torrent.TorrentFile file : this.files) {
			File actual = new File(parent, file.file.getPath());

			if (!actual.getCanonicalPath().startsWith(parentPath)) {
				throw new SecurityException("Torrent file path attempted " +
					"to break directory jail!");
			}

			actual.getParentFile().mkdirs();
			files.add(new FileStorage(actual, offset, file.size));
			offset += file.size;
		}
		this.bucket = new FileCollectionStorage(files, this.getSize());

		this.random = new Random(System.currentTimeMillis());
		this.stop = false;

		this.uploaded = 0;
		this.downloaded = 0;
		this.left = this.getSize();

		this.initialized = false;
		this.pieces = new Pieces(this, 0, 0, 0);
		this.rarest = Collections.synchronizedSortedSet(new TreeSet<Integer>());
		this.completedPieces = new BitSet();
		this.requestedPieces = new BitSet();
	}

	/**
	 * Create a new shared torrent from the given torrent file.
	 *
	 * @param source The <code>.torrent</code> file to read the torrent
	 * meta-info from.
	 * @param parent The parent directory or location of the torrent files.
	 * @throws IOException When the torrent file cannot be read or decoded.
	 * @throws NoSuchAlgorithmException
	 */
	public static SharedTorrent fromFile(File source, File parent)
		throws IOException, NoSuchAlgorithmException {
		FileInputStream fis = new FileInputStream(source);
		byte[] data = new byte[(int)source.length()];
		fis.read(data);
		fis.close();
		return new SharedTorrent(data, parent);
	}

	/**
	 * Get the number of bytes uploaded for this torrent.
	 */
	public long getUploaded() {
		return this.uploaded;
	}

	/**
	 * Get the number of bytes downloaded for this torrent.
	 *
	 * <p>
	 * <b>Note:</b> this could be more than the torrent's length, and should
	 * not be used to determine a completion percentage.
	 * </p>
	 */
	public long getDownloaded() {
		return this.downloaded;
	}

	/**
	 * Get the number of bytes left to download for this torrent.
	 */
	public long getLeft() {
		return this.left;
	}

	/**
	 * Tells whether this torrent has been fully initialized yet.
	 */
	public boolean isInitialized() {
		return this.initialized;
	}

	/**
	 * Stop the torrent initialization as soon as possible.
	 */
	public void stop() {
		this.stop = true;
	}

	/**
	 * Build this torrent's pieces array.
	 *
	 * <p>
	 * Hash and verify any potentially present local data and create this
	 * torrent's pieces array from their respective hash provided in the
	 * torrent meta-info.
	 * </p>
	 *
	 * <p>
	 * This function should be called soon after the constructor to initialize
	 * the pieces array.
	 * </p>
	 */
	public synchronized void init() throws InterruptedException, IOException {
		if (this.isInitialized()) {
			throw new IllegalStateException("Torrent was already initialized!");
		}

		int threads = getHashingThreadsCount();
		int nPieces = (int) (Math.ceil(
				(double)this.getSize() / this.pieceLength));
		int step = 10;

		long lastLength = bucket.size() - (pieceLength * (nPieces - 1));
		this.pieces = new Pieces(this, nPieces, pieceLength, lastLength);
		this.completedPieces = new BitSet(nPieces);
		this.piecesHashes.clear();

		ExecutorService executor = Executors.newFixedThreadPool(threads);
		List<Future<Integer>> results = new LinkedList<Future<Integer>>();

		try {
			logger.info("Analyzing local data for {} with {} threads ({} pieces)...",
				new Object[] { this.getName(), threads, nPieces });
			for (int idx=0; idx<nPieces; idx++) {

				// The last piece may be shorter than the torrent's global piece
				// length. Let's make sure we get the right piece length in any
				// situation.
				long off = ((long)idx) * this.pieceLength;
				long len = Math.min(
					this.bucket.size() - off,
					this.pieceLength);

				Callable<Integer> hasher = new Pieces.CallableHasher(pieces, idx);
				results.add(executor.submit(hasher));

				if (results.size() >= threads) {
					this.validatePieces(results);
				}

				if (idx / (float)nPieces * 100f > step) {
					logger.info("  ... {}% complete", step);
					step += 10;
				}
			}

			this.validatePieces(results);
		} finally {
			// Request orderly executor shutdown and wait for hashing tasks to
			// complete.
			executor.shutdown();
			while (!executor.isTerminated()) {
				if (this.stop) {
					throw new InterruptedException("Torrent data analysis " +
						"interrupted.");
				}

				Thread.sleep(10);
			}
		}

		logger.debug("{}: we have {}/{} bytes ({}%) [{}/{} pieces].",
			new Object[] {
				this.getName(),
				(this.getSize() - this.left),
				this.getSize(),
				String.format("%.1f", (100f * (1f - this.left / (float)this.getSize()))),
				this.completedPieces.cardinality(),
				this.pieces.getNumPieces()
			});
		this.initialized = true;
	}

	/**
	 * Process the pieces enqueued for hash validation so far.
	 *
	 * @param results The list of {@link Future}s of pieces to process.
	 */
	private void validatePieces(List<Future<Integer>> results)
			throws InterruptedException, IOException {
		try {
			for (Future<Integer> task : results) {
				Integer idx = task.get();
				if (this.pieces.isValid(idx)) {
					this.completedPieces.set(idx);
					this.left -= pieces.size(idx);
				}
			}

			results.clear();
		} catch (Exception e) {
			throw new IOException("Error while hashing a torrent piece!", e);
		}
	}


	public synchronized void close() {
		try {
			this.bucket.close();
		} catch (IOException ioe) {
			logger.error("Error closing torrent byte storage: {}",
				ioe.getMessage());
		}
	}

	/**
	 * Get the number of pieces in this torrent.
	 */
	public int getPieceCount() {
		if (this.pieces == null) {
			throw new IllegalStateException("Torrent not initialized yet.");
		}

		return this.pieces.getNumPieces();
	}


	/**
	 * Return a copy of the bit field of available pieces for this torrent.
	 *
	 * <p>
	 * Available pieces are pieces available in the swarm, and it does not
	 * include our own pieces.
	 * </p>
	 */
	public BitSet getAvailablePieces() {
		if (!this.isInitialized()) {
			throw new IllegalStateException("Torrent not yet initialized!");
		}

		int nPieces = this.pieces.getNumPieces();
		BitSet availablePieces = new BitSet(nPieces);

		synchronized (this.pieces) {
		    for (int i = 0; i < nPieces; ++i) {
				if (pieces.available(i)) {
					availablePieces.set(i);
				}
			}
		}

		return availablePieces;
	}

	/**
	 * Return a copy of the completed pieces bitset.
	 */
	public BitSet getCompletedPieces() {
		if (!this.isInitialized()) {
			throw new IllegalStateException("Torrent not yet initialized!");
		}

		synchronized (this.completedPieces) {
			return (BitSet)this.completedPieces.clone();
		}
	}

	/**
	 * Return a copy of the requested pieces bitset.
	 */
	public BitSet getRequestedPieces() {
		if (!this.isInitialized()) {
			throw new IllegalStateException("Torrent not yet initialized!");
		}

		synchronized (this.requestedPieces) {
			return (BitSet)this.requestedPieces.clone();
		}
	}

	/**
	 * Tells whether this torrent has been fully downloaded, or is fully
	 * available locally.
	 */
	public synchronized boolean isComplete() {
		return this.pieces.getNumPieces() > 0 &&
			this.completedPieces.cardinality() == this.pieces.getNumPieces();
	}

	/**
	 * Finalize the download of this torrent.
	 *
	 * <p>
	 * This realizes the final, pre-seeding phase actions on this torrent,
	 * which usually consists in putting the torrent data in their final form
	 * and at their target location.
	 * </p>
	 *
	 * @see TorrentByteStorage#finish
	 */
	public synchronized void finish() throws IOException {
		if (!this.isInitialized()) {
			throw new IllegalStateException("Torrent not yet initialized!");
		}

		if (!this.isComplete()) {
			throw new IllegalStateException("Torrent download is not complete!");
		}

		this.bucket.finish();
	}

	public synchronized boolean isFinished() {
		return this.isComplete() && this.bucket.isFinished();
	}

	/**
	 * Return the completion percentage of this torrent.
	 *
	 * <p>
	 * This is computed from the number of completed pieces divided by the
	 * number of pieces in this torrent, times 100.
	 * </p>
	 */
	public float getCompletion() {
		return this.isInitialized()
			? this.completedPieces.cardinality() /
				(float)this.pieces.getNumPieces() * 100.0f
			: 0.0f;
	}

	/**
	 * Mark a piece as completed, decrementing the piece size in bytes from our
	 * left bytes to download counter.
	 */
	public synchronized void markCompleted(int idx) {
		if (this.completedPieces.get(idx)) {
			return;
		}

		// A completed piece means that's that much data left to download for
		// this torrent.
		this.left -= pieces.size(idx);
		this.completedPieces.set(idx);
	}

	/** PeerActivityListener handler(s). *************************************/

	/**
	 * Peer choked handler.
	 *
	 * <p>
	 * When a peer chokes, the requests made to it are canceled and we need to
	 * mark the eventually piece we requested from it as available again for
	 * download tentative from another peer.
	 * </p>
	 *
	 * @param peer The peer that choked.
	 */
	@Override
	public synchronized void handlePeerChoked(SharingPeer peer) {
		int piece = peer.getRequestedPiece();

		if (piece != -1) {
			this.requestedPieces.set(piece, false);
		}

		logger.trace("Peer {} choked, we now have {} outstanding " +
				"request(s): {}",
			new Object[] {
				peer,
				this.requestedPieces.cardinality(),
				this.requestedPieces
		});
	}

	/**
	 * Peer ready handler.
	 *
	 * <p>
	 * When a peer becomes ready to accept piece block requests, select a piece
	 * to download and go for it.
	 * </p>
	 *
	 * @param peer The peer that became ready.
	 */
	@Override
	public synchronized void handlePeerReady(SharingPeer peer) {
		BitSet interesting = peer.getAvailablePieces();
		interesting.andNot(this.completedPieces);
		interesting.andNot(this.requestedPieces);

		logger.trace("Peer {} is ready and has {} interesting piece(s).",
			peer, interesting.cardinality());

		// If we didn't find interesting pieces, we need to check if we're in
		// an end-game situation. If yes, we request an already requested piece
		// to try to speed up the end.
		if (interesting.cardinality() == 0) {
			interesting = peer.getAvailablePieces();
			interesting.andNot(this.completedPieces);
			if (interesting.cardinality() == 0) {
				logger.trace("No interesting piece from {}!", peer);
				return;
			}

			if (this.completedPieces.cardinality() <
					ENG_GAME_COMPLETION_RATIO * this.pieces.getNumPieces()) {
				logger.trace("Not far along enough to warrant end-game mode.");
				return;
			}

			logger.trace("Possible end-game, we're about to request a piece " +
				"that was already requested from another peer.");
		}

		// Extract the RAREST_PIECE_JITTER rarest pieces from the interesting
		// pieces of this peer.
		ArrayList<Integer> choice = new ArrayList<Integer>(RAREST_PIECE_JITTER);
		synchronized (this.rarest) {
			for (int idx : this.rarest) {
				if (interesting.get(idx)) {
					choice.add(idx);
					if (choice.size() >= RAREST_PIECE_JITTER) {
						break;
					}
				}
			}
		}

		int chosen = choice.get(random.nextInt(Math.min(choice.size(),
				                                    RAREST_PIECE_JITTER)));
		this.requestedPieces.set(chosen);

		logger.trace("Requesting {} from {}, we now have {} " +
				"outstanding request(s): {}",
			new Object[] {
				chosen,
				peer,
				this.requestedPieces.cardinality(),
				this.requestedPieces
			});

		peer.downloadPiece(chosen);
	}

	/**
	 * Piece availability handler.
	 *
	 * <p>
	 * Handle updates in piece availability from a peer's HAVE message. When
	 * this happens, we need to mark that piece as available from the peer.
	 * </p>
	 *
	 * @param peer The peer we got the update from.
	 * @param idx The index of the piece that became available.
	 */
	@Override
	public synchronized void handlePieceAvailability(SharingPeer peer,
			                                         int idx) {
		// If we don't have this piece, tell the peer we're interested in
		// getting it from him.
		if (!this.completedPieces.get(idx) &&
			!this.requestedPieces.get(idx)) {
			peer.interesting();
		}

		this.rarest.remove(idx);
		pieces.seenAt(idx, peer);
		this.rarest.add(idx);

		logger.trace("Peer {} contributes {} piece(s) [{}/{}/{}].",
			new Object[] {
				peer,
				peer.getAvailablePieces().cardinality(),
				this.completedPieces.cardinality(),
				this.getAvailablePieces().cardinality(),
				this.pieces.getNumPieces()
			});

		if (!peer.isChoked() &&
			peer.isInteresting() &&
			!peer.isDownloading()) {
			this.handlePeerReady(peer);
		}
	}

	/**
	 * Bit field availability handler.
	 *
	 * <p>
	 * Handle updates in piece availability from a peer's BITFIELD message.
	 * When this happens, we need to mark in all the pieces the peer has that
	 * they can be reached through this peer, thus augmenting the global
	 * availability of pieces.
	 * </p>
	 *
	 * @param peer The peer we got the update from.
	 * @param availablePieces The pieces availability bit field of the peer.
	 */
	@Override
	public synchronized void handleBitfieldAvailability(SharingPeer peer,
			BitSet availablePieces) {
		// Determine if the peer is interesting for us or not, and notify it.
		BitSet interesting = (BitSet)availablePieces.clone();
		interesting.andNot(this.completedPieces);
		interesting.andNot(this.requestedPieces);

		if (interesting.cardinality() == 0) {
			peer.notInteresting();
		} else {
			peer.interesting();
		}

		// Record that the peer has all the pieces it told us it had.
		for (int i = availablePieces.nextSetBit(0); i >= 0;
				i = availablePieces.nextSetBit(i+1)) {
			this.rarest.remove(i);
			this.pieces.seenAt(i, peer);
			this.rarest.add(i);
		}

		logger.trace("Peer {} contributes {} piece(s) ({} interesting) " +
			"[completed={}; available={}/{}].",
			new Object[] {
				peer,
				availablePieces.cardinality(),
				interesting.cardinality(),
				this.completedPieces.cardinality(),
				this.getAvailablePieces().cardinality(),
				this.pieces.getNumPieces()
			});
	}

	/**
	 * Piece upload completion handler.
	 *
	 * <p>
	 * When a piece has been sent to a peer, we just record that we sent that
	 * many bytes. If the piece is valid on the peer's side, it will send us a
	 * HAVE message and we'll record that the piece is available on the peer at
	 * that moment (see <code>handlePieceAvailability()</code>).
	 * </p>
	 *
	 * @param peer The peer we got this piece from.
	 * @param idx the index of the piece sent
	 */
	@Override
	public synchronized void handlePieceSent(SharingPeer peer, int idx) {
		logger.trace("Completed upload of {} to {}.", idx, peer);
		this.uploaded += pieces.size(idx);
	}

	/**
	 * Piece download completion handler.
	 *
	 * <p>
	 * If the complete piece downloaded is valid, we can record in the torrent
	 * completedPieces bit field that we know have this piece.
	 * </p>
	 *
	 * @param peer The peer we got this piece from.
	 * @param idx The index of the piece in question.
	 */
	@Override
	public synchronized void handlePieceCompleted(SharingPeer peer,
	                                              int idx) throws IOException {
		// Regardless of validity, record the number of bytes downloaded and
		// mark the piece as not requested anymore
		this.downloaded += pieces.size(idx);
		this.requestedPieces.set(idx, false);

		logger.trace("We now have {} piece(s) and {} outstanding request(s): {}",
			new Object[] {
				this.completedPieces.cardinality(),
				this.requestedPieces.cardinality(),
				this.requestedPieces
			});
	}

	/**
	 * Peer disconnection handler.
	 *
	 * <p>
	 * When a peer disconnects, we need to mark in all of the pieces it had
	 * available that they can't be reached through this peer anymore.
	 * </p>
	 *
	 * @param peer The peer we got this piece from.
	 */
	@Override
	public synchronized void handlePeerDisconnected(SharingPeer peer) {
		BitSet availablePieces = peer.getAvailablePieces();

		for (int i = availablePieces.nextSetBit(0); i >= 0;
				i = availablePieces.nextSetBit(i+1)) {
			this.rarest.remove(i);
			this.pieces.noLongerAt(i, peer);
			this.rarest.add(i);
		}

		int requested = peer.getRequestedPiece();
		if (requested != -1) {
			this.requestedPieces.set(requested, false);
		}

		logger.debug("Peer {} went away with {} piece(s) [completed={}; available={}/{}]",
			new Object[] {
				peer,
				availablePieces.cardinality(),
				this.completedPieces.cardinality(),
				this.getAvailablePieces().cardinality(),
				this.pieces.getNumPieces()
			});
		logger.trace("We now have {} piece(s) and {} outstanding request(s): {}",
			new Object[] {
				this.completedPieces.cardinality(),
				this.requestedPieces.cardinality(),
				this.requestedPieces
			});
	}

	@Override
	public synchronized void handleIOException(SharingPeer peer,
			IOException ioe) { /* Do nothing */ }


	/**
	 * For accessing from Pieces.
	 * @return
	 */
	TorrentByteStorage getBucket() {
		return bucket;
	}

	/**
	 * For accessing from Pieces.
	 * @return
	 */
	int getPieceLength() {
		return pieceLength;
	}

	ByteBuffer getPiecesHashes() {
		return piecesHashes;
	}

	public Pieces getPieces() {
	    return pieces;
	}
}
