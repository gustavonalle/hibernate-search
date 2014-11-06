/*
 * Hibernate Search, full-text search for your domain model
 *
 * License: GNU Lesser General Public License (LGPL), version 2.1 or later
 * See the lgpl.txt file in the root directory or <http://www.gnu.org/licenses/lgpl-2.1.html>.
 */
package org.hibernate.search.test.backend.lucene;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hibernate.search.backend.spi.WorkType.ADD;
import static org.hibernate.search.backend.spi.WorkType.DELETE;
import static org.hibernate.search.backend.spi.WorkType.UPDATE;

/**
 * Manages generation and storage of {@link org.hibernate.search.backend.spi.Work}
 *
 * @author gustavonalle
 */
public class WorkLog {

	private final Queue<Work> workLog = new ConcurrentLinkedQueue<>();
	private final RandomGenerator randomGenerator = RandomGenerator.withDefaults();

	private volatile AtomicInteger addCounter, deleteCounter, updateCounter;
	private final List<Integer> addedDocuments = new ArrayList<>();

	/**
	 * Constructor
	 * @param total total index works
	 * @param pctAdd % of ADD works (0-100)
	 * @param pctUpd % of UPDATE works (0-100)
	 */
	public WorkLog(int total, int pctAdd, int pctUpd) {
		if ( pctAdd + pctUpd > 100 ) {
			throw new IllegalArgumentException( "percentages should add up to 100" );
		}
		if ( pctAdd == 0 ) {
			throw new IllegalArgumentException( "Add percentage must be greater than 0" );
		}
		this.addCounter = new AtomicInteger( (total * pctAdd) / 100 );
		this.updateCounter = new AtomicInteger( (total * pctUpd) / 100 );
		this.deleteCounter = new AtomicInteger( total - addCounter.get() - updateCounter.get() );
		if ( addCounter.get() == 0 ) {
			throw new IllegalArgumentException( "At least one document should be added" );
		}
	}

	private synchronized Integer getPreviouslyAddedId() {
		if ( addedDocuments.isEmpty() ) {
			return null;
		}
		if ( addedDocuments.size() == 1 ) {
			return addedDocuments.get( 0 );
		}
		int idx = randomGenerator.randomIntNotZero( addedDocuments.size() ) - 1;
		return addedDocuments.get( idx );
	}

	private Work createAddWork() {
		Quote random = Quote.random();
		Work work = new Work( random, random.getId(), ADD );
		addCounter.decrementAndGet();
		return work;
	}

	private Work createUpdateWork(int id) {
		Work work = new Work( Quote.random( id ), id, UPDATE );
		updateCounter.decrementAndGet();
		return work;
	}

	private Work createDeleteWork(int id) {
		Work work = new Work( Quote.random( id ), id, DELETE );
		deleteCounter.decrementAndGet();
		return work;
	}

	/**
	 * Generate new Random work respecting the configured percentages
	 * @return Random work involving {@link org.hibernate.search.test.backend.lucene.Quote}
	 * or null if called more than 'total' times
	 */
	public Work generateNewWork() {
		if ( addCounter.get() == 0 && deleteCounter.get() == 0 && updateCounter.get() == 0 ) {
			return null;
		}
		Work work = null;
		while ( work == null ) {
			WorkType workType = randomGenerator.oneOf( ADD, DELETE, UPDATE );
			if ( workType.equals( ADD ) ) {
				if ( addCounter.get() > 0 ) {
					work = createAddWork();
				}
			}
			else {
				Integer previouslyAddedId = getPreviouslyAddedId();
				if ( workType.equals( DELETE ) ) {
					if ( deleteCounter.get() > 0 && previouslyAddedId != null ) {
						work = createDeleteWork( previouslyAddedId );
					}
				}
				if ( workType.equals( UPDATE ) ) {
					if ( updateCounter.get() > 0 && previouslyAddedId != null ) {
						work = createUpdateWork( previouslyAddedId );
					}

				}
			}
		}
		return work;

	}

	public void workApplied(Work work) {
		workLog.add( work );
		if ( work.getType().equals( ADD ) ) {
			synchronized ( this ) {
				addedDocuments.add( (Integer) work.getId() );
			}
		}
	}

	/**
	 * test the work generation
	 */
	public static void main(String[] args) {
		WorkLog workLog = new WorkLog( 15, 80, 20 );

		Work workUnit = workLog.generateNewWork();
		System.out.println( workUnit );
		while ( workUnit != null ) {
			workUnit = workLog.generateNewWork();
			System.out.println( workUnit );
			if ( workUnit != null ) {
				workLog.workApplied( workUnit );
			}
		}
		System.out.println( workLog.calculateIndexSize() );
	}

	/**
	 * Calculates the expect number of documents in the index by replaying
	 * the work log, taking into account deletes, add and updates
	 *
	 * @return index size
	 */
	public int calculateIndexSize() {
		Set<Serializable> added = new HashSet<>();
		for ( Work work : workLog ) {
			if ( work.getType().equals( DELETE ) ) {
				added.remove( work.getId() );
			}
			else {
				added.add( work.getId() );
			}
		}
		return added.size();
	}
}
