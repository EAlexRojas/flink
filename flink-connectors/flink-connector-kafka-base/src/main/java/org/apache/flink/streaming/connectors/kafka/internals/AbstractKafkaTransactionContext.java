package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Context associated to this instance of the FlinkKafkaProducer. User for keeping track of the
 * transactionalIds.
 */
@VisibleForTesting
@Internal
public abstract class AbstractKafkaTransactionContext {

	private final Set<String> transactionalIds;

	public AbstractKafkaTransactionContext(Set<String> transactionalIds) {
		checkNotNull(transactionalIds);
		this.transactionalIds = transactionalIds;
	}

	public Set<String> getTransactionalIds() {
		return transactionalIds;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		AbstractKafkaTransactionContext that = (AbstractKafkaTransactionContext) o;

		return transactionalIds.equals(that.transactionalIds);
	}

	@Override
	public int hashCode() {
		return transactionalIds.hashCode();
	}
}

