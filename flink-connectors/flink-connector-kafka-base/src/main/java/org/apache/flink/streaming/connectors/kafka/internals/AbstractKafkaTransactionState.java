package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import org.apache.kafka.clients.producer.Producer;

import javax.annotation.Nullable;

/**
 * State for handling transactions.
 */
@VisibleForTesting
@Internal
public abstract class AbstractKafkaTransactionState {

	@Nullable
	private final	String transactionalId;

	private final long producerId;

	private final short epoch;

	public AbstractKafkaTransactionState(
			@Nullable String transactionalId,
			long producerId,
			short epoch) {
		this.transactionalId = transactionalId;
		this.producerId = producerId;
		this.epoch = epoch;
	}

	public boolean isTransactional() {
		return transactionalId != null;
	}

	@Nullable
	public String getTransactionalId() {
		return transactionalId;
	}

	public long getProducerId() {
		return producerId;
	}

	public short getEpoch() {
		return epoch;
	}

	public abstract Producer<byte[], byte[]> getProducer();

	@Override
	public String toString() {
		return String.format(
				"%s [transactionalId=%s, producerId=%s, epoch=%s]",
				this.getClass().getSimpleName(),
				transactionalId,
				producerId,
				epoch);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		AbstractKafkaTransactionState that = (AbstractKafkaTransactionState) o;

		if (producerId != that.producerId) {
			return false;
		}
		if (epoch != that.epoch) {
			return false;
		}
		return transactionalId != null ? transactionalId.equals(that.transactionalId) : that.transactionalId == null;
	}

	@Override
	public int hashCode() {
		int result = transactionalId != null ? transactionalId.hashCode() : 0;
		result = 31 * result + (int) (producerId ^ (producerId >>> 32));
		result = 31 * result + (int) epoch;
		return result;
	}
}
