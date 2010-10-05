"""celery.backends.amqp"""
import socket
import time
import warnings

from datetime import timedelta

from carrot.messaging import Consumer, ConsumerSet, Publisher

from celery import conf
from celery import states
from celery.backends.base import BaseDictBackend
from celery.exceptions import TimeoutError
from celery.messaging import establish_connection
from celery.utils import timeutils


class AMQResultWarning(UserWarning):
    pass


class ResultPublisher(Publisher):
    exchange = conf.RESULT_EXCHANGE
    exchange_type = conf.RESULT_EXCHANGE_TYPE
    delivery_mode = conf.RESULT_PERSISTENT and 2 or 1
    serializer = conf.RESULT_SERIALIZER
    durable = conf.RESULT_PERSISTENT
    auto_delete = True

    def __init__(self, connection, task_id, **kwargs):
        super(ResultPublisher, self).__init__(connection,
                        routing_key=task_id,
                        **kwargs)


class ResultConsumer(Consumer):
    exchange = conf.RESULT_EXCHANGE
    exchange_type = conf.RESULT_EXCHANGE_TYPE
    durable = conf.RESULT_PERSISTENT
    no_ack = True
    auto_delete = True

    def __init__(self, connection, task_id, **kwargs):
        super(ResultConsumer, self).__init__(connection,
                queue=task_id, routing_key=task_id, **kwargs)


class AMQPBackend(BaseDictBackend):
    """AMQP backend. Publish results by sending messages to the broker
    using the task id as routing key.

    **NOTE:** Results published using this backend is read-once only.
    After the result has been read, the result is deleted. (however, it's
    still cached locally by the backend instance).

    """

    _connection = None

    def __init__(self, connection=None, exchange=None, exchange_type=None,
            persistent=None, serializer=None, auto_delete=None,
            expires=None, **kwargs):
        self._connection = connection
        self.queue_arguments = {}
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.persistent = persistent
        self.serializer = serializer
        self.auto_delete = auto_delete
        self.expires = expires
        if self.expires is None:
            self.expires = conf.AMQP_TASK_RESULT_EXPIRES
        if isinstance(self.expires, timedelta):
            self.expires = timeutils.timedelta_seconds(self.expires)
        if self.expires is not None:
            self.expires = int(self.expires)
            # WARNING: Requries RabbitMQ 2.1.0 or higher.
            # x-expires must be a signed-int, or long describing the
            # expiry time in milliseconds.
            self.queue_arguments["x-expires"] = int(self.expires * 1000.0)
        super(AMQPBackend, self).__init__(**kwargs)

    def _create_publisher(self, task_id, connection):
        delivery_mode = self.persistent and 2 or 1

        # Declares the queue.
        self._create_consumer(task_id, connection).close()

        return ResultPublisher(connection, task_id,
                               exchange=self.exchange,
                               exchange_type=self.exchange_type,
                               delivery_mode=delivery_mode,
                               serializer=self.serializer)

    def _create_consumer(self, task_id, connection):
        return ResultConsumer(connection, task_id,
                              exchange=self.exchange,
                              exchange_type=self.exchange_type,
                              durable=self.persistent,
                              queue_arguments=self.queue_arguments)

    def store_result(self, task_id, result, status, traceback=None,
            max_retries=20, retry_delay=0.2):
        """Send task return value and status."""
        result = self.encode_result(result, status)

        meta = {"task_id": task_id,
                "result": result,
                "status": status,
                "traceback": traceback}

        for i in range(max_retries + 1):
            try:
                publisher = self._create_publisher(task_id, self.connection)
                publisher.send(meta)
                publisher.close()
            except Exception, exc:
                if not max_retries:
                    raise
                self._connection = None
                warnings.warn(AMQResultWarning(
                    "Error sending result %s: %r" % (task_id, exc)))
                time.sleep(retry_delay)
            break

        return result

    def get_task_meta(self, task_id, cache=True):
        return self.poll(task_id)

    def wait_for(self, task_id, timeout=None, cache=True):
        cached_meta = self._cache.get(task_id)

        if cached_meta and cached_meta["status"] in states.READY_STATES:
            meta = cached_meta
        else:
            try:
                meta = self.consume(task_id, timeout=timeout)
            except socket.timeout:
                raise TimeoutError("The operation timed out.")

        if meta["status"] == states.SUCCESS:
            return meta["result"]
        elif meta["status"] in states.PROPAGATE_STATES:
            raise self.exception_to_python(meta["result"])
        else:
            return self.wait_for(task_id, timeout, cache)

    def poll(self, task_id):
        consumer = self._create_consumer(task_id, self.connection)
        result = consumer.fetch()
        try:
            if result:
                payload = self._cache[task_id] = result.payload
                return payload
            else:

                # Use previously received status if any.
                if task_id in self._cache:
                    return self._cache[task_id]

                return {"status": states.PENDING, "result": None}
        finally:
            consumer.close()

    def drain_events(self, consumer, timeout=None):
        wait = self.connection.drain_events
        results = {}

        def callback(meta, message):
            if meta["status"] in states.READY_STATES:
                results[message.delivery_info["routing_key"]] = meta

        consumer.register_callback(callback)

        time_start = time.time()
        while True:
            # Total time spent may exceed a single call to wait()
            if timeout and time.time() - time_start >= timeout:
                raise socket.timeout()
            wait(timeout=timeout)
            if results:
                # Got event on the wanted channel.
                break

        for task_id, meta in results.items():
            self._cache[task_id] = meta

        return results

    def consume(self, task_id, timeout=None):
        consumer = self._create_consumer(task_id, self.connection)
        consumer.consume()
        try:
            return self.drain_events(consumer, timeout=timeout).values()[0]
        finally:
            consumer.close()

    def get_many(self, task_ids, timeout=None):
        consumers = [self._create_consumer(task_id, self.connection)
                            for task_id in task_ids]
        cset = ConsumerSet(self.connection, consumers=consumers)
        cset.consume()
        ids = set(task_ids)
        results = {}
        try:
            while ids:
                r = self.drain_events(cset, timeout=timeout)
                ids = ids ^ set(r.keys())
                results.update(r)
        finally:
            cset.close()

        return results

    def close(self):
        if self._connection is not None:
            self._connection.close()

    @property
    def connection(self):
        if not self._connection:
            self._connection = establish_connection()
        return self._connection

    def reload_task_result(self, task_id):
        raise NotImplementedError(
                "reload_task_result is not supported by this backend.")

    def reload_taskset_result(self, task_id):
        """Reload taskset result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_taskset_result is not supported by this backend.")

    def save_taskset(self, taskset_id, result):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "save_taskset is not supported by this backend.")

    def restore_taskset(self, taskset_id, cache=True):
        """Get the result of a taskset."""
        raise NotImplementedError(
                "restore_taskset is not supported by this backend.")
