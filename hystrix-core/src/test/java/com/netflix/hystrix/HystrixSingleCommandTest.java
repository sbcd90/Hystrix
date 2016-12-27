package com.netflix.hystrix;

import com.hystrix.junit.HystrixRequestContextRule;
import com.netflix.config.ConfigurationManager;
import com.netflix.hystrix.HystrixCommandProperties.ExecutionIsolationStrategy;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import rx.Single;
import rx.SingleSubscriber;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.schedulers.Schedulers;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class HystrixSingleCommandTest extends CommonHystrixCommandTests<TestHystrixSingleCommand<Integer>> {

    @Rule
    public HystrixRequestContextRule ctx = new HystrixRequestContextRule();

    @After
    public void cleanup() {
        // force properties to be clean as well
        ConfigurationManager.getConfigInstance().clear();

        /*
         * RxJava will create one worker for each processor when we schedule Singles in the
         * Schedulers.computation(). Any leftovers here might lead to a congestion in a following
         * thread. To ensure all existing threads have completed we now schedule some singles
         * that will execute in distinct threads due to the latch..
         */
        int count = Runtime.getRuntime().availableProcessors();
        final CountDownLatch latch = new CountDownLatch(count);
        ArrayList<Future<Boolean>> futures = new ArrayList<Future<Boolean>>();
        for (int i = 0;i < count; ++i) {
            futures.add(Single.create(new Single.OnSubscribe<Boolean>() {
                @Override
                public void call(SingleSubscriber<? super Boolean> singleSubscriber) {
                    latch.countDown();
                    try {
                        latch.await();

                        singleSubscriber.onSuccess(true);
                    } catch (InterruptedException e) {
                        singleSubscriber.onError(e);
                    }
                }
            }).subscribeOn(Schedulers.computation()).toBlocking().toFuture());
        }
        for (Future<Boolean> future: futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }

        //TODO commented out as it has issues when built from command-line even though it works from IDE
        //        HystrixCommandKey key = Hystrix.getCurrentThreadExecutingCommand();
        //        if (key != null) {
        //            throw new IllegalStateException("should be null but got: " + key);
        //        }
    }

    /**
     * Test a successful semaphore-isolated command execution.
     */
    @Test
    public void testSemaphoreSingleSuccess() {
        testSingleSuccess(ExecutionIsolationStrategy.SEMAPHORE);
    }

    /**
     * Test a successful thread-isolated command execution.
     */
    @Test
    public void testThreadSingleSuccess() {
        testSingleSuccess(ExecutionIsolationStrategy.THREAD);
    }

    private void testSingleSuccess(ExecutionIsolationStrategy isolationStrategy) {
        try {
            TestHystrixSingleCommand<Boolean> command = new SuccessfulTestCommand(isolationStrategy);
            assertEquals(true, command.single().toBlocking().value());

            assertEquals(null, command.getFailedExecutionException());

            assertTrue(command.getExecutionTimeInMilliseconds() > -1);
            assertTrue(command.isSuccessfulExecution());
            assertFalse(command.isResponseFromFallback());
            assertCommandExecutionEvents(command, HystrixEventType.EMIT, HystrixEventType.SUCCESS);
            assertEquals(0, command.metrics.getCurrentConcurrentExecutionCount());
            assertSaneHystrixRequestLog(1);
            assertNull(command.getExecutionException());
            assertEquals(isolationStrategy.equals(ExecutionIsolationStrategy.THREAD), command.isExecutedInThread());
        } catch (Exception e) {
            e.printStackTrace();
            fail("We received an exception.");
        }
    }

    @Override
    void assertHooksOnSuccess(Func0<TestHystrixSingleCommand<Integer>> ctor, Action1<TestHystrixSingleCommand<Integer>> assertion) {
        assertBlockingSingle(ctor.call(), assertion, true);
        assertNonBlockingSingle(ctor.call(), assertion, true);
    }

    @Override
    void assertHooksOnFailure(Func0<TestHystrixSingleCommand<Integer>> ctor, Action1<TestHystrixSingleCommand<Integer>> assertion) {
        assertBlockingSingle(ctor.call(), assertion, false);
        assertNonBlockingSingle(ctor.call(), assertion, false);
    }

    @Override
    void assertHooksOnFailure(Func0<TestHystrixSingleCommand<Integer>> ctor, Action1<TestHystrixSingleCommand<Integer>> assertion, boolean failFast) {

    }

    /* ******************************************************************************** */
    /* ******************************************************************************** */
    /* private HystrixCommand class implementations for unit testing */
    /* ******************************************************************************** */
    /* ******************************************************************************** */

    static AtomicInteger uniqueNameCounter = new AtomicInteger(0);

    @Override
    TestHystrixSingleCommand<Integer> getCommand(HystrixCommandProperties.ExecutionIsolationStrategy isolationStrategy, AbstractTestHystrixCommand.ExecutionResult executionResult, int executionLatency, AbstractTestHystrixCommand.FallbackResult fallbackResult, int fallbackLatency, HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker, HystrixThreadPool threadPool, int timeout, AbstractTestHystrixCommand.CacheEnabled cacheEnabled, Object value, AbstractCommand.TryableSemaphore executionSemaphore, AbstractCommand.TryableSemaphore fallbackSemaphore, boolean circuitBreakerDisabled) {
        HystrixCommandKey commandKey = HystrixCommandKey.Factory.asKey("FlexibleObservable-" + uniqueNameCounter.getAndIncrement());
        return FlexibleTestHystrixSingleCommand.from(commandKey, isolationStrategy, executionResult, executionLatency, fallbackResult, fallbackLatency, circuitBreaker, threadPool, timeout, cacheEnabled, value, executionSemaphore, fallbackSemaphore, circuitBreakerDisabled);
    }

    @Override
    TestHystrixSingleCommand<Integer> getCommand(HystrixCommandKey commandKey, HystrixCommandProperties.ExecutionIsolationStrategy isolationStrategy, AbstractTestHystrixCommand.ExecutionResult executionResult, int executionLatency, AbstractTestHystrixCommand.FallbackResult fallbackResult, int fallbackLatency, HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker, HystrixThreadPool threadPool, int timeout, AbstractTestHystrixCommand.CacheEnabled cacheEnabled, Object value, AbstractCommand.TryableSemaphore executionSemaphore, AbstractCommand.TryableSemaphore fallbackSemaphore, boolean circuitBreakerDisabled) {
        return FlexibleTestHystrixSingleCommand.from(commandKey, isolationStrategy, executionResult, executionLatency, fallbackResult, fallbackLatency, circuitBreaker, threadPool, timeout, cacheEnabled, value, executionSemaphore, fallbackSemaphore, circuitBreakerDisabled);
    }

    private static class FlexibleTestHystrixSingleCommand {
        public static Integer EXECUTE_VALUE = 1;
        public static Integer FALLBACK_VALUE = 11;

        public static AbstractFlexibleTestHystrixSingleCommand from(HystrixCommandKey commandKey, ExecutionIsolationStrategy isolationStrategy, AbstractTestHystrixCommand.ExecutionResult executionResult, int executionLatency, AbstractTestHystrixCommand.FallbackResult fallbackResult, int fallbackLatency, HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker, HystrixThreadPool threadPool,
                                                                    int timeout, AbstractTestHystrixCommand.CacheEnabled cacheEnabled, Object value, AbstractCommand.TryableSemaphore executionSemaphore,
                                                                    AbstractCommand.TryableSemaphore fallbackSemaphore, boolean circuitBreakerDisabled) {
            if (fallbackResult.equals(AbstractTestHystrixCommand.FallbackResult.UNIMPLEMENTED)) {
                return new FlexibleTestHystrixSingleCommandNoFallback(commandKey, isolationStrategy, executionResult, executionLatency, circuitBreaker, threadPool, timeout, cacheEnabled, value, executionSemaphore, fallbackSemaphore, circuitBreakerDisabled);
            } else {
                return new FlexibleTestHystrixSingleCommandWithFallback(commandKey, isolationStrategy, executionResult, executionLatency, fallbackResult, fallbackLatency, circuitBreaker, threadPool, timeout, cacheEnabled, value, executionSemaphore, fallbackSemaphore, circuitBreakerDisabled);
            }
        }
    }

    private static class AbstractFlexibleTestHystrixSingleCommand extends TestHystrixSingleCommand<Integer> {
        private final AbstractTestHystrixCommand.ExecutionResult executionResult;
        private final int executionLatency;
        private final CacheEnabled cacheEnabled;
        private final Object value;

        public AbstractFlexibleTestHystrixSingleCommand(HystrixCommandKey commandKey, ExecutionIsolationStrategy isolationStrategy, AbstractTestHystrixCommand.ExecutionResult executionResult, int executionLatency,
                                                        HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker, HystrixThreadPool threadPool, int timeout, CacheEnabled cacheEnabled, Object value, TryableSemaphore executionSemaphore,
                                                        TryableSemaphore fallbackSemaphore, boolean circuitBreakerDisabled) {
            super(testPropsBuilder(circuitBreaker)
            .setCommandKey(commandKey)
            .setCircuitBreaker(circuitBreaker)
            .setMetrics(circuitBreaker.metrics)
            .setThreadPool(threadPool)
            .setCommandPropertiesDefaults(HystrixCommandPropertiesTest.getUnitTestPropertiesSetter()
            .withExecutionIsolationStrategy(isolationStrategy)
            .withExecutionTimeoutInMilliseconds(timeout)
            .withCircuitBreakerEnabled(!circuitBreakerDisabled))
            .setExecutionSemaphore(executionSemaphore)
            .setFallbackSemaphore(fallbackSemaphore));
            this.executionResult = executionResult;
            this.executionLatency = executionLatency;
            this.cacheEnabled = cacheEnabled;
            this.value = value;
        }

        @Override
        protected Single<Integer> construct() {
            if (executionResult == ExecutionResult.FAILURE) {
                addLatency(executionLatency);
                throw new RuntimeException("Execution Sync Failure for TestHystrixSingleCommand");
            } else if (executionResult == ExecutionResult.HYSTRIX_FAILURE) {
                addLatency(executionLatency);
                throw new HystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION, HystrixSingleCommandTest.AbstractFlexibleTestHystrixSingleCommand.class, "Execution Hystrix Failure for TestHystrixSingleCommand", new RuntimeException("Execution Failure for TestHystrixSingleCommand"), new RuntimeException("Fallback Failure for TestHystrixSingleCommand"));
            } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.RECOVERABLE_ERROR) {
                addLatency(executionLatency);
                throw new java.lang.Error("Execution Sync Error for TestHystrixSingleCommand");
            } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.UNRECOVERABLE_ERROR) {
                addLatency(executionLatency);
                throw new OutOfMemoryError("Execution Sync OOME for TestHystrixSingleCommand");
            } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.BAD_REQUEST) {
                addLatency(executionLatency);
                throw new HystrixBadRequestException("Execution Bad Request Exception for TestHystrixSingleCommand");
            }

            return Single.create(new Single.OnSubscribe<Integer>() {
                @Override
                public void call(SingleSubscriber<? super Integer> singleSubscriber) {
                    System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " construct() method has been subscribed to");
                    addLatency(executionLatency);
                    if (executionResult == AbstractTestHystrixCommand.ExecutionResult.SUCCESS) {
                        singleSubscriber.onSuccess(1);
                    } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.ASYNC_FAILURE) {
                        singleSubscriber.onError(new RuntimeException("Execution Async Failure for TestHystrixSingleCommand after 0 emits"));
                    } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.ASYNC_HYSTRIX_FAILURE) {
                        singleSubscriber.onError(new HystrixRuntimeException(HystrixRuntimeException.FailureType.COMMAND_EXCEPTION, HystrixSingleCommandTest.AbstractFlexibleTestHystrixSingleCommand.class, "Execution Hystrix Failure for TestHystrixSingleCommand", new RuntimeException("Execution Failure for TestHystrixSingleCommand"), new RuntimeException("Fallback Failure for TestHystrixSingleCommand")));
                    } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.ASYNC_RECOVERABLE_ERROR) {
                        singleSubscriber.onError(new java.lang.Error("Execution Async Error for TestHystrixSingleCommand"));
                    } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.ASYNC_UNRECOVERABLE_ERROR) {
                        singleSubscriber.onError(new OutOfMemoryError("Execution Async OOME for TestHystrixSingleCommand"));
                    } else if (executionResult == AbstractTestHystrixCommand.ExecutionResult.ASYNC_BAD_REQUEST) {
                        singleSubscriber.onError(new HystrixBadRequestException("Execution Async Bad Request Exception for TestHystrixSingleCommand"));
                    } else {
                        singleSubscriber.onError(new RuntimeException("You passed in a executionResult enum that can't be represented in HystrixObservableCommand: " + executionResult));
                    }
                }
            });
        }

        @Override
        protected String getCacheKey() {
            if (cacheEnabled == CacheEnabled.YES)
                return value.toString();
            else
                return null;
        }

        protected void addLatency(int latency) {
            if (latency > 0) {
                try {
                    System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " About to sleep for : " + latency);
                    Thread.sleep(latency);
                    System.out.println(System.currentTimeMillis() + " : " + Thread.currentThread().getName() + " Woke up from sleep!");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    // ignore and sleep some more to simulate a dependency that doesn't obey interrupts
                    try {
                        Thread.sleep(latency);
                    } catch (Exception e2) {
                        // ignore
                    }
                    System.out.println("after interruption with extra sleep");
                }
            }
        }
    }

    private static class FlexibleTestHystrixSingleCommandWithFallback extends HystrixSingleCommandTest.AbstractFlexibleTestHystrixSingleCommand {
        private final AbstractTestHystrixCommand.FallbackResult fallbackResult;
        private final int fallbackLatency;

        public FlexibleTestHystrixSingleCommandWithFallback(HystrixCommandKey commandKey, ExecutionIsolationStrategy isolationStrategy, AbstractTestHystrixCommand.ExecutionResult executionResult, int executionLatency, FallbackResult fallbackResult, int fallbackLatency, HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker, HystrixThreadPool threadPool, int timeout, CacheEnabled cacheEnabled, Object value, TryableSemaphore executionSemaphore, TryableSemaphore fallbackSemaphore, boolean circuitBreakerDisabled) {
            super(commandKey, isolationStrategy, executionResult, executionLatency, circuitBreaker, threadPool, timeout, cacheEnabled, value, executionSemaphore, fallbackSemaphore, circuitBreakerDisabled);
            this.fallbackResult = fallbackResult;
            this.fallbackLatency = fallbackLatency;
        }

        @Override
        protected Single<Integer> resumeWithFallback() {
            if (fallbackResult == AbstractTestHystrixCommand.FallbackResult.FAILURE) {
                addLatency(fallbackLatency);
                throw new RuntimeException("Fallback Sync Failure for TestHystrixCommand");
            } else if (fallbackResult == FallbackResult.UNIMPLEMENTED) {
                addLatency(fallbackLatency);
                return super.resumeWithFallback();
            }
            return Single.create(new Single.OnSubscribe<Integer>() {
                @Override
                public void call(SingleSubscriber<? super Integer> singleSubscriber) {
                    addLatency(fallbackLatency);
                    if (fallbackResult == AbstractTestHystrixCommand.FallbackResult.SUCCESS) {
                        singleSubscriber.onSuccess(11);
                    } else if (fallbackResult == AbstractTestHystrixCommand.FallbackResult.ASYNC_FAILURE) {
                        singleSubscriber.onError(new RuntimeException("Fallback Async Failure for TestHystrixCommand after 0 fallback emits"));
                    } else {
                        singleSubscriber.onError(new RuntimeException("You passed in a fallbackResult enum that can't be represented in HystrixSingleCommand: " + fallbackResult));
                    }
                }
            });
        }
    }

    private static class FlexibleTestHystrixSingleCommandNoFallback extends HystrixSingleCommandTest.AbstractFlexibleTestHystrixSingleCommand {
        public FlexibleTestHystrixSingleCommandNoFallback(HystrixCommandKey commandKey, ExecutionIsolationStrategy isolationStrategy, AbstractTestHystrixCommand.ExecutionResult executionResult, int executionLatency, HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker, HystrixThreadPool threadPool, int timeout, CacheEnabled cacheEnabled, Object value, TryableSemaphore executionSemaphore, TryableSemaphore fallbackSemaphore, boolean circuitBreakerDisabled) {
            super(commandKey, isolationStrategy, executionResult, executionLatency, circuitBreaker, threadPool, timeout, cacheEnabled, value, executionSemaphore, fallbackSemaphore, circuitBreakerDisabled);
        }
    }

    /**
     * Successful execution - no fallback implementation.
     */
    private static class SuccessfulTestCommand extends TestHystrixSingleCommand<Boolean> {

        public SuccessfulTestCommand(ExecutionIsolationStrategy isolationStrategy) {
            this(HystrixCommandPropertiesTest.getUnitTestPropertiesSetter().withExecutionIsolationStrategy(isolationStrategy));
        }

        public SuccessfulTestCommand(HystrixCommandProperties.Setter properties) {
            super(testPropsBuilder().setCommandPropertiesDefaults(properties));
        }

        @Override
        protected Single<Boolean> construct() {
            return Single.just(true).subscribeOn(Schedulers.computation());
        }

    }
}