package com.netflix.hystrix;

abstract public class TestHystrixSingleCommand<T> extends HystrixSingleCommand<T> implements AbstractTestHystrixCommand<T> {

  private final TestCommandBuilder builder;

  public TestHystrixSingleCommand(TestCommandBuilder builder) {
    super(builder.owner, builder.dependencyKey, builder.threadPoolKey, builder.circuitBreaker, builder.threadPool,
      builder.commandPropertiesDefaults, builder.threadPoolPropertiesDefaults, builder.metrics,builder.fallbackSemaphore,
      builder.executionSemaphore, TEST_PROPERTIES_FACTORY, builder.executionHook);
    this.builder = builder;
  }

  public TestCommandBuilder getBuilder() {
    return builder;
  }

  static TestCommandBuilder testPropsBuilder() {
    return new TestCommandBuilder(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE);
  }

  static TestCommandBuilder testPropsBuilder(HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker) {
    return new TestCommandBuilder(HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE)
      .setCircuitBreaker(circuitBreaker);
  }

  static TestCommandBuilder testPropsBuilder(HystrixCommandProperties.ExecutionIsolationStrategy isolationStrategy,
                                             HystrixCircuitBreakerTest.TestCircuitBreaker circuitBreaker) {
    return new TestCommandBuilder(isolationStrategy).setCircuitBreaker(circuitBreaker);
  }
}