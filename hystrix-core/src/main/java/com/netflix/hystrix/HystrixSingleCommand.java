package com.netflix.hystrix;

import com.netflix.hystrix.strategy.executionhook.HystrixCommandExecutionHook;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Single;

public abstract class HystrixSingleCommand<R> extends AbstractCommand<R> implements HystrixSingle<R>, HystrixInvokableInfo<R> {
  private static final Logger logger = LoggerFactory.getLogger(HystrixSingleCommand.class);

  protected HystrixSingleCommand(HystrixCommandGroupKey group) {
    this(new Setter(group));
  }

  @Override
  protected boolean shouldOutputOnSuccessEvents() {
    return true;
  }

  @Override
  protected String getFallbackMethodName() {
    return "resumeWithFallback";
  }

  @Override
  protected boolean isFallbackUserDefined() {
    Boolean containsFromMap = commandContainsFallback.get(commandKey);
    if (containsFromMap != null) {
      return containsFromMap;
    } else {
      Boolean toInsertIntoMap;
      try {
        getClass().getDeclaredMethod("resumeWithFallback");
        toInsertIntoMap = true;
      } catch (NoSuchMethodException nsme) {
        toInsertIntoMap = false;
      }
      commandContainsFallback.put(commandKey, toInsertIntoMap);
      return toInsertIntoMap;
    }
  }

  @Override
  protected boolean commandIsScalar() {
    return false;
  }

  protected HystrixSingleCommand(Setter setter) {
    this(setter.groupKey, setter.commandKey, setter.threadPoolKey, null, null, setter.commandPropertiesDefaults, setter.threadPoolPropertiesDefaults,
      null, null, null, null, null);
  }

  HystrixSingleCommand(HystrixCommandGroupKey group, HystrixCommandKey key, HystrixThreadPoolKey threadPoolKey, HystrixCircuitBreaker circuitBreaker, HystrixThreadPool threadPool,
                       HystrixCommandProperties.Setter commandPropertiesDefaults, HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults,
                       HystrixCommandMetrics metrics, TryableSemaphore fallbackSemaphore, TryableSemaphore executionSemaphore,
                       HystrixPropertiesStrategy propertiesStrategy, HystrixCommandExecutionHook executionHook) {
    super(group, key, threadPoolKey, circuitBreaker, threadPool, commandPropertiesDefaults, threadPoolPropertiesDefaults, metrics, fallbackSemaphore, executionSemaphore,
      propertiesStrategy, executionHook);
  }

  final public static class Setter {

    protected final HystrixCommandGroupKey groupKey;
    protected HystrixCommandKey commandKey;
    protected HystrixThreadPoolKey threadPoolKey;
    protected HystrixCommandProperties.Setter commandPropertiesDefaults;
    protected HystrixThreadPoolProperties.Setter threadPoolPropertiesDefaults;

    protected Setter(HystrixCommandGroupKey groupKey) {
      this.groupKey = groupKey;

      commandPropertiesDefaults = setDefaults(HystrixCommandProperties.Setter());
    }

    public static Setter withGroupKey(HystrixCommandGroupKey groupKey) {
      return new Setter(groupKey);
    }

    public Setter andCommandKey(HystrixCommandKey commandKey) {
      this.commandKey = commandKey;
      return this;
    }

    public Setter andCommandPropertiesDefaults(HystrixCommandProperties.Setter commandPropertiesDefaults) {
      this.commandPropertiesDefaults = setDefaults(commandPropertiesDefaults);
      return this;
    }

    private HystrixCommandProperties.Setter setDefaults(HystrixCommandProperties.Setter commandPropertiesDefaults) {
      if (commandPropertiesDefaults.getExecutionIsolationStrategy() == null) {
        commandPropertiesDefaults.withExecutionIsolationStrategy(
          HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE);
      }
      return commandPropertiesDefaults;
    }
  }

  protected abstract Single<R> construct();

  protected Single<R> resumeWithFallback() {
    return Single.error(new UnsupportedOperationException("No fallback available."));
  }

  @Override
  final protected Single<R> getExecutionSingle() {
    return construct();
  }

  @Override
  final protected Single<R> getFallbackSingle() {
    return resumeWithFallback();
  }
}