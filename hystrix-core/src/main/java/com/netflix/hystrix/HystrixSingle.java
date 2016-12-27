package com.netflix.hystrix;

import rx.Single;

public interface HystrixSingle<R> extends HystrixInvokable<R> {

  public Single<R> single();

  public Single<R> toSingle();
}