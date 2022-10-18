package org.github.forax.framework.interceptor;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class InterceptorRegistry {

  private final HashMap<Class<?>, List<Interceptor>> interceptorMap = new HashMap<>();

  private final HashMap<Method, Invocation> interceptorsCache = new HashMap<>();

  public void addAroundAdvice(Class<? extends Annotation> annotationClass, AroundAdvice aroundAdvice) {
    Objects.requireNonNull(annotationClass);
    Objects.requireNonNull(aroundAdvice);
    Interceptor interceptor = ((instance, method, args, invocation) -> {
      aroundAdvice.before(instance, method, args);
      Object result = null;
      try {
        result = invocation.proceed(instance, method, args);
      } finally {
        aroundAdvice.after(instance, method, args, result);
      }
      return result;
    });
    interceptorMap.computeIfAbsent(annotationClass, __ -> new ArrayList<>()).add(interceptor);
  }

  public <T> T createProxy(Class<T> type, T instance) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(instance);
    return type.cast(Proxy.newProxyInstance(type.getClassLoader(),
      new Class<?>[]{ type },
      (proxy, method, args) -> {
        var invocation = interceptorsCache.computeIfAbsent(method, m -> {
          var interceptors = findInterceptors(method);
          return getInvocation(interceptors);
        });
        return invocation.proceed(instance, method, args);
      }));
  }

  public void addInterceptor(Class <? extends Annotation> annotationClass, Interceptor interceptor) {
    Objects.requireNonNull(annotationClass);
    Objects.requireNonNull(interceptor);
    interceptorMap.computeIfAbsent(annotationClass, __ -> new ArrayList<>())
            .add(interceptor);
    interceptorsCache.clear();
  }

  List<Interceptor> findInterceptors(Method method) {
    return Stream.of(
            Arrays.stream(method.getDeclaringClass().getAnnotations()),
            Arrays.stream(method.getAnnotations()),
            Arrays.stream(method.getParameterAnnotations()).flatMap(Arrays::stream))
            .flatMap(s -> s)
            .distinct()
            .flatMap(annotation -> interceptorMap.getOrDefault(annotation.annotationType(),
                    List.of()).stream())
            .toList();
  }

  static Invocation getInvocation(List<Interceptor> interceptors) {
    Invocation invocation = Utils::invokeMethod;
    for (var interceptor : Utils.reverseList(interceptors)) {
      var oldInvocation = invocation;
      invocation = ((instance, method, args) -> interceptor.intercept(instance, method, args, oldInvocation));
    }
    return invocation;
  }
}
