package com.github.forax.framework.injector;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public final class InjectorRegistry {
    static <T> List<PropertyDescriptor> findInjectableProperties(Class<T> type) {
        Objects.requireNonNull(type);

        var descriptors = Utils.beanInfo(type);
        return Arrays.stream(descriptors.getPropertyDescriptors())
            .filter(property -> {
                var setter = property.getWriteMethod();
                return setter != null && setter.isAnnotationPresent(Inject.class);
            })
            .toList();
    }

    private final HashMap<Class<?>, Supplier<?>> registry = new HashMap<>();
    public <T> void registerInstance(Class<T> type, T instance) {
      Objects.requireNonNull(type);
      Objects.requireNonNull(instance);

      registerProvider(type, () -> instance);
    }

    public <T> void registerProvider(Class<T> type, Supplier<? extends T> supplier) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(supplier);

        var res = registry.putIfAbsent(type, supplier);
        if (res!= null) {
            throw new IllegalStateException("Only one instance of type " + type.getName() + " can be registered");
        }
    }

    public <T> T lookupInstance(Class<T> type) {
      Objects.requireNonNull(type);
      var instance = registry.get(type);
      if (instance == null) {
          throw new IllegalStateException("No instance registered for type " + type.getName());
      }
      return type.cast(instance.get());
    }

    public <T> void registerProviderClass(Class<T>type, Class<? extends T> providerClass) {
        Objects.requireNonNull(type);
        Objects.requireNonNull(providerClass);

        var constructor = findInjectableConstructor(providerClass);
        var constructorParameters = constructor.getParameterTypes();
        var annotatedProperty = findInjectableProperties(providerClass);

        Supplier<T> supplier = () -> {
            var args = Arrays.stream(constructorParameters)
                    .map(this::lookupInstance)
                    .toArray();
            var instance = Utils.newInstance(constructor, args);
            for (var property : annotatedProperty) {
                var setter = property.getWriteMethod();
                var arg = lookupInstance(setter.getParameterTypes()[0]);
                Utils.invokeMethod(instance, setter, arg);
            }
            return type.cast(instance);
        };

        registerProvider(type, supplier);
    }

    private <T> void registerProviderClassMirror(Class<T> type) {
        registerProviderClass(type, type);
    }

    public void registerProviderClass(Class<?> providerClass) {
        Objects.requireNonNull(providerClass);
        registerProviderClassMirror(providerClass);
    }

    private static Constructor<?> findInjectableConstructor(Class<?> type) {
        return Arrays.stream(type.getConstructors())
            .filter(constructor -> constructor.isAnnotationPresent(Inject.class))
            .reduce((c1, c2) -> {
                throw new IllegalStateException("Multiple constructors annotated " + c1 + " " + c2);
            })
            .orElseGet(() -> Utils.defaultConstructor(type));
    }
}
