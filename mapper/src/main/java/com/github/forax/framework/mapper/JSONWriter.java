package com.github.forax.framework.mapper;

import java.beans.Beans;
import java.beans.PropertyDescriptor;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.RECORD_COMPONENT;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.stream.Collectors.joining;

public final class JSONWriter {

  private interface Generator {
    String generate(JSONWriter writer, Object bean);
  }

  private static final ClassValue<List<Generator>> PROPERTIES_CLASS_VALUE = new ClassValue<>() {
    @Override
    protected List<Generator> computeValue(Class<?> type) {
      if (type.isRecord()) {
        return computeRecord(type);
      }
      return computeBean(type);
    }

    private List<Generator> computeRecord(Class<?> record) {
      var components = record.getRecordComponents();
      return Arrays.stream(components)
          .filter(component -> !component.getClass().equals("class"))
          .<Generator>map(component -> {
            var getter = component.getAccessor();
            var key = computeKeyName(getter, component.getName());
            return (writer, instance) ->  key + writer.toJSON(Utils.invokeMethod(instance, getter));
          })
          .toList();
    }

    private List<Generator> computeBean(Class<?> bean) {
      var beanInfo = Utils.beanInfo(bean);
      var properties = beanInfo.getPropertyDescriptors();
      return Arrays.stream(properties)
          .filter(property -> !property.getName().equals("class"))
          .<Generator>map(property -> {
            var getter = property.getReadMethod();
            var key = computeKeyName(getter, property.getName());
            return (writer, instance) ->  key + writer.toJSON(Utils.invokeMethod(instance, getter));
          })
          .toList();
    }

    private String computeKeyName(Method getter, String defaultValue) {
      String keyName;
      var annotation = getter.getAnnotation(JSONProperty.class);
      if (annotation != null) {
        keyName = annotation.value();
      } else {
        keyName = defaultValue;
      }
      return "\"" + keyName + "\": ";
    }
  };

  public String toJSON(Object o) {
    return switch (o) {
      case String s -> "\"" + s + "\"";
      case Integer i -> "" + i;
      case Double d -> "" + d;
      case Boolean b -> "" + b;
      case null -> "null";
      case Object obj -> {
        var fun = map.get(obj.getClass());
        if (fun != null) {
          yield fun.apply(obj);
        }
        var generators = PROPERTIES_CLASS_VALUE.get(o.getClass());
        yield generators.stream()
          .map(generator -> generator.generate(this, o))
          .collect(joining(", ", "{", "}"));
      }
    };
  }

  private final HashMap<Class<?>, Function<Object, String>> map = new HashMap<>();

  public <T> void configure(Class<T> type, Function<T, String> fun) {
    Objects.requireNonNull(type);
    Objects.requireNonNull(fun);

    var result = map.putIfAbsent(type, o -> fun.apply(type.cast(o)));
    if (result != null) {
      throw new IllegalStateException("Configuration for " + type.getName() + " already exists");
    }
  }
}
