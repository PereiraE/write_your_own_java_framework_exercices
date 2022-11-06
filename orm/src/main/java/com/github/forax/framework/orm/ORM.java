package com.github.forax.framework.orm;

import java.beans.BeanInfo;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.StringJoiner;
import javax.sql.DataSource;
import java.io.Serial;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import jdk.jshell.execution.Util;

public final class ORM {
  private ORM() {
    throw new AssertionError();
  }

  @FunctionalInterface
  public interface TransactionBlock {
    void run() throws SQLException;
  }

  private static final Map<Class<?>, String> TYPE_MAPPING = Map.of(
      int.class, "INTEGER",
      Integer.class, "INTEGER",
      long.class, "BIGINT",
      Long.class, "BIGINT",
      String.class, "VARCHAR(255)"
  );

  private static Class<?> findBeanTypeFromRepository(Class<?> repositoryType) {
    var repositorySupertype = Arrays.stream(repositoryType.getGenericInterfaces())
        .flatMap(superInterface -> {
          if (superInterface instanceof ParameterizedType parameterizedType
              && parameterizedType.getRawType() == Repository.class) {
            return Stream.of(parameterizedType);
          }
          return null;
        })
        .findFirst()
        .orElseThrow(() -> new IllegalArgumentException("invalid repository interface " + repositoryType.getName()));
    var typeArgument = repositorySupertype.getActualTypeArguments()[0];
    if (typeArgument instanceof Class<?> beanType) {
      return beanType;
    }
    throw new IllegalArgumentException("invalid type argument " + typeArgument + " for repository interface " + repositoryType.getName());
  }

  private static class UncheckedSQLException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 42L;

    private UncheckedSQLException(SQLException cause) {
      super(cause);
    }

    @Override
    public SQLException getCause() {
      return (SQLException) super.getCause();
    }
  }


  // --- do not change the code above

  private static final ThreadLocal<Connection> CONNECTION_THREAD_LOCAL = new ThreadLocal<>();

  public static void transaction(DataSource dataSource, TransactionBlock block) throws SQLException {
    Objects.requireNonNull(dataSource);
    Objects.requireNonNull(block);

    try (var connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      CONNECTION_THREAD_LOCAL.set(connection);
      try {
        block.run();
        connection.commit();
      } catch (SQLException | RuntimeException e) {
        var rootCause = e instanceof UncheckedSQLException unchecked ? unchecked.getCause() : e;
        try {
          connection.rollback();
        } catch (SQLException e2) {
          e.addSuppressed(e2);
        }
        throw Utils.rethrow(rootCause);
      } finally {
        CONNECTION_THREAD_LOCAL.remove();
      }
    }
  }

  static Connection currentConnection() {
    var connection = CONNECTION_THREAD_LOCAL.get();
    if (connection == null) {
      throw new IllegalStateException("No connection available");
    }
    return connection;
  }

  public static void createTable(Class<?> beanClass) throws SQLException {
    Objects.requireNonNull(beanClass);
    var connection = currentConnection();
    var prefix = "CREATE TABLE " + findTableName(beanClass) + " (";
    var beanInfo = Utils.beanInfo(beanClass);

    var joiner = new StringJoiner(", ", prefix, ");");
    var id = (String) null;
    for (PropertyDescriptor property : beanInfo.getPropertyDescriptors()) {
      if (property.getName().equals("class")) {
        continue;
      }
      var columnName = findColumnName(property);
      if (isId(property)) {
        if (id != null) {
          throw new IllegalStateException("Multiple ids defined " + id + " " + columnName);
        }
        id = columnName;
      }
      var line =  columnName + " " + findColumnType(property);
      joiner.add(line);
    }
    if (id != null) {
      joiner.add("PRIMARY KEY (" + id + ")");
    }
    var query = joiner.toString();

    try (var statement = connection.createStatement()) {
      statement.executeUpdate(query);
    }
    connection.commit();
  }

  static String findTableName(Class<?> beanClass) {
    var tableAnnotation = beanClass.getAnnotation(Table.class);
    if(tableAnnotation == null) {
      return beanClass.getSimpleName().toUpperCase(Locale.ROOT);
    }
    return tableAnnotation.value();
  }

  static String findColumnName(PropertyDescriptor descriptor) {
    var columnAnnotation = descriptor.getReadMethod().getAnnotation(Column.class);
    if (columnAnnotation == null) {
      return descriptor.getName().toUpperCase(Locale.ROOT);
    }
    return columnAnnotation.value();
  }

  private static String findColumnType(PropertyDescriptor descriptor) {
    var type = descriptor.getPropertyType();
    var mapping = TYPE_MAPPING.get(type);
    if (mapping == null) {
      throw new IllegalStateException("Unknown property type : " + descriptor);
    }
    var nullable = type.isPrimitive() ? " NOT NULL": " ";
    var generatedValue = descriptor.getReadMethod().isAnnotationPresent(GeneratedValue.class);
    var autoIncrement = generatedValue ? " AUTO_INCREMENT" : " ";
    return mapping + nullable + autoIncrement;
  }

  private static boolean isId(PropertyDescriptor descriptor) {
    return descriptor.getReadMethod().isAnnotationPresent(Id.class);
  }

  public static<T> T createRepository(Class<T> repositoryType) {
    Objects.requireNonNull(repositoryType);

    var repositoryProxy = Proxy.newProxyInstance(repositoryType.getClassLoader(),
            new Class<?>[] {repositoryType},
            (proxy, method, args) -> {
              var methodName = method.getName();
              return switch (methodName) {
                case "findAll" -> {
                  var connection = currentConnection();
                  var type = findBeanTypeFromRepository(repositoryType);
                  var query = "SELECT * FROM " + findTableName(type);
                  var beanInfo = Utils.beanInfo(type);
                  var constructor = type.getConstructor();
                  yield findAll(connection, query, beanInfo, constructor);
                }
                case "equals", "toString", "hashCode" -> throw new UnsupportedOperationException();
                default -> throw new IllegalStateException();
              };
            });
    return repositoryType.cast(repositoryProxy);
  }

  static<T> T toEntityClass(ResultSet resultSet, BeanInfo beanInfo, Constructor<T> constructor) {
    var properties = Arrays.stream(beanInfo.getPropertyDescriptors())
            .filter(descriptor -> !descriptor.getName().equals("class"))
            .toList();
    try {
      var entity = constructor.newInstance();
      var index = 1;
      for (var property : properties) {
        var setter = property.getWriteMethod();
        Utils.invokeMethod(entity, setter, resultSet.getObject(index));
        index++;
      }
      return constructor.getDeclaringClass().cast(entity);
    } catch (InstantiationException | InvocationTargetException | IllegalAccessException  e) {
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  static<T> List<T> findAll(Connection connection, String sqlQuery, BeanInfo beanInfo, Constructor<T> constructor) {
    try(var statement = connection.prepareStatement(sqlQuery)) {
      var result = statement.executeQuery();
      var list = new ArrayList<T>();
      while (result.next()) {
        list.add(toEntityClass(result, beanInfo, constructor));
      }
      return list;
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }
}
