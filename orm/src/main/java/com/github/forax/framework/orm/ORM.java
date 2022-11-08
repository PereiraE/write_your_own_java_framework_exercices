package com.github.forax.framework.orm;

import static java.util.Collections.*;
import static java.util.function.Predicate.*;
import static java.util.stream.Collectors.*;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.Statement;
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
        try {
          block.run();
          connection.commit();
        } catch (UncheckedSQLException e) {
          throw e.getCause();
        }
      } catch (SQLException | RuntimeException e) {
        try {
          connection.rollback();
        } catch (SQLException e2) {
          e.addSuppressed(e2);
        }
        throw e;
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

  public static<R extends Repository<T,ID>, T, ID> R createRepository(Class<R> repositoryType) {
    Objects.requireNonNull(repositoryType);

    var type = findBeanTypeFromRepository(repositoryType);
    var beanInfo = Utils.beanInfo(type);
    var defaultConstructor = Utils.defaultConstructor(type);
    var tableName = findTableName(type);
    var idProperty = findId(beanInfo);
    return repositoryType.cast(
      Proxy.newProxyInstance(repositoryType.getClassLoader(),
        new Class<?>[] {repositoryType},
        (proxy, method, args) -> {
          var methodName = method.getName();
          if (method.getDeclaringClass() == Object.class) {
            throw new UnsupportedOperationException(methodName + " not supported");
          }
          var connection = currentConnection();
          try {
            var methodAnnotation = method.getAnnotation(Query.class);
            if (methodAnnotation != null) {
              return findAll(connection, methodAnnotation.value(), beanInfo, defaultConstructor, args);
            }
            return switch (methodName) {
              case "findAll" -> {
                var query = "SELECT * FROM " + tableName;
                yield findAll(connection, query, beanInfo, defaultConstructor);
              }
              case "save" -> save(connection, tableName, beanInfo, args[0], idProperty);
              case "findById" -> {
                var query = """
                        SELECT * FROM %s WHERE %s = ?;\
                        """.formatted(tableName, findColumnName(idProperty));
                yield findAll(connection, query, beanInfo, defaultConstructor, args[0])
                        .stream().findFirst();
              }
              default -> {
                if (methodName.startsWith("findBy")) {
                  var name = methodName.substring("findBy".length());
                  var propertyName = Introspector.decapitalize(name);
                  var property = findProperty(beanInfo, propertyName);
                  yield findAll(connection, """
                          SELECT * FROM %s WHERE %s = ?\
                          """.formatted(tableName, property.getName())
                          , beanInfo, defaultConstructor, args[0]).stream().findFirst();
                }
                throw new IllegalStateException();
              }
            };
          } catch (SQLException e) {
            throw new UncheckedSQLException(e);
          }
        }));
  }

  static Object toEntityClass(ResultSet resultSet, BeanInfo beanInfo, Constructor<?> constructor) {
    var properties = Arrays.stream(beanInfo.getPropertyDescriptors())
            .filter(descriptor -> !descriptor.getName().equals("class"))
            .toList();
    try {
      var entity = Utils.newInstance(constructor);
      var index = 1;
      for (var property : properties) {
        var setter = property.getWriteMethod();
        Utils.invokeMethod(entity, setter, resultSet.getObject(index));
        index++;
      }
      return constructor.getDeclaringClass().cast(entity);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e);
    }
  }

  static List<?> findAll(Connection connection, String sqlQuery, BeanInfo beanInfo, Constructor<?> constructor,
                         Object... args)
          throws SQLException {
    var list = new ArrayList<>();
    try(var statement = connection.prepareStatement(sqlQuery)) {
      if (args != null) {
        for (int i = 0; i < args.length; i++) {
          statement.setObject(i + 1, args[i]);
        }
      }
      try(var resultSet = statement.executeQuery()) {
        while (resultSet.next()) {
          var instance = toEntityClass(resultSet, beanInfo, constructor);
          list.add(instance);
        }
      }
    }
    return list;
  }

  static String createSaveQuery(String tableName, BeanInfo beanInfo) {
    var properties = beanInfo.getPropertyDescriptors();
    var columnNames = Arrays.stream(properties)
            .map(PropertyDescriptor::getName)
            .filter(not("class"::equals))
            .collect(joining(", "));

    var jokers = String.join(", ", nCopies(properties.length - 1, "?"));
    return  """
        MERGE INTO %s (%s) VALUES (%s);\
        """.formatted(
            tableName,
            columnNames,
            jokers
        );
  }

  static Object save(Connection connection, String tableName, BeanInfo beanInfo,
                                  Object bean, PropertyDescriptor idProperty)
          throws SQLException {
    var query = createSaveQuery(tableName, beanInfo);
    var descriptors = beanInfo.getPropertyDescriptors();
    var index = 1;
    try (var statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
      for (var descriptor : descriptors) {
        if (descriptor.getName().equals("class")) {
          continue;
        }
        var getter = descriptor.getReadMethod();
        var value = Utils.invokeMethod(bean, getter);
        statement.setObject(index, value);
        index++;
      }
      statement.executeUpdate();
      if (idProperty != null) {
        try (var resultSet = statement.getGeneratedKeys()) {
          if (resultSet.next()) {
            var key = resultSet.getObject(1);
            var setter = idProperty.getWriteMethod();
            Utils.invokeMethod(bean, setter, key);
          }
        }
      }
      return bean;
    }
  }

  static PropertyDescriptor findId(BeanInfo beanInfo) {
    var descriptors = beanInfo.getPropertyDescriptors();
    return Arrays.stream(descriptors).filter(descriptor -> !descriptor.getName().equals("class"))
            .filter(descriptor -> descriptor.getReadMethod().isAnnotationPresent(Id.class))
            .findFirst()
            .orElse(null);
  }

  static PropertyDescriptor findProperty(BeanInfo beanInfo, String name) {
    return Arrays.stream(beanInfo.getPropertyDescriptors())
            .filter(descriptor -> !descriptor.getName().equals("class"))
            .filter(descriptor -> descriptor.getName().equals(name))
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No property name " + name));
  }
}
