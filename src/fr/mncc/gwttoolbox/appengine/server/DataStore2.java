/**
 * Copyright (c) 2013 MNCC
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * @author http://www.mncc.fr
 */
package fr.mncc.gwttoolbox.appengine.server;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Query;
import com.google.appengine.api.datastore.Text;
import com.google.java.contract.Ensures;
import com.google.java.contract.Requires;
import fr.mncc.gwttoolbox.appengine.shared.SQuery2;
import fr.mncc.gwttoolbox.primitives.shared.Entity;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

@SuppressWarnings("serial")
public class DataStore2 {

  private final static Logger logger_ = Logger.getLogger(DataStore2.class.getCanonicalName());

  @Requires({"kind != null", "id >= 0"})
  @Ensures("result != null")
  private static Key createKey(String kind, long id) {
    return KeyFactory.createKey(kind, id);
  }

  @Requires({"kind != null", "id >= 0", "ancestorKind != null", "ancestorId >= 0"})
  @Ensures("result != null")
  private static Key createKey(String kind, long id, String ancestorKind, long ancestorId) {
    return ancestorKind == null ? createKey(kind, id) : KeyFactory.createKey(createKey(
        ancestorKind, ancestorId), kind, id);
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  private static Iterable<Key> createKeys(String kind, Iterable<Long> ids) {
    List<Key> keys = new ArrayList<Key>();
    for (Long id : ids)
      keys.add(createKey(kind, id));
    return keys;
  }

  @Requires({"kind != null", "ids != null", "ancestorKind != null", "ancestorId >= 0"})
  @Ensures("result != null")
  private static Iterable<Key> createKeys(String kind, Iterable<Long> ids, String ancestorKind,
      long ancestorId) {
    if (ancestorKind == null)
      return createKeys(kind, ids);

    List<Key> keys = new ArrayList<Key>();
    for (Long id : ids)
      keys.add(createKey(kind, id, ancestorKind, ancestorId));
    return keys;
  }

  @Deprecated
  public static fr.mncc.gwttoolbox.primitives.shared.Entity fromAppEngineEntity(
      com.google.appengine.api.datastore.Entity appEngineEntity) {
    return convertToToolboxEntity(appEngineEntity);
  }

  @Deprecated
  private static com.google.appengine.api.datastore.Entity toAppEngineEntity(
      fr.mncc.gwttoolbox.primitives.shared.Entity toolboxEntity, String ancestorKind,
      long ancestorId) {
    return convertToAppEngineEntity(toolboxEntity, ancestorKind, ancestorId);
  }

  @Requires({"toolboxEntity != null", "ancestorKind != null", "ancestorId >= 0"})
  @Ensures("result != null")
  private static com.google.appengine.api.datastore.Entity convertToAppEngineEntity(
      fr.mncc.gwttoolbox.primitives.shared.Entity toolboxEntity, String ancestorKind,
      long ancestorId) {

    // Create a new AppEngine entity
    com.google.appengine.api.datastore.Entity appEngineEntity = null;
    if (ancestorKind == null) {
      if (toolboxEntity.getId() != 0)
        appEngineEntity =
            new com.google.appengine.api.datastore.Entity(toolboxEntity.getKind(), toolboxEntity
                .getId());
      else
        appEngineEntity = new com.google.appengine.api.datastore.Entity(toolboxEntity.getKind());
    } else {
      if (toolboxEntity.getId() != 0)
        appEngineEntity =
            new com.google.appengine.api.datastore.Entity(toolboxEntity.getKind(), toolboxEntity
                .getId(), createKey(ancestorKind, ancestorId));
      else
        appEngineEntity =
            new com.google.appengine.api.datastore.Entity(toolboxEntity.getKind(), createKey(
                ancestorKind, ancestorId));
    }

    // Fill the AppEngine entity with the proper values, taking care of a few AppEngine limitations
    for (String propertyName : toolboxEntity.keySet()) {

      Object propertyValue = toolboxEntity.getAsObject(propertyName);
      if (propertyValue == null)
        continue;

      if (propertyValue instanceof String && ((String) propertyValue).length() >= 500) // DataStore
                                                                                       // limits
                                                                                       // String
                                                                                       // objects to
                                                                                       // 500
                                                                                       // characters
        propertyValue = new Text((String) propertyValue);
      else if (propertyValue instanceof Timestamp) // DataStore is not able to store Timestamp
                                                   // objects
        propertyValue = new Date(((Timestamp) propertyValue).getTime());
      else if (propertyValue instanceof Time) // DataStore is not able to store Time objects
        propertyValue = new Date(((Time) propertyValue).getTime());

      appEngineEntity.setProperty(propertyName, propertyValue);
    }
    return appEngineEntity;
  }

  @Requires("appEngineEntity != null")
  @Ensures("result != null")
  private static fr.mncc.gwttoolbox.primitives.shared.Entity convertToToolboxEntity(
      com.google.appengine.api.datastore.Entity appEngineEntity) {

    // Create a new Toolbox entity
    fr.mncc.gwttoolbox.primitives.shared.Entity toolboxEntity =
        new fr.mncc.gwttoolbox.primitives.shared.Entity(appEngineEntity.getKind(), appEngineEntity
            .getKey().getId());

    // Fill the Toolbox entity with the proper values, removing any AppEngine specific type
    for (String propertyName : appEngineEntity.getProperties().keySet()) {

      Object propertyValue = appEngineEntity.getProperty(propertyName);
      if (propertyValue instanceof Text)
        toolboxEntity.put(propertyName, ((Text) propertyValue).getValue());
      else if (propertyValue instanceof Date)
        toolboxEntity.put(propertyName, new Timestamp(((Date) propertyValue).getTime()));
      else
        toolboxEntity.put(propertyName, propertyValue);
    }
    return toolboxEntity;
  }

  @Requires({"toolboxEntities != null", "ancestorKind != null", "ancestorId >= 0"})
  @Ensures("result != null")
  private static Iterable<com.google.appengine.api.datastore.Entity> convertToAppEngineEntities(
      Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> toolboxEntities, String ancestorKind,
      long ancestorId) {
    List<com.google.appengine.api.datastore.Entity> appEngineEntities =
        new ArrayList<com.google.appengine.api.datastore.Entity>();
    for (fr.mncc.gwttoolbox.primitives.shared.Entity toolboxEntity : toolboxEntities)
      appEngineEntities.add(convertToAppEngineEntity(toolboxEntity, ancestorKind, ancestorId));
    return appEngineEntities;
  }

  @Requires("appEngineEntities != null")
  @Ensures("result != null")
  private static Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> convertToToolboxEntities(
      Iterable<com.google.appengine.api.datastore.Entity> appEngineEntities) {
    List<fr.mncc.gwttoolbox.primitives.shared.Entity> toolboxEntities =
        new ArrayList<fr.mncc.gwttoolbox.primitives.shared.Entity>();
    for (com.google.appengine.api.datastore.Entity appEngineEntity : appEngineEntities)
      toolboxEntities.add(convertToToolboxEntity(appEngineEntity));
    return toolboxEntities;
  }

  @Requires("entity != null")
  @Ensures("result != null")
  public static Long putSync(fr.mncc.gwttoolbox.primitives.shared.Entity entity) {
    return putSync(entity, null, 0);
  }

  @Requires("entity != null")
  @Ensures("result != null")
  public static Long putSync(fr.mncc.gwttoolbox.primitives.shared.Entity entity,
      String ancestorKind, long ancestorId) {
    try {
      return put(entity, ancestorKind, ancestorId).get();
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.getMessage(), e);
    }
    return 0L;
  }

  @Requires("entity != null")
  @Ensures("result != null")
  public static Future<Long> put(fr.mncc.gwttoolbox.primitives.shared.Entity entity) {
    return put(entity, null, 0);
  }

  @Requires("entity != null")
  @Ensures("result != null")
  public static Future<Long> put(fr.mncc.gwttoolbox.primitives.shared.Entity entity,
      String ancestorKind, long ancestorId) {
    final Future<Key> key =
        LowLevelDataStore2.put(convertToAppEngineEntity(entity, ancestorKind, ancestorId));
    final Future<Long> id = new Future<Long>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return key.cancel(mayInterruptIfRunning);
      }

      @Override
      public boolean isCancelled() {
        return key.isCancelled();
      }

      @Override
      public boolean isDone() {
        return key.isDone();
      }

      @Override
      public Long get() throws InterruptedException, ExecutionException {
        return key.get().getId();
      }

      @Override
      public Long get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
          TimeoutException {
        return key.get(timeout, unit).getId();
      }
    };
    return id;
  }

  @Requires("entities != null")
  @Ensures("result != null")
  public static List<Long> putSync(Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> entities) {
    return putSync(entities, null, 0);
  }

  @Requires("entities != null")
  @Ensures("result != null")
  public static List<Long> putSync(Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> entities,
      String ancestorKind, long ancestorId) {
    try {
      return put(entities, ancestorKind, ancestorId).get();
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.getMessage(), e);
    }
    return new ArrayList<Long>();
  }

  @Requires("entities != null")
  @Ensures("result != null")
  public static Future<List<Long>> put(
      Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> entities) {
    return put(entities, null, 0);
  }

  @Requires("entities != null")
  @Ensures("result != null")
  public static Future<List<Long>> put(
      Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> entities, String ancestorKind,
      long ancestorId) {
    final Future<List<Key>> keys =
        LowLevelDataStore2.put(convertToAppEngineEntities(entities, ancestorKind, ancestorId));
    final Future<List<Long>> ids = new Future<List<Long>>() {
      @Override
      public boolean cancel(boolean mayInterruptIfRunning) {
        return keys.cancel(mayInterruptIfRunning);
      }

      @Override
      public boolean isCancelled() {
        return keys.isCancelled();
      }

      @Override
      public boolean isDone() {
        return keys.isDone();
      }

      @Override
      public List<Long> get() throws InterruptedException, ExecutionException {
        List<Long> idsTmp = new ArrayList<Long>();
        List<Key> keysTmp = keys.get();
        for (Key key : keysTmp)
          idsTmp.add(key.getId());
        return idsTmp;
      }

      @Override
      public List<Long> get(long timeout, TimeUnit unit) throws InterruptedException,
          ExecutionException, TimeoutException {
        List<Long> idsTmp = new ArrayList<Long>();
        List<Key> keysTmp = keys.get(timeout, unit);
        for (Key key : keysTmp)
          idsTmp.add(key.getId());
        return idsTmp;
      }
    };
    return ids;
  }

  @Requires({"kind != null", "id > 0"})
  @Ensures("result != null")
  public static fr.mncc.gwttoolbox.primitives.shared.Entity getSync(String kind, long id) {
    return getSync(kind, id, null, 0);
  }

  @Requires({"kind != null", "id > 0"})
  @Ensures("result != null")
  public static fr.mncc.gwttoolbox.primitives.shared.Entity getSync(String kind, long id,
      String ancestorKind, long ancestorId) {
    try {
      return get(kind, id, ancestorKind, ancestorId).get();
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.getMessage(), e);
    }
    return new Entity(kind);
  }

  @Requires({"kind != null", "id > 0"})
  @Ensures("result != null")
  public static Future<fr.mncc.gwttoolbox.primitives.shared.Entity> get(String kind, long id) {
    return get(kind, id, null, 0);
  }

  @Requires({"kind != null", "id > 0"})
  @Ensures("result != null")
  public static Future<fr.mncc.gwttoolbox.primitives.shared.Entity> get(String kind, long id,
      String ancestorKind, long ancestorId) {
    final Key key =
        ancestorKind == null ? createKey(kind, id) : createKey(kind, id, ancestorKind, ancestorId);
    final Future<com.google.appengine.api.datastore.Entity> appEngineEntity =
        LowLevelDataStore2.get(key);
    final Future<fr.mncc.gwttoolbox.primitives.shared.Entity> toolboxEntity =
        new Future<fr.mncc.gwttoolbox.primitives.shared.Entity>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            return appEngineEntity.cancel(mayInterruptIfRunning);
          }

          @Override
          public boolean isCancelled() {
            return appEngineEntity.isCancelled();
          }

          @Override
          public boolean isDone() {
            return appEngineEntity.isDone();
          }

          @Override
          public fr.mncc.gwttoolbox.primitives.shared.Entity get() throws InterruptedException,
              ExecutionException {
            return convertToToolboxEntity(appEngineEntity.get());
          }

          @Override
          public fr.mncc.gwttoolbox.primitives.shared.Entity get(long timeout, TimeUnit unit)
              throws InterruptedException, ExecutionException, TimeoutException {
            return convertToToolboxEntity(appEngineEntity.get(timeout, unit));
          }
        };
    return toolboxEntity;
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  public static Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity> getSync(String kind,
      Iterable<Long> ids) {
    return getSync(kind, ids, null, 0);
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  public static Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity> getSync(String kind,
      Iterable<Long> ids, String ancestorKind, long ancestorId) {
    try {
      return get(kind, ids, ancestorKind, ancestorId).get();
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.getMessage(), e);
    }
    return new HashMap<Long, fr.mncc.gwttoolbox.primitives.shared.Entity>();
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  public static Future<Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity>> get(String kind,
      Iterable<Long> ids) {
    return get(kind, ids, null, 0);
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  public static Future<Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity>> get(String kind,
      Iterable<Long> ids, String ancestorKind, long ancestorId) {
    final Iterable<Key> keys =
        ancestorKind == null ? createKeys(kind, ids) : createKeys(kind, ids, ancestorKind,
            ancestorId);
    final Future<Map<Key, com.google.appengine.api.datastore.Entity>> appEngineEntities =
        LowLevelDataStore2.get(keys);
    final Future<Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity>> toolboxEntities =
        new Future<Map<Long, Entity>>() {
          @Override
          public boolean cancel(boolean mayInterruptIfRunning) {
            return appEngineEntities.cancel(mayInterruptIfRunning);
          }

          @Override
          public boolean isCancelled() {
            return appEngineEntities.isCancelled();
          }

          @Override
          public boolean isDone() {
            return appEngineEntities.isDone();
          }

          @Override
          public Map<Long, Entity> get() throws InterruptedException, ExecutionException {
            Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity> toolboxEntitiesTmp =
                new HashMap<Long, Entity>();
            Map<Key, com.google.appengine.api.datastore.Entity> appEngineEntitiesTmp =
                appEngineEntities.get();
            for (Key key : appEngineEntitiesTmp.keySet())
              toolboxEntitiesTmp.put(key.getId(), convertToToolboxEntity(appEngineEntitiesTmp
                  .get(key)));
            return toolboxEntitiesTmp;
          }

          @Override
          public Map<Long, Entity> get(long timeout, TimeUnit unit) throws InterruptedException,
              ExecutionException, TimeoutException {
            Map<Long, fr.mncc.gwttoolbox.primitives.shared.Entity> toolboxEntitiesTmp =
                new HashMap<Long, Entity>();
            Map<Key, com.google.appengine.api.datastore.Entity> appEngineEntitiesTmp =
                appEngineEntities.get(timeout, unit);
            for (Key key : appEngineEntitiesTmp.keySet())
              toolboxEntitiesTmp.put(key.getId(), convertToToolboxEntity(appEngineEntitiesTmp
                  .get(key)));
            return toolboxEntitiesTmp;
          }
        };
    return toolboxEntities;
  }

  @Requires({"kind != null", "id > 0"})
  public static boolean deleteSync(String kind, long id) {
    return deleteSync(kind, id, null, 0);
  }

  @Requires({"kind != null", "id > 0"})
  public static boolean deleteSync(String kind, long id, String ancestorKind, long ancestorId) {
    try {
      delete(kind, id, ancestorKind, ancestorId).get();
      return true;
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.getMessage(), e);
    }
    return false;
  }

  @Requires({"kind != null", "id > 0"})
  @Ensures("result != null")
  public static Future<Void> delete(String kind, long id) {
    return delete(kind, id, null, 0);
  }

  @Requires({"kind != null", "id > 0"})
  @Ensures("result != null")
  public static Future<Void> delete(String kind, long id, String ancestorKind, long ancestorId) {
    if (ancestorKind == null)
      return LowLevelDataStore2.delete(createKey(kind, id));
    return LowLevelDataStore2.delete(createKey(kind, id, ancestorKind, ancestorId));
  }

  @Requires({"kind != null", "ids != null"})
  public static boolean deleteSync(String kind, Iterable<Long> ids) {
    return deleteSync(kind, ids, null, 0);
  }

  @Requires({"kind != null", "ids != null"})
  public static boolean deleteSync(String kind, Iterable<Long> ids, String ancestorKind,
      long ancestorId) {
    try {
      delete(kind, ids, ancestorKind, ancestorId).get();
      return true;
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.getMessage(), e);
    }
    return false;
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  public static Future<Void> delete(String kind, Iterable<Long> ids) {
    return delete(kind, ids, null, 0);
  }

  @Requires({"kind != null", "ids != null"})
  @Ensures("result != null")
  public static Future<Void> delete(String kind, Iterable<Long> ids, String ancestorKind,
      long ancestorId) {
    if (ancestorKind.isEmpty())
      return LowLevelDataStore2.delete(createKeys(kind, ids));
    return LowLevelDataStore2.delete(createKeys(kind, ids, ancestorKind, ancestorId));
  }

  @Requires("toolboxQuery != null")
  @Ensures("result >= 0")
  public static long listSize(SQuery2 toolboxQuery) {
    // logger_.log(Level.INFO, toolboxQuery.toString());
    Query appEngineQuery = QueryConverter2.getAsAppEngineQuery(toolboxQuery);
    return LowLevelDataStore2.listSize(appEngineQuery);
  }

  @Requires("toolboxQuery != null")
  @Ensures("result != null")
  public static Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> list(SQuery2 toolboxQuery) {
    // logger_.log(Level.INFO, toolboxQuery.toString());
    Query appEngineQuery = QueryConverter2.getAsAppEngineQuery(toolboxQuery);
    return convertToToolboxEntities(LowLevelDataStore2.list(appEngineQuery));
  }

  @Requires({"toolboxQuery != null", "startIndex >= 0", "amount > 0"})
  @Ensures("result != null")
  public static Iterable<fr.mncc.gwttoolbox.primitives.shared.Entity> list(SQuery2 toolboxQuery,
      int startIndex, int amount) {
    // logger_.log(Level.INFO, toolboxQuery.toString());
    Query appEngineQuery = QueryConverter2.getAsAppEngineQuery(toolboxQuery);
    return convertToToolboxEntities(LowLevelDataStore2.list(appEngineQuery, startIndex, amount));
  }

  @Requires("toolboxQuery != null")
  @Ensures("result != null")
  public static Iterable<Long> listIds(SQuery2 toolboxQuery) {
    // logger_.log(Level.INFO, toolboxQuery.toString());
    Query appEngineQuery = QueryConverter2.getAsAppEngineQuery(toolboxQuery);
    List<Long> ids = new ArrayList<Long>();
    for (Key key : LowLevelDataStore2.listKeys(appEngineQuery))
      ids.add(key.getId());
    return ids;
  }

  @Requires({"toolboxQuery != null", "startIndex >= 0", "amount > 0"})
  @Ensures("result != null")
  public static Iterable<Long> listIds(SQuery2 toolboxQuery, int startIndex, int amount) {
    // logger_.log(Level.INFO, toolboxQuery.toString());
    Query appEngineQuery = QueryConverter2.getAsAppEngineQuery(toolboxQuery);
    List<Long> ids = new ArrayList<Long>();
    for (Key key : LowLevelDataStore2.listKeys(appEngineQuery, startIndex, amount))
      ids.add(key.getId());
    return ids;
  }
}
