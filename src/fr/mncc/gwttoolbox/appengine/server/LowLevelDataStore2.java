/**
 * Copyright (c) 2012 MNCC
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.appengine.api.datastore.*;
import com.google.java.contract.Ensures;
import com.google.java.contract.Requires;

public class LowLevelDataStore2 {

  private final static int OFFSET_LIMIT = 1000;
  private final static AsyncDatastoreService dataStore_ = DatastoreServiceFactory
      .getAsyncDatastoreService();

  @Ensures("dataStore_ != null")
  public static AsyncDatastoreService getInstance() {
    return dataStore_;
  }

  @Requires("entity != null")
  public static Future<Key> put(com.google.appengine.api.datastore.Entity entity) {
    return dataStore_.put(entity);
  }

  @Requires("entities != null")
  public static Future<List<Key>> put(Iterable<com.google.appengine.api.datastore.Entity> entities) {
    return dataStore_.put(entities);
  }

  @Requires("key != null")
  public static Future<com.google.appengine.api.datastore.Entity> get(Key key) {
    return dataStore_.get(key);
  }

  @Requires("keys != null")
  public static Future<Map<Key, com.google.appengine.api.datastore.Entity>> get(Iterable<Key> keys) {
    return dataStore_.get(keys);
  }

  @Requires("key != null")
  public static Future<Void> delete(Key key) {
    return dataStore_.delete(key);
  }

  @Requires("keys != null")
  public static Future<Void> delete(Iterable<Key> keys) {
    return dataStore_.delete(keys);
  }

  @Requires("query != null")
  public static long listSize(Query query) {
    List<Query.SortPredicate> sortPredicates =
        new ArrayList<Query.SortPredicate>(query.getSortPredicates());
    query.getSortPredicates().clear();

    List<Projection> projections = new ArrayList<Projection>(query.getProjections());
    query.getProjections().clear();

    boolean isKeysOnly = query.isKeysOnly();
    query.setKeysOnly();

    PreparedQuery preparedQuery = dataStore_.prepare(query);
    FetchOptions fetchOptions = FetchOptions.Builder.withOffset(0);
    long size = preparedQuery.asList(fetchOptions).size();

    if (!isKeysOnly)
      query.clearKeysOnly();

    for (Projection projection : projections)
      query.addProjection(projection);

    for (Query.SortPredicate sortPredicate : sortPredicates)
      query.addSort(sortPredicate.getPropertyName(), sortPredicate.getDirection());
    return size;
  }

  @Requires("query != null")
  public static Iterable<com.google.appengine.api.datastore.Entity> list(Query query) {
    return list(query, 0, OFFSET_LIMIT);
  }

  @Requires({"query != null", "startIndex >= 0", "amount > 0"})
  @SuppressWarnings("deprecation")
  public static Iterable<com.google.appengine.api.datastore.Entity> list(Query query,
      int startIndex, int amount) {

    PreparedQuery preparedQuery = dataStore_.prepare(query);
    int offset = startIndex;

    if (offset < OFFSET_LIMIT) {
      if ((offset + amount) >= OFFSET_LIMIT)
        amount = amount - (offset + amount - OFFSET_LIMIT);
      return preparedQuery.asList(FetchOptions.Builder.withLimit(amount).offset(offset).chunkSize(
          amount));
    }

    int steps = startIndex / OFFSET_LIMIT;
    offset = startIndex - steps * OFFSET_LIMIT;
    Cursor cursor = getCursor(steps, preparedQuery);

    if (cursor != null)
      return preparedQuery.asList(FetchOptions.Builder.withLimit(amount).offset(offset).chunkSize(
          amount).cursor(cursor));
    return null;
  }

  @Requires("query != null")
  @Ensures("result != null")
  public static Iterable<Key> listKeys(Query query) {
    return listKeys(query, 0, OFFSET_LIMIT);
  }

  @Requires({"query != null", "startIndex >= 0", "amount > 0"})
  @Ensures("result != null")
  public static Iterable<Key> listKeys(Query query, int startIndex, int amount) {
    boolean isKeysOnly = query.isKeysOnly();
    query.setKeysOnly();

    List<Key> ids = new ArrayList<Key>();
    for (com.google.appengine.api.datastore.Entity entity : LowLevelDataStore2.list(query,
        startIndex, amount))
      ids.add(entity.getKey());

    if (!isKeysOnly)
      query.clearKeysOnly();
    return ids;
  }

  @SuppressWarnings("deprecation")
  private static Cursor getCursor(int steps, PreparedQuery preparedQuery) {
    QueryResultList<com.google.appengine.api.datastore.Entity> queryResultList =
        preparedQuery.asQueryResultList(FetchOptions.Builder.withLimit(1).chunkSize(1).offset(
            OFFSET_LIMIT - 1));
    Cursor cursor = queryResultList.getCursor();
    while (--steps > 0 && cursor != null) {
      queryResultList =
          preparedQuery.asQueryResultList(FetchOptions.Builder.withLimit(1).chunkSize(1).offset(
              OFFSET_LIMIT - 1).cursor(cursor));
      cursor = queryResultList.getCursor();
    }
    return cursor;
  }
}
