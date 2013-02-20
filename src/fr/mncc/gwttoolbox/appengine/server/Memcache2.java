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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.memcache.Expiration;

public class Memcache2 {

  private final static Logger logger_ = Logger.getLogger(Memcache2.class.getCanonicalName());

  public static void putSync(String namespace, Object key, Object value) {
    putSync(namespace, key, value, -1);
  }

  public static void put(String namespace, Object key, Object value) {
    put(namespace, key, value, -1);
  }

  public static void putAllSync(String namespace, Map<Object, Object> values) {
    putAllSync(namespace, values, -1);
  }

  public static void putAll(String namespace, Map<Object, Object> values) {
    putAll(namespace, values, -1);
  }

  public static void putSync(String namespace, Object key, Object value, int expirationInSeconds) {
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      if (expirationInSeconds < 0)
        LowLevelMemcache.getInstance().put(key, value);
      else
        LowLevelMemcache.getInstance().put(key, value,
            Expiration.byDeltaSeconds(expirationInSeconds));
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkey = "
          + key.toString() + "\nvalue = " + value.toString() + "\nexpirationInSeconds = "
          + expirationInSeconds);
    } finally {
      NamespaceManager.set(oldNamespace);
    }
  }

  public static void put(String namespace, Object key, Object value, int expirationInSeconds) {
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      if (expirationInSeconds < 0)
        LowLevelMemcacheAsync.getInstance().put(key, value);
      else
        LowLevelMemcacheAsync.getInstance().put(key, value,
            Expiration.byDeltaSeconds(expirationInSeconds));
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkey = "
          + key.toString() + "\nvalue = " + value.toString() + "\nexpirationInSeconds = "
          + expirationInSeconds);
    } finally {
      NamespaceManager.set(oldNamespace);
    }
  }

  public static void putAllSync(String namespace, Map<Object, Object> values,
      int expirationInSeconds) {
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      if (expirationInSeconds < 0)
        LowLevelMemcache.getInstance().putAll(values);
      else
        LowLevelMemcache.getInstance().putAll(values,
            Expiration.byDeltaSeconds(expirationInSeconds));
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nvalues = "
          + values.toString() + "\nexpirationInSeconds = " + expirationInSeconds);
    } finally {
      NamespaceManager.set(oldNamespace);
    }
  }

  public static void putAll(String namespace, Map<Object, Object> values, int expirationInSeconds) {
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      if (expirationInSeconds < 0)
        LowLevelMemcacheAsync.getInstance().putAll(values);
      else
        LowLevelMemcacheAsync.getInstance().putAll(values,
            Expiration.byDeltaSeconds(expirationInSeconds));
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nvalues = "
          + values.toString() + "\nexpirationInSeconds = " + expirationInSeconds);
    } finally {
      NamespaceManager.set(oldNamespace);
    }
  }

  public static Object getSync(String namespace, Object key) {
    return LowLevelMemcache.get(namespace, key);
  }

  public static Future<Object> get(String namespace, Object key) {
    return LowLevelMemcacheAsync.get(namespace, key);
  }

  public static Map<Object, Object> getAllSync(String namespace, List<Object> keys) {
    return LowLevelMemcache.getAll(namespace, keys);
  }

  public static Future<Map<Object, Object>> getAll(String namespace, List<Object> keys) {
    return LowLevelMemcacheAsync.getAll(namespace, keys);
  }

  public static boolean deleteSync(String namespace, Object key) {
    return LowLevelMemcache.delete(namespace, key);
  }

  public static Future<Boolean> delete(String namespace, Object key) {
    return LowLevelMemcacheAsync.delete(namespace, key);
  }

  public static Set<Object> deleteAllSync(String namespace, List<Object> keys) {
    return LowLevelMemcache.deleteAll(namespace, keys);
  }

  public static Future<Set<Object>> deleteAll(String namespace, List<Object> keys) {
    return LowLevelMemcacheAsync.deleteAll(namespace, keys);
  }
}
