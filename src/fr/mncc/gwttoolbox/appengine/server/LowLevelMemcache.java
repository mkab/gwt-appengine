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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.appengine.api.NamespaceManager;
import com.google.appengine.api.memcache.MemcacheService;
import com.google.appengine.api.memcache.MemcacheServiceFactory;

public class LowLevelMemcache {

  private final static Logger logger_ = Logger.getLogger(LowLevelMemcache.class.getCanonicalName());
  private final static MemcacheService memcache_ = MemcacheServiceFactory.getMemcacheService();

  public static MemcacheService getInstance() {
    return memcache_;
  }

  @Deprecated
  public static void put(String namespace, Object key, Object value) {
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      memcache_.put(key, value);
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkey = "
          + key.toString() + "\nvalue = " + value.toString());
    } finally {
      NamespaceManager.set(oldNamespace);
    }
  }

  @Deprecated
  public static void putAll(String namespace, Map<Object, Object> values) {
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      memcache_.putAll(values);
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nvalues = "
          + values.toString());
    } finally {
      NamespaceManager.set(oldNamespace);
    }
  }

  public static Object get(String namespace, Object key) {
    Object object = null;
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      object = memcache_.get(key);
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkey = "
          + key.toString());
    } finally {
      NamespaceManager.set(oldNamespace);
    }
    return object;
  }

  public static Map<Object, Object> getAll(String namespace, List<Object> keys) {
    Map<Object, Object> searchedValues = null;
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      searchedValues = memcache_.getAll(keys);
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkeys = "
          + keys.toString());
    } finally {
      NamespaceManager.set(oldNamespace);
    }
    return searchedValues;
  }

  public static boolean delete(String namespace, Object key) {
    boolean isOk = false;
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      isOk = memcache_.delete(key);
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkey = "
          + key.toString());
    } finally {
      NamespaceManager.set(oldNamespace);
    }
    return isOk;
  }

  public static Set<Object> deleteAll(String namespace, List<Object> keys) {
    Set<Object> removedValues = null;
    String oldNamespace = NamespaceManager.get();
    NamespaceManager.set(namespace);
    try {
      removedValues = memcache_.deleteAll(keys);
    } catch (Exception e) {
      logger_.log(Level.SEVERE, e.toString() + "\nnamespace = " + namespace + "\nkeys = "
          + keys.toString());
    } finally {
      NamespaceManager.set(oldNamespace);
    }
    return removedValues;
  }
}
